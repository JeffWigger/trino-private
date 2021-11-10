/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.DeltaPage;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.trino.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    // Hashtables that map primary keys to the storage position.
    // TODO: chose a good initial capacity
    // This will be unaccounted for in the tracked storage size
    @GuardedBy("this")
    private Map<Long, Map<Slice, TableDataPosition>> hashTables;
    private Map<Long, List<ColumnInfo>> indecies;

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId, List<ColumnInfo> indecies)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
            hashTables.put(tableId, new HashMap<>());
            this.indecies.put(tableId, indecies);
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        TableData tableData = tables.get(tableId);

        int pageSize = page.getPositionCount();

        // checking the hashTable
        Map<Slice, TableDataPosition> htable = hashTables.get(tableId);
        if (htable.isEmpty()){
            htable = new HashMap<Slice, TableDataPosition>(2*pageSize);
            hashTables.put(tableId, htable);
        }
        int pageNr = tableData.getPageNumber() + 1;

        for(int i = 0; i < pageSize; i ++){
            Page row = page.getSingleValuePage(i);
            List<ColumnInfo> indecies = this.indecies.get(tableId);
            // TODO: For TPC benchmarks this is good enough, but should add resizing if it is too small.
            Slice keyData =  Slices.allocate(50);
            SliceOutput key = keyData.getOutput();//Slices.allocate(5);
            for(ColumnInfo ci : indecies){
                if(ci.isPrimaryKey()){// should always be the case
                    Type type = ci.getType();
                    Block block = page.getBlock(((MemoryColumnHandle)ci.getHandle()).getColumnIndex());
                    // from RecordPageSource
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        boolean b = type.getBoolean(block, i);
                        key.writeBoolean(b);
                    }
                    else if (javaType == long.class) {
                        long l = type.getLong(block, i);
                        key.writeLong(l);
                    }
                    else if (javaType == double.class) {
                        double d = type.getDouble(block, i);
                        key.writeDouble(d);
                    }
                    else if (javaType == Slice.class) {
                        Slice s = type.getSlice(block, i);
                        key.writeBytes(s);
                    }
                    else {
                        // TODO: THIS PROBABLY DOES NOT WORK!
                        Object o = type.getObject(block, i);
                        key.writeBytes(o.toString());
                    }
                }else{
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "got index column that is not an index");
                }
            }
            //.slice only get the part of the slice we have written too!
            htable.put(key.slice(), new TableDataPosition(pageNr, i));
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        tableData.add(page);
    }

    public synchronized void addDelta(Long tableId, DeltaPage page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(page);
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new TrinoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        boolean done = false;
        long totalRows = 0;
        for (int i = partNumber; i < tableData.getPages().size() && !done; i += totalParts) {
            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                continue;
            }

            Page page = tableData.getPages().get(i);
            totalRows += page.getPositionCount();
            if (limit.isPresent() && totalRows > limit.getAsLong()) {
                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
                done = true;
            }
            partitionedPages.add(getColumns(page, columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
                hashTables.remove(tableId);
                indecies.remove(tableId);
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static final class TableData
    {
        private final List<Page> pages = new ArrayList<>();
        private long rows;

        // TODO: check that it is synchronized
        public int add(Page page)
        {
            pages.add(page);
            rows += page.getPositionCount();
            return pages.size();
        }

        private List<Page> getPages()
        {
            return pages;
        }

        private int getPageNumber()
        {
            return pages.size();
        }

        private long getRows()
        {
            return rows;
        }
    }

    private static final class TableDataPosition
    {
        public int positon;
        public int pageNr;

        public TableDataPosition(int pageNr, int position){
            this.pageNr = pageNr;
            this.positon = position;
        }
    }
}
