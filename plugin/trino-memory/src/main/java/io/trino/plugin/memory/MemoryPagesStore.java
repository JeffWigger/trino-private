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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.DeltaFlagRequest;
import io.trino.spi.DeltaPage;
import io.trino.spi.DeltaPageBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.UpdatablePage;
import io.trino.spi.block.Block;
import io.trino.spi.block.UpdatableBlock;
import io.trino.spi.type.Type;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.nio.file.Paths;

import static io.trino.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.trino.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private int decreases = 0;
    private final long maxBytes;

    static final int MAX_PAGE_SIZE = 10000; // only affects the regular inserts

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();
    private final Map<Long, List<UpdatablePage>> tablesDelta = new HashMap<>();

    // Hashtables that map primary keys to the storage position.
    // TODO: chose a good initial capacity
    // This will be unaccounted for in the tracked storage size
    @GuardedBy("this")
    private Map<Long, Map<Slice, TableDataPosition>> hashTables = new HashMap<>();
    // private Map<Long, Map<Slice, TableDataPosition>> hashTablesDelta = new HashMap<>();,preprocessing deltas has little value, as for merging the hashTable we do the same amount of work, or could use putall, but it is not more efficient, it calls put for all entreis
    private Map<Long, List<ColumnInfo>> indices = new HashMap<>();

    private Path statisticsFilePath;
    private File file;
    private FileWriter statisticsWriter;

    private int splitsPerNode;

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
        splitsPerNode = config.getSplitsPerNode();
        String fileDirectory = "/scratch/wigger/";
        String statisticsFileName = "MemoryPagesStore";

        try {
            Path dir = Paths.get(fileDirectory);
            Files.createDirectories(dir);
            statisticsFilePath = dir.resolve(statisticsFileName);
            this.file = new File(statisticsFilePath.toString());
            if(!this.file.exists()){
                this.file.createNewFile();
            }
            assert(this.file.exists() && file.canWrite());
            statisticsWriter = new FileWriter(this.file, true);
            statisticsWriter.write(String.format("UpdateNr,InsertBytes,DeleteBytes,UpdateBytes,InsertCount,DeleteCount,UpdateCount,MicroSeconds\n"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    public synchronized void initialize(long tableId, List<ColumnInfo> indices)
    {
        // TODO: should update the indices in case a new column was added.
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
            tablesDelta.put(tableId, new LinkedList<>());
            hashTables.put(tableId, new HashMap<>());
            this.indices.put(tableId, indices);
        }
    }

    public synchronized int add(Long tableId, Page page)
    {
        int added = 0;
        int index = 0;
        int positionCount = page.getPositionCount();
        if (positionCount > MAX_PAGE_SIZE){
            while(true){
                int length =  index + MAX_PAGE_SIZE < positionCount ? MAX_PAGE_SIZE : positionCount - index;
                added += add(tableId, page.getRegion(index, length));
                index += length;
                if (index >= positionCount){
                    return added;
                }
            }
        }
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        TableData tableData = tables.get(tableId);

        int pageSize = page.getPositionCount();

        // checking the hashTable
        Map<Slice, TableDataPosition> htable = hashTables.get(tableId);
        if (htable.isEmpty()) {
            htable = new HashMap<Slice, TableDataPosition>((int) (pageSize * 1.3));
            hashTables.put(tableId, htable);
        }
        int pageNr = tableData.getPageNumber(); // + 1; getPageNumber returns 0 for the first page that gets added.
        for (int i = 0; i < pageSize; i++) {
            Page row = page.getSingleValuePage(i);
            Slice key = getKey(tableId, row);
            TableDataPosition tableDataPosition = htable.getOrDefault(key, null);
            if (tableDataPosition != null) {
                // entry is already in the DB
                // implicit insert
                UpdatablePage uPage = tableData.pages.get(tableDataPosition.pageNr);
                uPage.updateRow(row, tableDataPosition.position);
                continue;
            }
            htable.put(key, new TableDataPosition(pageNr, i));
            added++;
        }
        if (added == 0) {
            return 0;
        }
        UpdatablePage updatablePage = make_updatable(page, tableId);
        updatablePage.compact();

        long newSize = currentBytes + updatablePage.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        tableData.add(updatablePage);
        return added;
    }

    /**
     * row must be the result of page.getSingleValuePage
     */
    private Slice getKey(long tableId, Page row)
    {
        List<ColumnInfo> indecies = this.indices.get(tableId);
        // 10 is a good enough estimated size
        DynamicSliceOutput key = new DynamicSliceOutput(10);
        for (ColumnInfo ci : indecies) {
            if (ci.isPrimaryKey()) {// should always be the case
                Type type = ci.getType();
                Block block = row.getBlock(((MemoryColumnHandle) ci.getHandle()).getColumnIndex());
                // from RecordPageSource
                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    boolean b = type.getBoolean(block, 0);
                    key.writeBoolean(b);
                }
                else if (javaType == long.class) {
                    long l = type.getLong(block, 0);
                    key.writeLong(l);
                }
                else if (javaType == double.class) {
                    double d = type.getDouble(block, 0);
                    key.writeDouble(d);
                }
                else if (javaType == Slice.class) {
                    Slice s = type.getSlice(block, 0);
                    key.writeBytes(s);
                }
                else {
                    // TODO: THIS PROBABLY DOES NOT WORK!
                    Object o = type.getObject(block, 0);
                    key.writeBytes(o.toString());
                }
            }
            //else {
            //  throw new TrinoException(GENERIC_INTERNAL_ERROR, "got index column that is not an index");
            //}
        }
        return key.slice();
    }

    public synchronized int addDeltaNew(Long tableId, DeltaPage page)
    {
        long startTime = System.nanoTime();
        long updatesBytes = 0;
        long insertsBytes = 0;
        long deletesBytes = 0;
        long updatesCount = 0;
        long insertsCount = 0;
        long deletesCount = 0;
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        int cols = page.getChannelCount();
        int size = page.getPositionCount();

        UpdatableBlock[] insertBlocks = new UpdatableBlock[cols];
        Type[] types = new Type[cols];

        // extract the types of each column, and create updatableBlocks of the right type
        for (int c = 0; c < cols; c++) {
            // todo: this copies or creates arrays twice, introduce function reset to updatable blocks
            insertBlocks[c] = page.getBlock(c).makeUpdatable().newLike();
        }
        UpdatablePage updatablePage = new UpdatablePage(insertBlocks);
        int added = 0;
        for (int i = 0; i < size; i++) {
            Page row = page.getSingleValuePage(i);
            // assumes the entries actually exist, what if not.
            Map<Slice, TableDataPosition> hashTable = hashTables.get(tableId);
            Slice key = getKey(tableId, row);
            TableDataPosition tableDataPosition = hashTable.getOrDefault(key, null);

            if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.INS.ordinal()) {
                if (tableDataPosition != null) {
                    // The entry with this key value already exists, so we treat this insert like an update!
                    TableData tableData = tables.get(tableId);
                    UpdatablePage uPage = tableData.pages.get(tableDataPosition.pageNr);
                    uPage.updateRow(row, tableDataPosition.position);
                    // Do not need to update the hashTables, as neither hash nor the storage position changed.
                    updatesBytes += row.getSizeInBytes();
                    updatesCount += 1;
                    continue;
                }
                added++;
                for (int c = 0; c < cols; c++) {
                    Type type = types[c];
                    Class<?> javaType = type.getJavaType();
                    UpdatableBlock insertsBlock = insertBlocks[c];
                    // TODO: what about the other types? // not needed for LevelDB
                    if (javaType == boolean.class) {
                        // Uses a byte array
                        type.writeBoolean(insertsBlock, type.getBoolean(row.getBlock(c), 0));
                    }
                    else if (javaType == long.class) {
                        type.writeLong(insertsBlock, type.getLong(row.getBlock(c), 0));
                    }
                    else if (javaType == double.class) {
                        // uses long array
                        type.writeDouble(insertsBlock, type.getDouble(row.getBlock(c), 0));
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = type.getSlice(row.getBlock(c), 0);
                        type.writeSlice(insertsBlock, slice, 0, slice.length());
                    }
                    else {
                        System.out.println("MemoryPageStore writes an object!");
                        type.writeObject(insertsBlock, type.getObject(row.getBlock(c), 0));
                    }
                }
                insertsBytes += row.getSizeInBytes();
                insertsCount += 1;
            }
            else {
                TableData tableData = tables.get(tableId);
                UpdatablePage uPage = tableData.pages.get(tableDataPosition.pageNr);
                if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.UPD.ordinal()) {
                    uPage.updateRow(row, tableDataPosition.position);
                    // Do not need to update the hashTables, as neither hash nor the storage position changed.
                    updatesBytes += row.getSizeInBytes();
                    updatesCount += 1;
                }
                else if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.DEL.ordinal()) { // delete
                    uPage.deleteRow(tableDataPosition.position);
                    hashTable.remove(key);
                    added--;
                    tableData.decreaseNumberOfRows(1);
                    deletesBytes += row.getSizeInBytes();
                    deletesCount += 1;
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unkown type of insert for delta updates");
                }
            }
        }

        // Page containing only the inserts
        UpdatablePage inserts = new UpdatablePage(false, insertBlocks[0].getPositionCount(), insertBlocks);

        inserts.compact(); // not needed for leveldb

        long newSize = currentBytes + inserts.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;
        tablesDelta.get(tableId).add(inserts);



        long endTime = System.nanoTime();
        try {
            statisticsWriter.write(String.format("%d, %d, %d, %d, %d, %d, %d, %d\n", DeltaFlagRequest.globalDeltaUpdateCount, insertsBytes, deletesBytes, updatesBytes, insertsCount, deletesCount, updatesCount, (endTime - startTime)/1000));
            statisticsWriter.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return added;
    }

    public synchronized int addDelta(Long tableId, DeltaPage page)
    {
        long startTime = System.nanoTime();
        long updatesBytes = 0;
        long insertsBytes = 0;
        long deletesBytes = 0;
        long updatesCount = 0;
        long insertsCount = 0;
        long deletesCount = 0;
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        int cols = page.getChannelCount();
        int size = page.getPositionCount();

        UpdatableBlock[] insertBlocks = new UpdatableBlock[cols];
        Type[] types = new Type[cols];

        // extract the types of each column, and create updatableBlocks of the right type
        for (int c = 0; c < cols; c++) {
            ColumnInfo ci = indices.get(tableId).get(c);
            Type type = ci.getType();
            types[c] = type;
            // TODO: better value expected entries.
            insertBlocks[c] = ci.getType().createBlockBuilder(null, 2).makeUpdatable();
        }
        int added = 0;
        for (int i = 0; i < size; i++) {
            Page row = page.getSingleValuePage(i);
            // assumes the entries actually exist, what if not.
            Map<Slice, TableDataPosition> hashTable = hashTables.get(tableId);
            Slice key = getKey(tableId, row);
            TableDataPosition tableDataPosition = hashTable.getOrDefault(key, null);

            if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.INS.ordinal()) {
                if (tableDataPosition != null) {
                    // The entry with this key value already exists, so we treat this insert like an update!
                    TableData tableData = tables.get(tableId);
                    UpdatablePage uPage = tableData.pages.get(tableDataPosition.pageNr);
                    uPage.updateRow(row, tableDataPosition.position);
                    // System.out.println("Implicit insert");
                    // Do not need to update the hashTables, as neither hash nor the storage position changed.
                    updatesBytes += row.getSizeInBytes();
                    updatesCount += 1;
                    continue;
                }
                added++;
                for (int c = 0; c < cols; c++) {
                    Type type = types[c];
                    Class<?> javaType = type.getJavaType();
                    UpdatableBlock insertsBlock = insertBlocks[c];
                    // TODO: what about the other types? // not needed for LevelDB

                    if (javaType == boolean.class) {
                        // Uses a byte array
                        type.writeBoolean(insertsBlock, type.getBoolean(row.getBlock(c), 0));
                    }
                    else if (javaType == long.class) {
                        type.writeLong(insertsBlock, type.getLong(row.getBlock(c), 0));
                    }
                    else if (javaType == double.class) {
                        // uses long array
                        type.writeDouble(insertsBlock, type.getDouble(row.getBlock(c), 0));
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = type.getSlice(row.getBlock(c), 0);
                        type.writeSlice(insertsBlock, slice, 0, slice.length());
                    }
                    else {
                        System.out.println("MemoryPageStore writes an object!");
                        type.writeObject(insertsBlock, type.getObject(row.getBlock(c), 0));
                    }
                }
                insertsBytes += row.getSizeInBytes();
                insertsCount += 1;
            }
            else {
                TableData tableData = tables.get(tableId);
                UpdatablePage uPage = tableData.pages.get(tableDataPosition.pageNr);
                if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.UPD.ordinal()) {
                    uPage.updateRow(row, tableDataPosition.position);
                    // Do not need to update the hashTables, as neither hash nor the storage position changed.
                    updatesBytes += row.getSizeInBytes();
                    updatesCount += 1;
                }
                else if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.DEL.ordinal()) { // delete
                    uPage.deleteRow(tableDataPosition.position);
                    hashTable.remove(key);
                    added--;
                    tableData.decreaseNumberOfRows(1);
                    deletesBytes += row.getSizeInBytes();
                    deletesCount += 1;
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unkown type of insert for delta updates");
                }
            }
        }

        // Page containing only the inserts
        UpdatablePage inserts = new UpdatablePage(false, insertBlocks[0].getPositionCount(), insertBlocks);

        inserts.compact(); // not needed for leveldb

        long newSize = currentBytes + inserts.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        //TableData tableData = tables.get(tableId);
        //tableData.add(inserts);
        add(tableId, inserts);
        long endTime = System.nanoTime();
        try {
            statisticsWriter.write(String.format("%d, %d, %d, %d, %d, %d, %d, %d\n", DeltaFlagRequest.globalDeltaUpdateCount, insertsBytes, deletesBytes, updatesBytes, insertsCount, deletesCount, updatesCount, (endTime - startTime)/1000));
            statisticsWriter.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return added;
    }

    public List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        long sleepTime = 1; //sleep for 1 ms
        while (true) {
            // replace DeltaFlagRequest.globalDeltaUpdateInProcess with atomic
            boolean sleep = false;
            DeltaFlagRequest.deltaFlagLock.readLock().lock();
            if (DeltaFlagRequest.globalDeltaUpdateInProcess) {
                sleep = true;
                if(sleepTime < 1000) {
                    sleepTime *= 2;
                }
            }
            DeltaFlagRequest.deltaFlagLock.readLock().unlock();
            try {
                if (sleep) {
                    Thread.sleep(sleepTime);
                }
                else {
                    break;
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
        // Problems that could arise due to the locking:
        // in TaskSource::setDeltaUpdateFlag the request may timeout if this process here takes too long
        // -> change the timeout times in CoordinatorModule httpClientBinder(binder).bindHttpClient for ForDeltaUpdate

        // Also the engine might be working on the results of splits gotten before the delta update synchronization was activated
        // We must sure that all the tasks working on this data finish their derived splits before derivations of the delta split reach
        // them as new input

        // it takes the DeltaFlagRequest lock such that TaskSource::setDeltaUpdateFlag must wait for the current split to be processed
        synchronized (this) {
            ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();
            if (!contains(tableId)) {
                throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
            }
            TableData tableData = tables.get(tableId);
            if (tableData.getRows() != expectedRows) {
                throw new TrinoException(MISSING_DATA,
                        format("Expected to find [%s] rows on a worker, but found [%s]. Table had [%d] decreases.", expectedRows, tableData.getRows(), decreases));
            }

            boolean done = false;
            long totalRows = 0;
            for (int i = partNumber; i < tableData.getPages().size() && !done; i += totalParts) {
                if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                    continue;
                }

                UpdatablePage uPage = tableData.getPages().get(i);
                Page page = null;

                if (limit.isPresent()) {
                    // && totalRows > limit.getAsLong()
                    if (limit.getAsLong() - totalRows - uPage.getPositionCount() >= 0) {
                        // can safely read everything from the page
                        page = uPage.getEntriesFrom(0, uPage.getPositionCount());
                        totalRows += page.getPositionCount();
                    }
                    else {
                        // Get only what is needed
                        page = uPage.getEntriesFrom(0, (int) (limit.getAsLong() - totalRows));
                        totalRows += page.getPositionCount();
                        if (totalRows > limit.getAsLong()) {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "got more than the limit");
                        }
                    }
                }
                else {
                    // creating a genuine Page, not a Updatable page
                    // this forces a copy, do not want to return the actual data
                    page = uPage.getEntriesFrom(0, uPage.getPositionCount());
                    totalRows += page.getPositionCount();
                }
                // columns get copied here
                partitionedPages.add(getColumns(page, columnIndexes));
            }
            return partitionedPages.build();
        }
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
                for (UpdatablePage removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
                hashTables.remove(tableId);
                indices.remove(tableId);
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }
        // Do not need to copy the blocks as they were already copied when extracted from the pages
        return new Page(false, page.getPositionCount(), outputBlocks);
    }

    private final class TableData
    {
        private final List<UpdatablePage> pages = new ArrayList<>();
        private long rows;

        // TODO: check that it is synchronized
        public synchronized int add(UpdatablePage page)
        {
            pages.add(page);
            rows += page.getPositionCount(); // TODO: does count deleted rows, but it is never used on pages with deleted rows
            return pages.size();
        }
        // needs to be only called with a lock held
        public synchronized void decreaseNumberOfRows(int decrease)
        {
            rows -= decrease;
            decreases += decrease;
        }

        private synchronized List<UpdatablePage> getPages()
        {
            return pages;
        }

        private synchronized int getPageNumber()
        {
            return pages.size();
        }

        private synchronized long getRows()
        {
            return rows;
        }
    }

    private static final class TableDataPosition
    {
        public int position;
        public int pageNr;

        public TableDataPosition(int pageNr, int position)
        {
            this.pageNr = pageNr;
            this.position = position;
        }
    }

    //UpdatableLongArrayBlock(@Nullable BlockBuilderStatus blockBuilderStatus, int positionCount, byte[] valueMarker, long[] values, int nullCounter, int deleteCounter)
    private UpdatablePage make_updatable(Page page, long tableId)
    {
        int cols = page.getChannelCount();
        UpdatableBlock[] blocks = new UpdatableBlock[page.getChannelCount()];
        for (int i = 0; i < cols; i++) {
            Block b = page.getBlock(i).getLoadedBlock();
            try {
                blocks[i] = b.makeUpdatable();
            }catch(UnsupportedOperationException e){
                e.printStackTrace();
                System.out.println("A block used by "+ indices.get(tableId).get(i).getName()+ ". It is of type " + indices.get(tableId).get(i).getType());
            }
        }
        UpdatablePage newpage = new UpdatablePage(false, blocks[0].getPositionCount(), blocks);
        return newpage;
    }

    public void close(){
        try {
            this.statisticsWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
