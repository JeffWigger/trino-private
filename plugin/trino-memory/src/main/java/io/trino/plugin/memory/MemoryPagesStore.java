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
import io.trino.spi.DeltaPageBuilder.Mode;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.UpdatablePage;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
    private final Map<Long, List<DeltaPage>[]> tablesDelta = new HashMap<>();

    // Hashtables that map primary keys to the storage position.
    // TODO: chose a good initial capacity
    // This will be unaccounted for in the tracked storage size
    @GuardedBy("this")
    private final Map<Long, Map<Slice, TableDataPosition>> hashTables = new HashMap<>();
    // private Map<Long, Map<Slice, TableDataPosition>> hashTablesDelta = new HashMap<>();,preprocessing deltas has little value, as for merging the hashTable we do the same amount of work, or could use putall, but it is not more efficient, it calls put for all entreis
    private final Map<Long, List<ColumnInfo>> indices = new HashMap<>();

    private FileWriter statisticsWriter;
    private FileWriter integrationWriter;

    private final int splitsPerNode;
    private long currentBucketCounter;

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
        splitsPerNode = config.getSplitsPerNode();
        String fileDirectory = "/scratch/wigger/";
        String statisticsFileName = "MemoryPagesStore";
        String BatchIntegrationFileName = "BatchIntegration";

        try {
            Path dir = Paths.get(fileDirectory);
            Files.createDirectories(dir);
            Path statisticsFilePath = dir.resolve(BatchIntegrationFileName);
            File file = new File(statisticsFilePath.toString());
            if(!file.exists()){
                file.createNewFile();
            }
            File integrationFile = new File(statisticsFilePath.toString());
            if(!integrationFile.exists()){
                integrationFile.createNewFile();
            }
            assert(integrationFile.exists() && integrationFile.canWrite());
            integrationWriter = new FileWriter(integrationFile, true);
            integrationWriter.write(String.format("UpdateNr,Added,Deleted,MicroSeconds\n"));


            statisticsFilePath = dir.resolve(statisticsFileName);
            File file2 = new File(statisticsFilePath.toString());
            if(!file2.exists()){
                file2.createNewFile();
            }
            assert(file2.exists() && file.canWrite());
            statisticsWriter = new FileWriter(file2, true);
            statisticsWriter.write(String.format("UpdateNr,InsertBytes,DeleteBytes,UpdateBytes,InsertCount,DeleteCount,UpdateCount,MicroSeconds\n"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        DeltaFlagRequest.registerCallback(() -> applyDeltas());

    }

    private synchronized int nextBucket(){
        return (int) (currentBucketCounter++) % splitsPerNode;
    }

    public synchronized void initialize(long tableId, List<ColumnInfo> indices)
    {
        // TODO: should update the indices in case a new column was added.
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData(splitsPerNode));
            // TODO: add cleanup for this
            List<DeltaPage>[] pages = new ArrayList[splitsPerNode];
            for(int i = 0; i < splitsPerNode; i++){
                pages[i] = new ArrayList<>();
            }
            tablesDelta.put(tableId, pages);
            // TODO: add cleanup for this
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
        int bucket = nextBucket();
        int pageNr = tableData.getPageNumber(bucket); // + 1; getPageNumber returns 0 for the first page that gets added.
        for (int i = 0; i < pageSize; i++) {
            Page row = page.getSingleValuePage(i);
            Slice key = getKey(tableId, row);
            TableDataPosition tableDataPosition = htable.getOrDefault(key, null);
            if (tableDataPosition != null) {
                // entry is already in the DB
                // implicit update
                // UpdatablePage uPage = tableData.pages[tableDataPosition.bucket].get(tableDataPosition.pageNr);
                // uPage.updateRow(row, tableDataPosition.position);
                // This updates the old records and leaves the current one in the page, --> have untracked data
                // continue;
                //TODO: Need to decide how to handle this


                // Best solution for now is to delete the old record
                UpdatablePage uPage = tableData.pages[tableDataPosition.bucket].get(tableDataPosition.pageNr);
                uPage.deleteRow(tableDataPosition.position);
                // Now we add the entry in the new page to the hash table
                added--;
                tableData.decreaseNumberOfRows(1);
            }
            htable.put(key, new TableDataPosition(bucket, pageNr, i));
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

        tableData.add(updatablePage, bucket);
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

    private String getKeyAsString(long tableId, Page row)
    {
        List<ColumnInfo> indecies = this.indices.get(tableId);
        // 10 is a good enough estimated size
        StringBuilder key = new StringBuilder();
        for (ColumnInfo ci : indecies) {
            if (ci.isPrimaryKey()) {// should always be the case
                Type type = ci.getType();
                Block block = row.getBlock(((MemoryColumnHandle) ci.getHandle()).getColumnIndex());
                // from RecordPageSource
                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    boolean b = type.getBoolean(block, 0);
                    key.append(b);
                    key.append("|");
                }
                else if (javaType == long.class) {
                    long l = type.getLong(block, 0);
                    key.append(l);
                    key.append("|");
                }
                else if (javaType == double.class) {
                    double d = type.getDouble(block, 0);
                    key.append(d);
                    key.append("|");
                }
                else if (javaType == Slice.class) {
                    Slice s = type.getSlice(block, 0);
                    key.append(s.toStringUtf8());
                    key.append("|");
                }
                else {
                    // TODO: THIS PROBABLY DOES NOT WORK!
                    Object o = type.getObject(block, 0);
                    key.append(o.toString());
                    key.append("|");
                }
            }
            //else {
            //  throw new TrinoException(GENERIC_INTERNAL_ERROR, "got index column that is not an index");
            //}
        }
        return key.toString();
    }

    public synchronized void applyDeltas(){
        // we do not further break up the pages, as they should already be small enough, else we need to make sure that we do not split at a deleted followed by an insert
        // as that may simulate an update.
        int added = 0;
        int delTotal = 0;
        long startTime = System.nanoTime();
        // TODO add timing, and logging
        for(Map.Entry<Long, List<DeltaPage>[]> entry : tablesDelta.entrySet()){
            long tableId = entry.getKey();

            for(int i = 0; i < splitsPerNode; i++) {
                List<DeltaPage> dPages = entry.getValue()[i];
                int numberOfPages = dPages.size();
                for(int j = 0; j < numberOfPages; j++){
                    DeltaPage dpage = dPages.get(j);
                    int entries = dpage.getPositionCount();
                    UpdatablePage pageBuilder = make_updatable(dpage, tableId);
                    Slice prevKey = null;
                    TableDataPosition prevTableDataPosition = null;
                    int deletes = 0;
                    for(int k = 0; k < entries; k++){
                        DeltaPage row = dpage.getSingleValuePage(k);
                        Map<Slice, TableDataPosition> hashTable = hashTables.get(tableId);
                        Slice key = getKey(tableId, row);
                        TableDataPosition tableDataPosition = hashTable.getOrDefault(key, null);

                        TableData tableData = tables.get(tableId);
                        int pageNr = tableData.getPageNumber(i);
                        if(row.getUpdateType().getByte(0,0) == Mode.DEL.ordinal()){
                            pageBuilder.deleteRow(k);
                            if(tableDataPosition != null){
                                UpdatablePage uPage = tableData.pages[tableDataPosition.bucket].get(tableDataPosition.pageNr);
                                uPage.deleteRow(tableDataPosition.position);
                                hashTable.remove(key);
                                added--;
                                deletes++;
                                tableData.decreaseNumberOfRows(1);
                                prevKey = key;
                                prevTableDataPosition = tableDataPosition;
                            }else{
                                System.err.println("Error: key: "+ getKeyAsString(tableId, row));
                                throw new TrinoException(GENERIC_INTERNAL_ERROR, "applyDelta delete without row in hashTables");
                            }
                        }else if(row.getUpdateType().getByte(0,0) == Mode.INS.ordinal()){
                            if(tableDataPosition != null){
                                // TODO: can happen if batch 1 adds a record and then Batch 2 adds the same record, but as a "update"
                                // after batch 1 was preprocessed batch 2 does not know about the changes in batch 1!

                                // should never happen as we simulate updates as delete and insert
                                throw new TrinoException(GENERIC_INTERNAL_ERROR, "applyDelta insert with being in hashTables");
                            }else{
                                added++;
                                // for the simulated deletes I need to know the bucket
                                // would want to add it to the same table to, to fill the hole again
                                // theoretically could add it to any pageNr as during delta update there are no queries going on
                                if(prevKey != null && prevKey.equals(key)){
                                    hashTable.put(key, prevTableDataPosition);
                                    UpdatablePage uPage = tableData.pages[prevTableDataPosition.bucket].get(prevTableDataPosition.pageNr);
                                    uPage.updateRow(row, prevTableDataPosition.position);
                                }else{
                                    // upage already contains the entry
                                    hashTable.put(key, new TableDataPosition(i, pageNr, k - deletes)); // as we will compact the upage
                                }
                            }
                            prevKey = null;
                            prevTableDataPosition = null;
                        } //TODO: upd
                    }
                    delTotal += deletes;
                }
                // Removing the entries that we just now added from the tablesDelta
                entry.getValue()[i].clear();
                assert tablesDelta.get(tableId)[i].size() == 0;
            }
        }
        long endTime = System.nanoTime();
        try {
            integrationWriter.write(String.format("%d,%d,%d,%d\n", DeltaFlagRequest.globalDeltaUpdateCount, added, delTotal,(endTime - startTime)/1000));
            integrationWriter.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void pageBuilderAddRecord(DeltaPageBuilder pageBuilder, ArrayList<Type> types, Page row, Mode mode){
        // based on RecordPageSource::getNextPage
        pageBuilder.declarePosition();
        for (int c = 0; c < types.size(); c++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(c);
            if (row.getBlock(c).isNull(0)) {
                output.appendNull();
            }
            else {
                Type type = types.get(c);
                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    // Uses a byte array
                    type.writeBoolean(output, type.getBoolean(row.getBlock(c), 0));
                }
                else if (javaType == long.class) {
                    type.writeLong(output, type.getLong(row.getBlock(c), 0));
                }
                else if (javaType == double.class) {
                    // uses long array
                    type.writeDouble(output, type.getDouble(row.getBlock(c), 0));
                }
                else if (javaType == Slice.class) {
                    Slice slice = type.getSlice(row.getBlock(c), 0);
                    type.writeSlice(output, slice, 0, slice.length());
                }
                else {
                    System.out.println("MemoryPageStore writes an object!");
                    type.writeObject(output, type.getObject(row.getBlock(c), 0));
                }
            }
        }
        pageBuilder.updateBuilder.writeByte(mode.ordinal());
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

        ArrayList<Type> types = new ArrayList<>(cols);

        // extract the types of each column, and create updatableBlocks of the right type
        for (int c = 0; c < cols; c++) {
            ColumnInfo ci = indices.get(tableId).get(c);
            Type type = ci.getType();
            types.add(type);
        }
        DeltaPageBuilder pageBuilder[] = new DeltaPageBuilder[splitsPerNode];
        for(int i = 0; i < splitsPerNode; i++) {
            pageBuilder[i] = new DeltaPageBuilder(types);
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
                    UpdatablePage uPage = tableData.pages[tableDataPosition.bucket].get(tableDataPosition.pageNr);

                    // TODO: what if the same value gets deleted several times in the same delta update ????, this works for the experimental data
                    pageBuilderAddRecord(pageBuilder[tableDataPosition.bucket], types, uPage.getSingleValuePage(tableDataPosition.position), Mode.DEL);
                    pageBuilderAddRecord(pageBuilder[tableDataPosition.bucket], types, row, Mode.INS);
                    // Do not need to update the hashTables, as neither hash nor the storage position changed.
                    updatesBytes += row.getSizeInBytes();
                    updatesCount += 1;
                    continue;
                }
                added++;
                int bucket = nextBucket();
                pageBuilderAddRecord(pageBuilder[bucket], types, row, Mode.INS);
                insertsBytes += row.getSizeInBytes();
                insertsCount += 1;
            }
            else {
                if (tableDataPosition != null) {
                    TableData tableData = tables.get(tableId);
                    UpdatablePage uPage = tableData.pages[tableDataPosition.bucket].get(tableDataPosition.pageNr);
                    if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.UPD.ordinal()) {
                        // TODO: what if the same value gets deleted several times in the same delta update ????, this works for the experimental data
                        pageBuilderAddRecord(pageBuilder[tableDataPosition.bucket], types, uPage.getSingleValuePage(tableDataPosition.position), Mode.DEL);
                        pageBuilderAddRecord(pageBuilder[tableDataPosition.bucket], types, row, Mode.INS);

                        // Do not need to update the hashTables, as neither hash nor the storage position changed.
                        updatesBytes += row.getSizeInBytes();
                        updatesCount += 1;
                    }
                    else if (page.getUpdateType().getByte(i, 0) == (byte) DeltaPageBuilder.Mode.DEL.ordinal()) { // delete
                        // TODO: what if the same value gets deleted several times in the same delta update ????, this works for the experimental data
                        pageBuilderAddRecord(pageBuilder[tableDataPosition.bucket], types, uPage.getSingleValuePage(tableDataPosition.position), Mode.DEL);
                        hashTable.remove(key);
                        added--;
                        //tableData.decreaseNumberOfRows(1);
                        deletesBytes += row.getSizeInBytes();
                        deletesCount += 1;
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unkown type of insert for delta updates");
                    }
                }else{
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Wanting to delete or update a record that is not yet in the memory table");
                }
            }
        }
        for(int i = 0; i < splitsPerNode; i++){
            tablesDelta.get(tableId)[i].add(pageBuilder[i].build());
        }
        long endTime = System.nanoTime();
        try {
            statisticsWriter.write(String.format("%d, %d, %d, %d, %d, %d, %d, %d\n", DeltaFlagRequest.globalDeltaUpdateCount, insertsBytes, deletesBytes, updatesBytes, insertsCount, deletesCount, updatesCount, (endTime - startTime)/1000));
            statisticsWriter.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return 0; //added; TODO: need to find a good solution here
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
        // Problems that could arise due to the locking:
        // in TaskSource::setDeltaUpdateFlag the request may timeout if this process here takes too long
        // -> change the timeout times in CoordinatorModule httpClientBinder(binder).bindHttpClient for ForDeltaUpdate

        // Also the engine might be working on the results of splits gotten before the delta update synchronization was activated
        // We must sure that all the tasks working on this data finish their derived splits before derivations of the delta split reach
        // them as new input

        assert(totalParts == splitsPerNode);

        // it takes the DeltaFlagRequest lock such that TaskSource::setDeltaUpdateFlag must wait for the current split to be processed
        synchronized (this) {
            ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();
            if (!contains(tableId)) {
                throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
            }
            TableData tableData = tables.get(tableId);
            // TODO: Currently cannot support it as addDelta does not directly change the data.
            /*if (tableData.getRows() != expectedRows) {
                throw new TrinoException(MISSING_DATA,
                        format("Expected to find [%s] rows on a worker, but found [%s]. Table had [%d] decreases.", expectedRows, tableData.getRows(), decreases));
            }*/

            boolean done = false;
            long totalRows = 0;

            List<UpdatablePage> uPages = tableData.getPages(partNumber);
            for (UpdatablePage uPage : uPages) {
                if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                    continue;
                }

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
        // TODO: cleanup for hashtables used for delta updates, low priority as it cannot happen during experimental setup

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for(int i = 0; i < splitsPerNode; i++) {
                    for (UpdatablePage removedPage : tablePagesEntry.getValue().getPages(i)) {
                        currentBytes -= removedPage.getRetainedSizeInBytes();
                    }
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
        private final List<UpdatablePage>[] pages;
        private long rows;

        TableData(int buckets){
            pages = new ArrayList[buckets];
            for(int i = 0; i < buckets; i++){
                pages[i] = new ArrayList<>();
            }
        }

        // TODO: check that it is synchronized
        public synchronized int add(UpdatablePage page, int bucket)
        {
            pages[bucket].add(page);
            rows += page.getPositionCount(); // TODO: does count deleted rows, but it is never used on pages with deleted rows
            return pages[bucket].size();
        }
        // needs to be only called with a lock held
        public synchronized void decreaseNumberOfRows(int decrease)
        {
            rows -= decrease;
            decreases += decrease;
        }

        private synchronized List<UpdatablePage> getPages(int bucket)
        {
            return pages[bucket];
        }

        private synchronized int getPageNumber(int bucket)
        {
            return pages[bucket].size();
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
        public int bucket;

        public TableDataPosition(int bucket, int pageNr, int position)
        {
            this.bucket = bucket;
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
            this.integrationWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
