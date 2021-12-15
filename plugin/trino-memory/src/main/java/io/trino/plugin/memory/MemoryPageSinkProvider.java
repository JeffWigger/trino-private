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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.DeltaPage;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryPagesStore pagesStore;
    private final HostAddress currentHostAddress;

    @Inject
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, NodeManager nodeManager)
    {
        this(pagesStore, requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, HostAddress currentHostAddress)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        MemoryOutputTableHandle memoryOutputTableHandle = (MemoryOutputTableHandle) outputTableHandle;
        long tableId = memoryOutputTableHandle.getTable();
        checkState(memoryOutputTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId, memoryOutputTableHandle.getIndecies());
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        MemoryInsertTableHandle memoryInsertTableHandle = (MemoryInsertTableHandle) insertTableHandle;
        long tableId = memoryInsertTableHandle.getTable();
        checkState(memoryInsertTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryInsertTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId, memoryInsertTableHandle.getIndecies());
        return new MemoryPageSink(pagesStore, currentHostAddress, tableId);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final MemoryPagesStore pagesStore;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private long addedRows;

        public MemoryPageSink(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            int added = 0;
            if (page instanceof DeltaPage){
                DeltaPage dpage = (DeltaPage) page;
                //System.out.println("MemoryPageSinkProvider: Page is a DeltaPage: "+ dpage.getClass().getName());
                added = pagesStore.addDelta(tableId, dpage);
            }else {
                //System.out.println("MemoryPageSinkProvider: Page got type: " + page.getClass().getName());
                added = pagesStore.add(tableId, page);
            }
            addedRows += added;
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, addedRows).toSlice()));
        }

        @Override
        public void abort()
        {
        }
    }
}
