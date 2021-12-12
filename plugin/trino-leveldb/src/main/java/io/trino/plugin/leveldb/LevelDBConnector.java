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
package io.trino.plugin.leveldb;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.trino.plugin.leveldb.LevelDBTransactionHandle.INSTANCE;

public class LevelDBConnector
        implements Connector
{
    private final LevelDBMetadata metadata;
    private final LevelDBSplitManager splitManager;
    private final LevelDBRecordSetProvider recordSetProvider;

    @Inject
    public LevelDBConnector()
    {
        LevelDBConfig config = new LevelDBConfig();
        // TODO: configs are not working
        System.out.println("ADDRESS: " + config.getAddress());
        System.out.println("PORT: " + config.getPort());
        // opening a tcp socket
        CommunicatorFactory comm = new CommunicatorFactory("::1", 7070);
        LevelDBInterface levelDBAPI = new LevelDBInterface(comm);
        this.metadata = new LevelDBMetadata(levelDBAPI);
        this.splitManager = new LevelDBSplitManager(levelDBAPI);
        this.recordSetProvider = new LevelDBRecordSetProvider(comm);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        // close all sockets, streams
    }
}
