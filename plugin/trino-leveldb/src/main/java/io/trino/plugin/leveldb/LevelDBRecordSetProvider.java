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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

public class LevelDBRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final CommunicatorFactory commFactory;

    public LevelDBRecordSetProvider(CommunicatorFactory commFactory)
    {
        this.commFactory = commFactory;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        System.out.println("LevelDBRecordSetProvider::getRecordSet");
        LevelDBSplit levelDBSplit = (LevelDBSplit) split;

        ImmutableList.Builder<LevelDBColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((LevelDBColumnHandle) handle);
        }
        LevelDBTableHandle tableHandle = (LevelDBTableHandle) table;
        // cannot use deltaupdate as it is now a sql statement
        System.out.println("LevelDBRecordSetProvider: "+ tableHandle.getSchemaName());
        if (tableHandle.getSchemaName().equals("dupdate")){
            return new LevelDBDeltaRecordSet(tableHandle, levelDBSplit, handles.build(), commFactory);
        }else{
            return new LevelDBRecordSet((LevelDBTableHandle) table, levelDBSplit, handles.build(), commFactory);
        }

    }
}
