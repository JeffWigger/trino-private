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

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class LevelDBSplitManager
        implements ConnectorSplitManager
{
    private final LevelDBInterface levelDBAPI;

    @Inject
    public LevelDBSplitManager(LevelDBInterface levelDBAPI)
    {
        System.out.println("LevelDBSplitManager");
        this.levelDBAPI = levelDBAPI;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        System.out.println("LevelDBSplitManager::getSplits");
        LevelDBTableHandle tableHandle = (LevelDBTableHandle) connectorTableHandle;
        JsonNode json = levelDBAPI.getTableDesc(tableHandle.getTableName(), Optional.of(tableHandle.getSchemaName()));
        LevelDBTable table;
        if (!tableHandle.getTableName().equalsIgnoreCase(json.get("name").asText())) { // TODO can json be null here or will it just be empty
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }
        else {
            table = new LevelDBTable(json, levelDBAPI.getAddress(), levelDBAPI.getPort());
        }
        // each split gets executed on the levelDB server, so we would copy the same data server times
        // --> only have one

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new LevelDBSplit(table.getAddress(), table.getPort()));

        /*for (int i = 0; i < table.getColumns().size(); i++) {
            splits.add(new LevelDBSplit(table.getAddress(), table.getPort()));
        }*/
        //Collections.shuffle(splits);
        // TODO: check alternative implementations of ConnectorSplitSource
        System.out.println("LevelDBSplitManager::getSplits end");
        return new FixedSplitSource(splits);
    }
}
