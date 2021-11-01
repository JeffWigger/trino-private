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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LevelDBMetadata
        implements ConnectorMetadata
{
    LevelDBInterface levelDBAPI;

    public LevelDBMetadata(LevelDBInterface levelDBAPI)
    {
        this.levelDBAPI = levelDBAPI;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        LinkedList<String> ll = new LinkedList<String>();
        // TODO: could maybe use a schema for the delta structure
        ll.add("default");
        return ll;
    }

    @Override
    public LevelDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        System.out.println("getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        List<String> tables = levelDBAPI.getTableNames(Optional.empty());
        // TODO: This is unfortunate
        for (String str: tables){
            System.out.println(str);
        }
        if (!tables.contains(tableName.getTableName().toUpperCase(Locale.ENGLISH))) {
            System.out.println("getTableHandle table "+tableName.getTableName().toUpperCase(Locale.ENGLISH)+" not included in tables");
            return null;
        }

        return new LevelDBTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        System.out.println("getTableMetadata");
        return getTableMetadata(((LevelDBTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        System.out.println("listTables");
        List<String> schemaNames;
        if (optionalSchemaName.isPresent()) {
            schemaNames = new ArrayList<String>();
            schemaNames.add(optionalSchemaName.get());
        }
        else {
            schemaNames = this.listSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : levelDBAPI.getTableNames(Optional.of(schemaName))) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        System.out.println("getColumnHandles");
        LevelDBTableHandle levelDBTableHandle = (LevelDBTableHandle) tableHandle;

        JsonNode json = levelDBAPI.getTableDesc(levelDBTableHandle.getTableName(), Optional.of(levelDBTableHandle.getSchemaName()));
        LevelDBTable table;
        if (!levelDBTableHandle.getTableName().equalsIgnoreCase(json.get("name").asText())) { // TODO can json be null here or will it just be empty
            throw new TableNotFoundException(levelDBTableHandle.toSchemaTableName());
        }
        else {
            table = new LevelDBTable(json, levelDBAPI.getAddress(), levelDBAPI.getPort());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (LevelDBColumn column : table.getColumns()) {
            columnHandles.put(column.getName(), new LevelDBColumnHandle(column.getName(), column.getType(), column.getIndex()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        System.out.println("listTableColumns");
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        System.out.println("getTableMetadata");
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        JsonNode json = levelDBAPI.getTableDesc(tableName.getTableName(), Optional.of(tableName.getSchemaName()));
        LevelDBTable table;
        System.out.println("getTableMetadata cmp: "+ tableName.getTableName() +" : "+json.get("name").asText() );
        if (!tableName.getTableName().equalsIgnoreCase(json.get("name").asText())) { // TODO can json be null here or will it just be empty
            System.out.println("getTableMetadata: not equal");
            return null;
        }
        else {
            System.out.println("getTableMetadata: Creating LevelDBTable");
            table = new LevelDBTable(json, levelDBAPI.getAddress(), levelDBAPI.getPort());
        }
        ConnectorTableMetadata data = new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        System.out.println("end getTableMetadata");
        return data;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        System.out.println("getColumnMetadata");
        return ((LevelDBColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        System.out.println("usesLegacyTableLayouts");
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        System.out.println("getTableProperties");
        //TODO ??
        return new ConnectorTableProperties();
    }
}
