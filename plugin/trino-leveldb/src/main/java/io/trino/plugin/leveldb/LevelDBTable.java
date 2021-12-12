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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class LevelDBTable
{
    private final String name;
    private final List<LevelDBColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final String address;
    private final int port;

    @JsonCreator
    public LevelDBTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<LevelDBColumn> columns,
            @JsonProperty("address") String address,
            @JsonProperty("address") int port)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.address = address;
        this.port = port;

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (LevelDBColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public LevelDBTable(JsonNode description, String address, int port)
    {
        this(description.get("name").asText(), getColumnList(description), address, port);
    }

    private static List<LevelDBColumn> getColumnList(JsonNode description)
    {
        System.out.println("LevelDBTable::getColumnList");
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<Map<String, Object>> cols = null;
        try {
            cols = mapper.treeToValue(description.get("columns"), ArrayList.class);
        }
        catch (JsonProcessingException e) {
            System.out.println("LevelDBTable::getColumnList Could not parse json");
            e.printStackTrace();
        }
        ArrayList<LevelDBColumn> columns = new ArrayList<LevelDBColumn>();
        // TODO: we build a similar list three times -> optimize
        for (Map<String, Object> col : cols) {
            LevelDBColumn c = new LevelDBColumn((String) col.get("name"), checkNotNull(LevelDBColumn.type_converter((String) col.get("type")), "Failed to convert %s::%s", description.get("name"), col.get("name")), Integer.valueOf((Integer) col.get("index")));
            columns.add(c);
        }
        System.out.println("LevelDBTable::getColumnList end");
        return columns;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<LevelDBColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public String getAddress()
    {
        return address;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
