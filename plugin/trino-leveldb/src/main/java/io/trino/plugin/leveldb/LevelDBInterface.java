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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LevelDBInterface
{
    private final CommunicatorFactory commFactory;

    LevelDBInterface(CommunicatorFactory commFactory)
    {
        this.commFactory = commFactory;
    }

    public JsonNode getTableDesc(String tableName, Optional<String> schemaName)
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        map.put("msg_type", "TableDesc");
        map.put("schema", schemaName.orElse("default"));
        map.put("table", tableName);
        Communicator comm = commFactory.getCommunicator();
        comm.write_json(mapper.valueToTree(map));
        JsonNode json = comm.read_json();
        comm.close();
        return json;
    }

    public List<String> getTableNames(Optional<String> schema)
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        map.put("msg_type", "TableNames");
        map.put("schema", schema.orElse("default"));

        Communicator comm = commFactory.getCommunicator();
        comm.write_json(mapper.valueToTree(map));
        JsonNode json = comm.read_json();
        ArrayList<String> tables = null;
        try {
            tables = mapper.treeToValue(json.get("tables"), ArrayList.class);
            for(String str : tables){
                System.out.println(str);
                System.out.println(str.charAt(0) == '\n');
                System.out.println(str.charAt(str.length()-1) == '\n');

            }
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        comm.close();

        return tables;
    }

    public String getAddress()
    {
        return this.commFactory.getAddress();
    }

    public int getPort()
    {
        return this.commFactory.getPort();
    }
}
