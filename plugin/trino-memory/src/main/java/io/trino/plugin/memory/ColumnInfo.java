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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    private final ColumnHandle handle;
    private final String name;
    private final Type type;
    private final boolean primaryKey;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty("columnHandle") ColumnHandle handle,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("primaryKey") boolean primaryKey)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.primaryKey = primaryKey;
    }

    @JsonProperty
    public ColumnHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    //@JsonProperty
    public ColumnMetadata getMetadata()
    {
        return new ColumnMetadata(name, type);
    }

    @JsonProperty
    public boolean isPrimaryKey(){
        return primaryKey;
    }

    @JsonProperty
    public  Type getType(){
        return type;
    }

    @Override
    public String toString()
    {
        return name + "::" + type +"::"+ primaryKey;
    }
}
