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
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class MemoryOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final long table;
    private final Set<Long> activeTableIds;
    private final List<ColumnInfo> indecies;

    @JsonCreator
    public MemoryOutputTableHandle(
            @JsonProperty("table") long table,
            @JsonProperty("activeTableIds") Set<Long> activeTableIds,
            @JsonProperty("indecies") List<ColumnInfo> indecies)
    {
        this.table = table;
        this.activeTableIds = requireNonNull(activeTableIds, "activeTableIds is null");
        this.indecies = indecies; // can be null!
    }

    @JsonProperty
    public long getTable()
    {
        return table;
    }

    @JsonProperty
    public Set<Long> getActiveTableIds()
    {
        return activeTableIds;
    }

    @JsonProperty
    public List<ColumnInfo> getIndecies(){
        return this.indecies;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("activeTableIds", activeTableIds)
                .toString();
    }
}
