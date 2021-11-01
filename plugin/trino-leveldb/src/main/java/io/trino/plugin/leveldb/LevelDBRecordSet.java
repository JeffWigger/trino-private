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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LevelDBRecordSet
        implements RecordSet
{
    private final List<LevelDBColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final CommunicatorFactory commFactory;
    private final LevelDBSplit split;
    private final LevelDBTableHandle table;

    public LevelDBRecordSet(LevelDBTableHandle table, LevelDBSplit split, List<LevelDBColumnHandle> columnHandles, CommunicatorFactory commFactory)
    {
        System.out.println("LevelDBRecordSet");
        this.table = table;
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LevelDBColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        this.commFactory = commFactory;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LevelDBRecordCursor(this.table, columnHandles, this.commFactory);
    }
}
