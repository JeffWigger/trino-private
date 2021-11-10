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

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Objects.requireNonNull;

// Based on ClickHouseTableProperties and KuduTableProperties
public final class MemoryTableProperties
{
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public MemoryTableProperties()
    {
        tableProperties = ImmutableList.of(
                booleanProperty(
                        PRIMARY_KEY_PROPERTY,
                        "If column belongs to primary key",
                        false,
                        false));
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return tableProperties;
    }

    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY);
    }
}
