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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Locale;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class LevelDBColumn
{
    private final String name;
    private final Type type;
    private final int index;

    @JsonCreator
    public LevelDBColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("type") int index)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.index = index;
    }

    static Type type_converter(String typeName)
    {
        String type = typeName.toLowerCase(Locale.ENGLISH).trim();
        switch (type) {
            case "bigint":
                return BigintType.BIGINT;
            case "int":
                return IntegerType.INTEGER;
            case "decimal":
                return DecimalType.createDecimalType(12, 2);
            case "date":
                return DateType.DATE;
            default:
                if (type.contains("varchar")) {
                    return VarcharType.createVarcharType(Integer.valueOf(type.substring(type.indexOf('(') + 1, type.indexOf(')'))));
                }
                else if (type.contains("char")) {
                    return CharType.createCharType(Integer.valueOf(type.substring(type.indexOf('(') + 1, type.indexOf(')'))));
                }
        }
        System.err.println("Could not convert " + typeName + " to a Trino type");
        return null;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getIndex()
    {
        return index;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LevelDBColumn other = (LevelDBColumn) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.index, other.index);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }
}
