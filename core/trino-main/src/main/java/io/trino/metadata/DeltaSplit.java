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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.spi.connector.ConnectorSplit;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class DeltaSplit
        extends Split
{
    @JsonCreator
    public DeltaSplit(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit,
            @JsonProperty("lifespan") Lifespan lifespan)
    {
        super(catalogName, connectorSplit, lifespan);
    }

    @Override
    @JsonProperty
    public CatalogName getCatalogName()
    {
        return super.getCatalogName();
    }

    @Override
    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return super.getConnectorSplit();
    }

    @Override
    @JsonProperty
    public Lifespan getLifespan()
    {
        return super.getLifespan();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", super.getCatalogName())
                .add("connectorSplit", super.getConnectorSplit())
                .add("lifespan", super.getLifespan())
                .toString();
    }
}
