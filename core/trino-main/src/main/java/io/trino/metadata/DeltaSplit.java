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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorDeltaSplit;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DeltaSplit
    extends Split
{

    @JsonCreator
    public DeltaSplit(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("connectorSplit") ConnectorDeltaSplit connectorSplit,
            @JsonProperty("lifespan") Lifespan lifespan)
    {
        super(catalogName, connectorSplit, lifespan);

    }

    @JsonProperty
    public ConnectorDeltaSplit getConnectorDeltaSplit()
    {
        return (ConnectorDeltaSplit) this.connectorSplit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("connectorDeltaSplit", connectorSplit)
                .add("lifespan", lifespan)
                .toString();
    }
}
