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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.NewTableLayout;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
// based on TableWriterNode
@Immutable
public class DeltaUpdateNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final Symbol rowCountSymbol;
    //private final Symbol fragmentSymbol;
    private final List<Symbol> outputs;

    @JsonCreator
    public DeltaUpdateNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") List<PlanNode> sources,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol)
            //@JsonProperty("fragmentSymbol") Symbol fragmentSymbol)
    {
        super(id);

        this.sources = requireNonNull(sources, "source is null");
        this.rowCountSymbol = requireNonNull(rowCountSymbol, "rowCountSymbol is null");
        //this.fragmentSymbol = requireNonNull(fragmentSymbol, "fragmentSymbol is null");

        ImmutableList.Builder<Symbol> outputs = ImmutableList.<Symbol>builder()
                .add(rowCountSymbol);
                //.add(fragmentSymbol);
        this.outputs = outputs.build();
    }

    @JsonProperty
    public Symbol getRowCountSymbol()
    {
        return rowCountSymbol;
    }

    /*@JsonProperty
    public Symbol getFragmentSymbol()
    {
        return fragmentSymbol;
    }*/

    @JsonProperty
    @Override
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeltaUpdate(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new DeltaUpdateNode(getId(), newChildren, rowCountSymbol); //fragmentSymbol
    }
}
