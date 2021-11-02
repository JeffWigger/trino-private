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
package io.trino.sql.tree;
//based on tree.Insert

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaUpdate
        extends Statement
{
    private final QualifiedName target;
    private final QualifiedName source;

    public DeltaUpdate(QualifiedName target, QualifiedName source)
    {
        this(Optional.empty(), target, source);
    }

    public DeltaUpdate(NodeLocation location, QualifiedName target, QualifiedName source)
    {
        this(Optional.of(location), target, source);
    }

    public DeltaUpdate(Optional<NodeLocation> location, QualifiedName target, QualifiedName source)
    {
        super(location);
        this.target = requireNonNull(target, "target is null");
        this.source = requireNonNull(source, "source is null");
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public QualifiedName getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeltaUpdate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, source);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        DeltaUpdate o = (DeltaUpdate) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(source, o.source);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("source", source)
                .toString();
    }
}
