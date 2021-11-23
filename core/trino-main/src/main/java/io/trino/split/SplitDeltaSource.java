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
package io.trino.split;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.metadata.DeltaSplit;
import io.trino.metadata.Split;
import io.trino.spi.connector.ConnectorPartitionHandle;

import java.io.Closeable;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public interface SplitDeltaSource
        extends SplitSource
{

    ListenableFuture<SplitDeltaBatch> getNextDeltaBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize);

    default ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize){
        throw new UnsupportedOperationException(getClass().getName());
    }


    class SplitDeltaBatch
        extends SplitBatch
    {
        public SplitDeltaBatch(List<DeltaSplit> splits, boolean lastBatch)
        {
            super(splits.stream().map(Split.class::cast).collect(toImmutableList()), lastBatch);
        }

        public List<DeltaSplit> getDeltaSplits()
        {
            return splits.stream().map(DeltaSplit.class::cast).collect(toImmutableList());
        }
    }
}
