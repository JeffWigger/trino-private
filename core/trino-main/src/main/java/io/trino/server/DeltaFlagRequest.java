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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.SessionRepresentation;
import io.trino.execution.TaskSource;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaFlagRequest
{
    private final boolean deltaUpdateInProcess;

    // use synchronized(DeltaFlagRequest.class)
    @GuardedBy("DeltaFlagRequest.class")
    public static boolean globalDeltaUpdateInProcess = false;

    @JsonCreator
    public DeltaFlagRequest(
            @JsonProperty("deltaUpdateInProcess") boolean deltaUpdateInProcess)
    {
        //requireNonNull(deltaUpdateInProcess, "deltaUpdateInProcess is null");
        this.deltaUpdateInProcess = deltaUpdateInProcess;
    }

    @JsonProperty
    public boolean getDeltaUpdateInProcess()
    {
        return deltaUpdateInProcess;
    }


    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("deltaUpdateInProcess", deltaUpdateInProcess)
                .toString();
    }
}
