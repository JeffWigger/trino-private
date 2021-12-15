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
package io.trino.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DeltaFlagRequest
{
    private final boolean deltaUpdateInProcess;
    private final int deltaUpdateCount;
    public static final ReentrantReadWriteLock deltaFlagLock = new ReentrantReadWriteLock(true);

    // use synchronized(DeltaFlagRequest.class)
    @GuardedBy("deltaFlagLock")
    public static boolean globalDeltaUpdateInProcess = false;

    public static int globalDeltaUpdateCount= 0;


    @JsonCreator
    public DeltaFlagRequest(
            @JsonProperty("deltaUpdateInProcess") boolean deltaUpdateInProcess,
            @JsonProperty("deltaUpdateCount") int deltaUpdateCount)
    {
        //requireNonNull(deltaUpdateInProcess, "deltaUpdateInProcess is null");
        this.deltaUpdateInProcess = deltaUpdateInProcess;
        this.deltaUpdateCount = deltaUpdateCount;
    }

    @JsonProperty
    public boolean getDeltaUpdateInProcess()
    {
        return deltaUpdateInProcess;
    }

    @JsonProperty
    public int getDeltaUpdateCount()
    {
        return deltaUpdateCount;
    }


    @Override
    public String toString()
    {
        if (deltaUpdateInProcess){
            return "deltaUpdateInProcess::true;deltaUpdateCount::"+deltaUpdateCount;
        }else{
            return "deltaUpdateInProcess::false;deltaUpdateCount::"+deltaUpdateCount;
        }
    }
}