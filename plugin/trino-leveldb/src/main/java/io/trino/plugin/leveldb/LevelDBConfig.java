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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class LevelDBConfig
{
    private String address;
    private int port;

    @NotNull
    public String getAddress()
    {
        return address;
    }

    @Config("ipv6-address")
    public LevelDBConfig setAddress(String address)
    {
        this.address = address;
        return this;
    }

    @NotNull
    public int getPort()
    {
        return port;
    }

    @Config("ipv6-port")
    public LevelDBConfig setPort(int port)
    {
        this.port = port;
        return this;
    }
}
