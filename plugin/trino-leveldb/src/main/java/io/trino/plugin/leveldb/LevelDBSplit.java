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
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

public class LevelDBSplit
        implements ConnectorSplit
{
    private final String address;
    private final int port;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;

    @JsonCreator
    public LevelDBSplit(
            @JsonProperty("address") String address,
            @JsonProperty("port") int port)
    {
        System.out.println("LevelDBSplit: " + address + " port: " + port);
        this.address = address;
        this.port = port;

        remotelyAccessible = true; // TODO
        addresses = ImmutableList.of(HostAddress.fromParts(address, port));
    }

    @JsonProperty
    public String getAddress()
    {
        return address;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        System.out.println("LevelDBSplit::getAddresses");
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
