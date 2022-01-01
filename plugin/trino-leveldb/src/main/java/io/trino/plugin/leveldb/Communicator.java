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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

class Communicator
{
    private final String address;
    private final int port;
    private BufferedWriter oWriter;
    private BufferedReader iReader;
    private Socket sock;

    public Communicator(String address, int port)
    {
        this.address = address;
        this.port = port;
        try {
            sock = new Socket(address, port);
            oWriter = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8));
            iReader = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write_json(JsonNode json)
    {
        ObjectMapper mapper = new ObjectMapper();
        String s = null;
        try {
            s = mapper.writer().writeValueAsString(json);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println("serialized json:" + s);
        try {
            System.out.println("write json: " + s);
            oWriter.write(s + "\n");
            oWriter.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JsonNode read_json()
    {
        String jstring = null;
        try {
            jstring = iReader.readLine();
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        System.out.println(jstring);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode ret = null;
        try {
            ret = mapper.readTree(jstring);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public String read_line()
    {
        // System.out.println("read_line");
        try {
            return iReader.readLine();
        }
        catch (IOException e) {
            System.err.println("read_line exception");
            e.printStackTrace();
            return null;
        }
    }

    public void close()
    {
        try {
            oWriter.close();
            iReader.close();
            sock.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getAddress()
    {
        return address;
    }

    public int getPort()
    {
        return port;
    }
}
