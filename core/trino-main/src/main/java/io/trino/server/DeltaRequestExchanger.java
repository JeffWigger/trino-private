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


import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.connector.CatalogName;
import io.trino.execution.LocationFactory;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.DeltaFlagRequest;
import io.trino.spi.TrinoException;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;

public class DeltaRequestExchanger
{

    private final InternalNodeManager internalNodeManager;
    private final LocationFactory locationFactory;
    private final JsonCodec<DeltaFlagRequest> deltaFlagRequestCodec;
    private final HttpClient httpClient;

    private SettableFuture<Void> phaseIIFuture = SettableFuture.create();

    FileWriter statisticsWriter;
    long startTime;

    @Inject
    public DeltaRequestExchanger(InternalNodeManager internalNodeManager,
                                LocationFactory locationFactory,
                                JsonCodec<DeltaFlagRequest> deltaFlagRequestCodec,
                                HttpClient httpClient){
        this.internalNodeManager = internalNodeManager;
        this.locationFactory = locationFactory;
        this.deltaFlagRequestCodec = deltaFlagRequestCodec;
        this.httpClient = httpClient;
    }
    /**
     * Unsets flag on all the nodes that use the memory connector.
     * By setting that flag to false all regular MemoryPagesStore::getPages are again free to continue.
     */
    public ListenableFuture<Void> unmarkDeltaUpdate(){

        // TODO: make sure we unmark all that were active when we marked them
        SettableFuture<Void> future = SettableFuture.create();
        Set<InternalNode> memoryNodes = this.internalNodeManager.getActiveConnectorNodes(new CatalogName("memory"));
        if(memoryNodes.isEmpty()){
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "There should be at least one node running the memory plugin");
        }
        DeltaFlagRequest deltaFlagRequest = new DeltaFlagRequest(false, DeltaFlagRequest.globalDeltaUpdateCount);
        List<HttpClient.HttpResponseFuture<JsonResponse<DeltaFlagRequest>>> responseFutures = new ArrayList<>();
        for (InternalNode node : memoryNodes){
            // System.out.println("sending delta update Flag request to: "+ node.getNodeIdentifier());
            URI flagSignalPoint = this.locationFactory.createDeltaFlagLocation(node);
            Request request = preparePost()
                    .setUri(flagSignalPoint)
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(createStaticBodyGenerator(deltaFlagRequestCodec.toJsonBytes(deltaFlagRequest)))
                    .build();
            HttpClient.HttpResponseFuture<JsonResponse<DeltaFlagRequest>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(deltaFlagRequestCodec));

            responseFutures.add(responseFuture);
        }
        ListenableFuture<List<JsonResponse<DeltaFlagRequest>>> allAsListFuture= Futures.successfulAsList(responseFutures);
        Futures.addCallback(allAsListFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(@Nullable List<JsonResponse<DeltaFlagRequest>> result)
            {
                System.out.println("Unsetting the deltaFlag succeeded");
                boolean success = true;
                if (result != null) {
                    for (JsonResponse<DeltaFlagRequest> res : result) {
                        if (!res.hasValue()) {
                            System.out.println("A response from setting the delta flag does not have a result");
                            success = false;
                        }
                        if (res.getStatusCode() != OK.code()) {
                            System.out.println("Unsetting the delta flag failed with error code: " + res.getStatusCode());
                            success = false;
                        }
                    }
                }else{
                    success = false;
                }
                if(success){
                    // start next part of execution
                    phaseIIFuture.set(null);
                    System.out.println("SUCCESSFULLY unset the delta update flag on all nodes");
                    future.set(null);
                }else{
                    future.setException(new TrinoException(GENERIC_INTERNAL_ERROR, "Could not set the delta flag on some of the nodes"));
                }
                long endTime = System.nanoTime();
                try {
                    statisticsWriter.write(String.format("%d, %d\n", DeltaFlagRequest.globalDeltaUpdateCount, (endTime - startTime)/1000000));
                    statisticsWriter.flush();
                    statisticsWriter.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                System.out.println("Setting the deltaFlag failed");
                future.setException(throwable);
                long endTime = System.nanoTime();
                try {
                    statisticsWriter.write(String.format("%d\n", (endTime - startTime)/1000000));
                    statisticsWriter.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, directExecutor());
        return future;
    }

    /**
     * Sets a flag on all the nodes that use the memory connector.
     * By setting that flag all regular MemoryPagesStore::getPages are blocked until the delta update is finished.
     */
    public ListenableFuture<Void> markDeltaUpdate(){
        // Could add a flag or state to QueryState / StateMachine indicating that a delta update is going on
        // splits will already be on the nodes, so this is mute unless we inform first all queries and have them
        // then inform all their tasks

        SettableFuture<Void> future = SettableFuture.create();

        // Or we inform all nodes that run a memoryDB that they need to block further page polls
        //TODO: Need to store them such that we can check in unmarkDeltaUpdate that we unmarked all of them successfully
        Set<InternalNode> memoryNodes = this.internalNodeManager.getActiveConnectorNodes(new CatalogName("memory"));
        if(memoryNodes.isEmpty()){
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "There should be at least one node running the memory plugin");
        }
        // the assumption is that there is only ever one delta update at a time
        DeltaFlagRequest.globalDeltaUpdateCount += 1;
        DeltaFlagRequest deltaFlagRequest = new DeltaFlagRequest(true, DeltaFlagRequest.globalDeltaUpdateCount);
        List<HttpClient.HttpResponseFuture<JsonResponse<DeltaFlagRequest>>> responseFutures = new ArrayList<>();
        for (InternalNode node : memoryNodes){
            System.out.println("sending delta update Flag request to: "+ node.getNodeIdentifier());
            URI flagSignalPoint = this.locationFactory.createDeltaFlagLocation(node);
            Request request = preparePost()
                    .setUri(flagSignalPoint)
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(createStaticBodyGenerator(deltaFlagRequestCodec.toJsonBytes(deltaFlagRequest)))
                    .build();
            HttpClient.HttpResponseFuture<JsonResponse<DeltaFlagRequest>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(deltaFlagRequestCodec));

            responseFutures.add(responseFuture);
        }
        ListenableFuture<List<JsonResponse<DeltaFlagRequest>>> allAsListFuture= Futures.successfulAsList(responseFutures);
        Futures.addCallback(allAsListFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(@Nullable List<JsonResponse<DeltaFlagRequest>> result)
            {
                System.out.println("Setting the deltaFlag succeeded");
                boolean success = true;
                if (result != null) {
                    for (JsonResponse<DeltaFlagRequest> res : result) {
                        if (!res.hasValue()) {
                            System.out.println("A response from setting the delta flag does not have a result");
                            success = false;
                        }
                        if (res.getStatusCode() != OK.code()) {
                            System.out.println("Setting the delta flag failed with error code: " + res.getStatusCode());
                            success = false;
                        }
                    }
                }else{
                    success = false;
                }
                if(success){
                    // start next part of execution
                    //outputConsumer.accept(Optional.empty());
                    //phaseIIFuture.set(null);
                    System.out.println("SUCCESSFULLY set the delta update flag on all nodes");
                    future.set(null);
                }else{
                    future.setException(new TrinoException(GENERIC_INTERNAL_ERROR, "Could not set the delta flag on some of the nodes"));
                }
            }
            @Override
            public void onFailure(Throwable throwable)
            {
                System.out.println("Setting the deltaFlag failed");
                future.setException(throwable);
            }
        }, directExecutor());
        return future;
    }
}
