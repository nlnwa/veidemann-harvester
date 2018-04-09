/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.HarvesterGrpc;
import no.nb.nna.veidemann.api.HarvesterGrpc.HarvesterBlockingStub;
import no.nb.nna.veidemann.api.HarvesterGrpc.HarvesterStub;
import no.nb.nna.veidemann.api.HarvesterProto.CleanupExecutionRequest;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageRequest;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HarvesterClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HarvesterClient.class);

    private final ManagedChannel channel;

    private final HarvesterBlockingStub blockingStub;

    private final HarvesterStub asyncStub;

    private long maxWaitForExhaustedHarvesterMs = 60000;

    public HarvesterClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
        LOG.info("Harvester client pointing to " + host + ":" + port);
    }

    public HarvesterClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up harvester client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = HarvesterGrpc.newBlockingStub(channel);
        asyncStub = HarvesterGrpc.newStub(channel);
    }

    public HarvesterClient withMaxWaitForExhaustedHarvesterMs(Duration maxWaitForExhaustedHarvester) {
        this.maxWaitForExhaustedHarvesterMs = maxWaitForExhaustedHarvester.toMillis();
        return this;
    }

    public CompletableFuture<HarvestPageReply> fetchPage(QueuedUri qUri, CrawlConfig config) {
        if (qUri.getExecutionId().isEmpty()) {
            throw new IllegalArgumentException("A queued URI must have the execution ID set.");
        }

        HarvestPageRequest request = HarvestPageRequest.newBuilder()
                .setQueuedUri(qUri)
                .setCrawlConfig(config)
                .build();

        CompletableFuture<HarvestPageReply> result = new CompletableFuture<>();

        long start = System.currentTimeMillis();

        innerFetchPage(request, result, start);

        return result;
    }

    private void innerFetchPage(HarvestPageRequest request, CompletableFuture<HarvestPageReply> result, long start) {
        asyncStub.harvestPage(request, new StreamObserver<HarvestPageReply>() {
            HarvestPageReply reply;

            @Override
            public void onNext(HarvestPageReply value) {
                reply = value;
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    StatusRuntimeException ex = (StatusRuntimeException) t;
                    if (Status.RESOURCE_EXHAUSTED.getCode().equals(ex.getStatus().getCode())) {
                        if (System.currentTimeMillis() - start > maxWaitForExhaustedHarvesterMs) {
                            LOG.info("Harvester was exhausted for {}ms giving up", (System.currentTimeMillis() - start));
                            result.completeExceptionally(t);
                        } else {
                            Random rnd = new Random();
                            long retryDelay = 500L + rnd.nextInt(1000);
                            LOG.info("Harvester was exhausted, will retry in {} milliseconds: {}", retryDelay, ex.getStatus());
                            try {
                                Thread.sleep(retryDelay);
                                innerFetchPage(request, result, start);
                            } catch (InterruptedException e) {
                                LOG.error("RPC was interrupted", e);
                                result.completeExceptionally(t);
                            }
                        }
                    } else {
                        LOG.error("RPC failed: " + ex.getStatus(), ex);
                        result.completeExceptionally(t);
                    }
                }
            }

            @Override
            public void onCompleted() {
                result.complete(reply);
            }
        });
    }

    public void cleanupExecution(String executionId) {
        try {
            CleanupExecutionRequest request = CleanupExecutionRequest.newBuilder()
                    .setExecutionId(executionId)
                    .build();
            blockingStub.cleanupExecution(request);
        } catch (StatusRuntimeException ex) {
            LOG.error("RPC failed: " + ex.getStatus(), ex);
            throw ex;
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

}
