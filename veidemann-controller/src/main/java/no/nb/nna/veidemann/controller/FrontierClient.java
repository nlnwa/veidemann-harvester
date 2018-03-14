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
package no.nb.nna.veidemann.controller;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.FrontierGrpc;
import no.nb.nna.veidemann.api.FrontierGrpc.FrontierBlockingStub;
import no.nb.nna.veidemann.api.FrontierGrpc.FrontierStub;
import no.nb.nna.veidemann.api.FrontierProto.CrawlSeedRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FrontierClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierClient.class);

    private final ManagedChannel channel;

    private final FrontierBlockingStub blockingStub;

    private final FrontierStub asyncStub;

    public FrontierClient(final String host, final int port, String supportedSeedType) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true), supportedSeedType);
        LOG.info("Harvester client pointing to " + host + ":" + port);
    }

    public FrontierClient(ManagedChannelBuilder<?> channelBuilder, String supportedSeedType) {
        LOG.info("Setting up harvester client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = FrontierGrpc.newBlockingStub(channel);
        asyncStub = FrontierGrpc.newStub(channel);
        JobExecutionUtil.addFrontierClient(supportedSeedType, this);
    }

    public CrawlExecutionStatus crawlSeed(CrawlJob crawlJob, Seed seed, JobExecutionStatus jobExecution) {
        try {
            CrawlSeedRequest request = CrawlSeedRequest.newBuilder()
                    .setJob(crawlJob)
                    .setSeed(seed)
                    .setJobExecutionId(jobExecution.getId())
                    .build();
            return blockingStub.crawlSeed(request);
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
