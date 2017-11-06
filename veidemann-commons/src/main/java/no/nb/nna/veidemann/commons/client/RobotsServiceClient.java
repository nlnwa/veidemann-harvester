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

package no.nb.nna.veidemann.commons.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.RobotsEvaluatorGrpc;
import no.nb.nna.veidemann.api.RobotsEvaluatorProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RobotsServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RobotsServiceClient.class);

    private final ManagedChannel channel;

    private final RobotsEvaluatorGrpc.RobotsEvaluatorBlockingStub blockingStub;

    private final RobotsEvaluatorGrpc.RobotsEvaluatorStub asyncStub;

    public RobotsServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
        LOG.info("Robots service client pointing to " + host + ":" + port);
    }

    public RobotsServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up Robots service client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = RobotsEvaluatorGrpc.newBlockingStub(channel);
        asyncStub = RobotsEvaluatorGrpc.newStub(channel);
    }

    public boolean isAllowed(QueuedUri queuedUri, CrawlConfig config) {
        try {
            RobotsEvaluatorProto.IsAllowedRequest request = RobotsEvaluatorProto.IsAllowedRequest.newBuilder()
                    .setExecutionId(queuedUri.getExecutionId())
                    .setUri(queuedUri.getUri())
                    .setUserAgent(config.getBrowserConfig().getUserAgent())
                    .setPoliteness(config.getPoliteness())
                    .build();
            RobotsEvaluatorProto.IsAllowedReply reply = blockingStub.isAllowed(request);
            return reply.getIsAllowed();
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
