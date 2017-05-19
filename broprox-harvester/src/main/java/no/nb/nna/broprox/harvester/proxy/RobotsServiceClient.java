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

package no.nb.nna.broprox.harvester.proxy;

import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.api.RobotsServiceGrpc;
import no.nb.nna.broprox.api.RobotsServiceProto;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.MessagesProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RobotsServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RobotsServiceClient.class);

    private final ManagedChannel channel;

    private final RobotsServiceGrpc.RobotsServiceBlockingStub blockingStub;

    private final RobotsServiceGrpc.RobotsServiceStub asyncStub;

    public RobotsServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
        LOG.info("Robots service client pointing to " + host + ":" + port);
    }

    public RobotsServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up Robots service client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = RobotsServiceGrpc.newBlockingStub(channel);
        asyncStub = RobotsServiceGrpc.newStub(channel);
    }

    public boolean isAllowed(String executionId, String uri, ConfigProto.CrawlConfig config) {
        try {
            RobotsServiceProto.IsAllowedRequest request = RobotsServiceProto.IsAllowedRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setUri(uri)
                    .setUserAgent(config.getBrowserConfig().getUserAgent())
                    .setPoliteness(config.getPoliteness())
                    .build();
            RobotsServiceProto.IsAllowedReply reply = blockingStub.isAllowed(request);
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
