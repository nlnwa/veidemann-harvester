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
package no.nb.nna.broprox.frontier.worker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.broprox.api.HarvesterGrpc;
import no.nb.nna.broprox.api.HarvesterGrpc.HarvesterBlockingStub;
import no.nb.nna.broprox.api.HarvesterGrpc.HarvesterStub;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageRequest;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HarvesterClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HarvesterClient.class);

    private final ManagedChannel channel;

    private final HarvesterBlockingStub blockingStub;

    private final HarvesterStub asyncStub;

    public HarvesterClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
        LOG.info("Harvester client pointing to " + host + ":" + port);
    }

    public HarvesterClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up harvester client");
        channel = channelBuilder.build();
        blockingStub = HarvesterGrpc.newBlockingStub(channel);
        asyncStub = HarvesterGrpc.newStub(channel);
    }

    public List<QueuedUri> fetchPage(String executionId, QueuedUri qUri) {
        HarvestPageRequest request = HarvestPageRequest.newBuilder()
                .setExecutionId(executionId)
                .setQueuedUri(qUri)
                .build();
        HarvestPageReply reply = blockingStub.harvestPage(request);
        return reply.getOutlinksList();
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
