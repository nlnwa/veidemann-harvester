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
package no.nb.nna.veidemann.harvester.api;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.HarvesterGrpc;
import no.nb.nna.veidemann.api.HarvesterProto.CleanupExecutionRequest;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageRequest;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.MaxActiveSessionsExceededException;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.proxy.RecordingProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class HarvesterService extends HarvesterGrpc.HarvesterImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(HarvesterService.class);

    private final BrowserController controller;

    private final RecordingProxy proxy;

    public HarvesterService(BrowserController controller, RecordingProxy proxy) {
        this.controller = controller;
        this.proxy = proxy;
    }

    @Override
    public void harvestPage(HarvestPageRequest request, StreamObserver<HarvestPageReply> respObserver) {
        QueuedUri fetchUri = request.getQueuedUri();
        MDC.put("eid", fetchUri.getExecutionId());
        MDC.put("uri", fetchUri.getUri());

        try {
            HarvestPageReply reply = controller.render(fetchUri, request.getCrawlConfig());

            respObserver.onNext(reply);
            respObserver.onCompleted();
        } catch (ClientClosedException ex) {
            LOG.error("Chrome client can't contact chrome, shutting down", ex);
            Status status = Status.UNAVAILABLE.withDescription(ex.toString());
            respObserver.onError(status.asException());
            System.exit(1);
        } catch (MaxActiveSessionsExceededException ex) {
            LOG.debug(ex.getMessage(), ex);
            Status status = Status.RESOURCE_EXHAUSTED.withDescription(ex.toString());
            respObserver.onError(status.asException());
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void cleanupExecution(CleanupExecutionRequest request, StreamObserver<Empty> responseObserver) {
        proxy.cleanCache(request.getExecutionId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
