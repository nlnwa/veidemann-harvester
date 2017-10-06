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
package no.nb.nna.broprox.harvester.api;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.ActiveSpan;
import io.opentracing.contrib.OpenTracingContextKey;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.api.HarvesterGrpc;
import no.nb.nna.broprox.api.HarvesterProto.CleanupExecutionRequest;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageRequest;
import no.nb.nna.broprox.harvester.browsercontroller.BrowserController;
import no.nb.nna.broprox.harvester.proxy.RecordingProxy;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        try {
            HarvestPageReply reply = controller.render(fetchUri, request.getCrawlConfig());

            respObserver.onNext(reply);
            respObserver.onCompleted();
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
