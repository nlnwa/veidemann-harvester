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

import java.util.List;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.OpenTracingContextKey;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.api.HarvesterGrpc;
import no.nb.nna.broprox.api.HarvesterProto.CleanupExecutionRequest;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageRequest;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.harvester.BroproxHeaderConstants;
import no.nb.nna.broprox.harvester.OpenTracingSpans;
import no.nb.nna.broprox.harvester.browsercontroller.BrowserController;
import no.nb.nna.broprox.harvester.proxy.RecordingProxy;

/**
 *
 */
public class HarvesterService extends HarvesterGrpc.HarvesterImplBase {

    private final DbAdapter db;

    private final BrowserController controller;

    private final RecordingProxy proxy;

    public HarvesterService(DbAdapter db, BrowserController controller, RecordingProxy proxy) {
        this.db = db;
        this.controller = controller;
        this.proxy = proxy;
    }

    @Override
    public void harvestPage(HarvestPageRequest request, StreamObserver<HarvestPageReply> respObserver) {
        String executionId = null;
        try {
            executionId = request.getExecutionId();
            if (executionId.isEmpty()) {
                executionId = BroproxHeaderConstants.MANUAL_EXID;
            }

            OpenTracingSpans.register(executionId, OpenTracingContextKey.activeSpan());

            QueuedUri fetchUri = request.getQueuedUri();
            List<QueuedUri> outlinks = controller.render(executionId, fetchUri, request.getCrawlConfig());
            HarvestPageReply reply = HarvestPageReply.newBuilder().addAllOutlinks(outlinks).build();

            respObserver.onNext(reply);
            respObserver.onCompleted();
        } catch (Exception ex) {
            respObserver.onError(ex);
        } finally {
            OpenTracingSpans.remove(executionId);
        }
    }

    @Override
    public void cleanupExecution(CleanupExecutionRequest request, StreamObserver<Empty> responseObserver) {
        proxy.cleanCache(request.getExecutionId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
