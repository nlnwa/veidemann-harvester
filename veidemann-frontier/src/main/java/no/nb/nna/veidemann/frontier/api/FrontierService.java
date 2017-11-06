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
package no.nb.nna.veidemann.frontier.api;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.ActiveSpan;
import io.opentracing.contrib.OpenTracingContextKey;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.FrontierGrpc;
import no.nb.nna.veidemann.api.FrontierProto.CrawlSeedRequest;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FrontierService extends FrontierGrpc.FrontierImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierService.class);

    private final Frontier frontier;

    public FrontierService(Frontier frontier) {
        this.frontier = frontier;
    }

    @Override
    public void crawlSeed(CrawlSeedRequest request, StreamObserver<CrawlExecutionStatus> respObserver) {
        try (ActiveSpan span = GlobalTracer.get()
                .buildSpan("scheduleSeed")
                .asChildOf(OpenTracingContextKey.activeSpan())
                .withTag(Tags.COMPONENT.getKey(), "Frontier")
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .withTag("uri", request.getSeed().getMeta().getName())
                .startActive()) {
            CrawlExecutionStatus reply = frontier.scheduleSeed(request.getJob(), request.getSeed());

            respObserver.onNext(reply);
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

}
