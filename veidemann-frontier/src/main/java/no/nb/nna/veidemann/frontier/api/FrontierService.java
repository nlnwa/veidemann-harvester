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
import no.nb.nna.veidemann.api.FrontierProto.CrawlExecutionId;
import no.nb.nna.veidemann.api.FrontierProto.CrawlSeedRequest;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvest;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvestSpec;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import no.nb.nna.veidemann.frontier.worker.Frontier;
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
    public void crawlSeed(CrawlSeedRequest request, StreamObserver<CrawlExecutionId> responseObserver) {
        try (ActiveSpan span = GlobalTracer.get()
                .buildSpan("scheduleSeed")
                .asChildOf(OpenTracingContextKey.activeSpan())
                .withTag(Tags.COMPONENT.getKey(), "Frontier")
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .withTag("uri", request.getSeed().getMeta().getName())
                .startActive()) {
            CrawlExecutionStatus reply = frontier.scheduleSeed(request);

            responseObserver.onNext(CrawlExecutionId.newBuilder().setId(reply.getId()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public StreamObserver<PageHarvest> getNextPage(StreamObserver<PageHarvestSpec> responseObserver) {
        return new StreamObserver<PageHarvest>() {
            CrawlExecution exe;

            @Override
            public void onNext(PageHarvest value) {
                switch (value.getMsgCase()) {
                    case REQUESTNEXTPAGE:
                        try {
                            PageHarvestSpec pageHarvestSpec = null;
                            while (pageHarvestSpec == null) {
                                exe = frontier.getNextPageToFetch();
                                pageHarvestSpec = exe.preFetch();
                                if (pageHarvestSpec == null) {
                                    exe.postFetchFinally();
                                }
                            }
                            responseObserver.onNext(pageHarvestSpec);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            Status status = Status.UNKNOWN.withDescription(e.toString());
                            responseObserver.onError(status.asException());
                        }
                        break;
                    case METRICS:
                        exe.postFetchSuccess(value.getMetrics());
                        break;
                    case OUTLINK:
                        try {
                            exe.queueOutlink(value.getOutlink());
                        } catch (DbException e) {
                            LOG.error("Could not add URI to queue", e);
                        }
                        break;
                    case ERROR:
                        try {
                            exe.postFetchFailure(value.getError());
                        } catch (DbException e) {
                            LOG.error("Could not handle failure", e);
                        }
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                try {
                    exe.postFetchFailure(t);
                } catch (DbException e) {
                    LOG.error("Could not handle failure", e);
                }
                exe.postFetchFinally();
            }

            @Override
            public void onCompleted() {
                exe.postFetchFinally();
                responseObserver.onCompleted();
            }
        };
    }
}
