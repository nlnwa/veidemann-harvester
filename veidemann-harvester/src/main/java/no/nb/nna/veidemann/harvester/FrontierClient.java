package no.nb.nna.veidemann.harvester;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.FrontierGrpc;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvest;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvestSpec;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.MaxActiveSessionsExceededException;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.browsercontroller.RenderResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class FrontierClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FrontierClient.class);

    private final static PageHarvest NEW_PAGE_REQUEST = PageHarvest.newBuilder().setRequestNextPage(true).build();

    private final BrowserController controller;

    private final ManagedChannel channel;

    private final FrontierGrpc.FrontierStub asyncStub;

    /**
     * Used to ensure that only the given number of browser sessions are run.
     */
    private final Semaphore availableSessions;

    public FrontierClient(BrowserController controller, String host, int port, int maxOpenSessions) {
        this(controller, ManagedChannelBuilder.forAddress(host, port).usePlaintext(), maxOpenSessions);
    }

    /**
     * Construct client for accessing RouteGuide server using the existing channel.
     */
    public FrontierClient(BrowserController controller, ManagedChannelBuilder<?> channelBuilder, int maxOpenSessions) {
        LOG.info("Setting up Frontier client");
        this.controller = controller;
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        asyncStub = FrontierGrpc.newStub(channel).withWaitForReady();
        availableSessions = new Semaphore(maxOpenSessions);
    }

    public void requestNextPage() throws InterruptedException {
        availableSessions.acquire();
        ResponseObserver responseObserver = new ResponseObserver();

        FrontierGrpc.FrontierStub s = asyncStub
                .withDeadlineAfter(10, TimeUnit.MINUTES);

        StreamObserver<PageHarvest> requestObserver = s
                .getNextPage(responseObserver);
        responseObserver.setRequestObserver(requestObserver);

        try {
            requestObserver.onNext(NEW_PAGE_REQUEST);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            availableSessions.release();
        }
    }

    private class ResponseObserver implements StreamObserver<PageHarvestSpec> {
        StreamObserver<PageHarvest> requestObserver;

        public void setRequestObserver(StreamObserver<PageHarvest> requestObserver) {
            this.requestObserver = requestObserver;
        }

        @Override
        public void onNext(PageHarvestSpec pageHarvestSpec) {
            QueuedUri fetchUri = pageHarvestSpec.getQueuedUri();
            MDC.put("eid", fetchUri.getExecutionId());
            MDC.put("uri", fetchUri.getUri());

            try {
                LOG.debug("Start page rendering");

                RenderResult result = controller.render(fetchUri, pageHarvestSpec.getCrawlConfig());

                PageHarvest.Builder reply = PageHarvest.newBuilder();

                if (result.hasError()) {
                    reply.setError(result.getError());
                } else {
                    reply.getMetricsBuilder()
                            .setBytesDownloaded(result.getBytesDownloaded())
                            .setUriCount(result.getUriCount());
                }

                requestObserver.onNext(reply.build());

                result.getOutlinks().forEach(ol -> {
                    requestObserver.onNext(PageHarvest.newBuilder().setOutlink(ol).build());
                });

                requestObserver.onCompleted();

                LOG.debug("Page rendering completed");
            } catch (ClientClosedException ex) {
                LOG.error("Chrome client can't contact chrome, shutting down", ex);
                Status status = Status.UNAVAILABLE.withDescription(ex.toString());
                requestObserver.onError(status.asException());

                // Wait a little before shutting down to allow logs to flush
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException e) {
                    // OK
                }
                System.exit(1);
            } catch (MaxActiveSessionsExceededException ex) {
                LOG.debug(ex.getMessage(), ex);
                Status status = Status.RESOURCE_EXHAUSTED.withDescription(ex.toString());
                requestObserver.onError(status.asException());
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
                Status status = Status.UNKNOWN.withDescription(ex.toString());
                requestObserver.onError(status.asException());
            } finally {
                MDC.clear();
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                LOG.info("Deadline expired while talking to the frontier", status);
            } else {
                LOG.warn("Get next page failed: {}", status);
            }
            availableSessions.release();
        }

        @Override
        public void onCompleted() {
            availableSessions.release();
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
