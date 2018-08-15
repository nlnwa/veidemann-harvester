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
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.util.Pool;
import no.nb.nna.veidemann.commons.util.Pool.Lease;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import no.nb.nna.veidemann.harvester.browsercontroller.RenderResult;
import org.netpreserve.commons.uri.ParsedQuery;
import org.netpreserve.commons.uri.ParsedQuery.Entry;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FrontierClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FrontierClient.class);

    private final static PageHarvest NEW_PAGE_REQUEST = PageHarvest.newBuilder().setRequestNextPage(true).build();

    private final BrowserController controller;

    private final ManagedChannel channel;

    private final FrontierGrpc.FrontierStub asyncStub;

    private final AtomicInteger idx = new AtomicInteger(0);

    private final Pool<ProxySession> pool;

    public FrontierClient(BrowserController controller, String host, int port, int maxOpenSessions,
                          String browserWsEndpoint, int firstProxyPort) {
        this(controller, ManagedChannelBuilder.forAddress(host, port).usePlaintext(), maxOpenSessions,
                browserWsEndpoint, firstProxyPort);
    }

    /**
     * Construct client for accessing RouteGuide server using the existing channel.
     */
    public FrontierClient(BrowserController controller, ManagedChannelBuilder<?> channelBuilder, int maxOpenSessions,
                          String browserWsEndpoint, int firstProxyPort) {
        LOG.info("Setting up Frontier client");
        this.controller = controller;
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        asyncStub = FrontierGrpc.newStub(channel).withWaitForReady();
        pool = new Pool<>(maxOpenSessions, () -> new ProxySession(idx.getAndIncrement(),
                browserWsEndpoint, firstProxyPort), null, p -> p.reset());
    }

    public void requestNextPage() throws InterruptedException {
        Lease<ProxySession> proxySessionLease = pool.lease();
        ResponseObserver responseObserver = new ResponseObserver(proxySessionLease);

        FrontierGrpc.FrontierStub s = asyncStub;

        StreamObserver<PageHarvest> requestObserver = s
                .getNextPage(responseObserver);
        responseObserver.setRequestObserver(requestObserver);

        try {
            requestObserver.onNext(NEW_PAGE_REQUEST);
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            proxySessionLease.close();
        }
    }

    @Override
    public void close() {
        try {
            boolean isTerminated = channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            if (!isTerminated) {
                LOG.warn("Harvester client has open connections after close");
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private class ResponseObserver implements StreamObserver<PageHarvestSpec> {
        private final Lease<ProxySession> proxySessionLease;
        StreamObserver<PageHarvest> requestObserver;

        public ResponseObserver(Lease<ProxySession> proxySessionLease) {
            this.proxySessionLease = proxySessionLease;
        }

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

                RenderResult result = controller.render(proxySessionLease.getObject(), fetchUri, pageHarvestSpec.getCrawlConfig());

                PageHarvest.Builder reply = PageHarvest.newBuilder();

                if (result.hasError()) {
                    reply.setError(result.getError());
                    requestObserver.onNext(reply.build());
                } else {
                    reply.getMetricsBuilder()
                            .setBytesDownloaded(result.getBytesDownloaded())
                            .setUriCount(result.getUriCount());
                    requestObserver.onNext(reply.build());

                    result.getOutlinks().forEach(ol -> {
                        requestObserver.onNext(PageHarvest.newBuilder().setOutlink(ol).build());
                    });
                }

                requestObserver.onCompleted();

                LOG.debug("Page rendering completed");
            } catch (Throwable t) {
                LOG.error("Page rendering failed: {}", t.getMessage(), t);
                PageHarvest.Builder reply = PageHarvest.newBuilder();
                reply.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
                requestObserver.onNext(reply.build());
                requestObserver.onCompleted();
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
            proxySessionLease.close();
        }

        @Override
        public void onCompleted() {
            proxySessionLease.close();
        }
    }

    public class ProxySession {
        private final int proxyId;
        private final int proxyPort;
        private final String browserWsEndpoint;
        private BrowserSession session;

        public ProxySession(int proxyId, String browserWSEndpoint, int firstProxyPort) {
            this.proxyId = proxyId;
            this.proxyPort = proxyId + firstProxyPort;
            Uri ws = UriConfigs.WHATWG.buildUri(browserWSEndpoint);
            ParsedQuery query = ws.getParsedQuery();
            Entry proxyEntry;
            if (query.containsKey("--proxy-server")) {
                proxyEntry = query.get("--proxy-server");
                String val = proxyEntry.getSingle().replaceFirst(":\\d+", ":" + String.valueOf(proxyPort));
                proxyEntry = new Entry("--proxy-server", val);
                query = query.put(proxyEntry);
            } else {
                proxyEntry = new Entry("--proxy-server", "http://harvester:" + proxyPort);
                query = query.add(proxyEntry);
            }
            browserWsEndpoint = UriConfigs.WHATWG.builder(ws).parsedQuery(query).build().toString();
            System.out.println("CREATED session :  " + this);
        }

        public int getProxyId() {
            return proxyId;
        }

        public int getProxyPort() {
            return proxyPort;
        }

        public String getBrowserWsEndpoint() {
            return browserWsEndpoint;
        }

        public BrowserSession getSession() {
            return session;
        }

        public void setSession(BrowserSession session) {
            this.session = session;
        }

        private void reset() {
            session = null;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("ProxySession{");
            sb.append("proxyId=").append(proxyId);
            sb.append(", proxyPort=").append(proxyPort);
            sb.append(", browserWsEndpoint='").append(browserWsEndpoint).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
}
