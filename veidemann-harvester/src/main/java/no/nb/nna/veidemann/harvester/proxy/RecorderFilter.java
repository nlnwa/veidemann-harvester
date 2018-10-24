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
package no.nb.nna.veidemann.harvester.proxy;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.opentracing.ActiveSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ContentWriterProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.ContentWriterClient.ContentWriterSession;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import no.nb.nna.veidemann.harvester.browsercontroller.CrawlLogRegistry;
import org.littleshoot.proxy.HostResolver;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private final int proxyId;

    private static final AtomicLong nextProxyRequestId = new AtomicLong();

    private final String uri;

    private CrawlLogRegistry.Entry crawlLogEntry;

    private final BrowserSessionRegistry sessionRegistry;

    private BrowserSession browserSession;

    private ContentCollector requestCollector;

    private ContentCollector responseCollector;

    private final DbAdapter db;

    private String jobExecutionId;

    private String executionId;

    private Timestamp fetchTimeStamp;

    private InetSocketAddress resolvedRemoteAddress;

    private HttpResponseStatus httpResponseStatus;

    private HttpVersion httpResponseProtocolVersion;

    private CrawlLog.Builder crawlLog;

    private Span requestSpan;

    private Span responseSpan;

    private final ContentWriterClient contentWriterClient;

    private final HostResolver hostResolver;

    private ContentWriterSession contentWriterSession;

    private boolean foundInCache = false;

    public RecorderFilter(final int proxyId, final String uri, final HttpRequest originalRequest,
                          final ChannelHandlerContext ctx, final ContentWriterClient contentWriterClient,
                          final BrowserSessionRegistry sessionRegistry, final HostResolver hostResolver) {

        super(originalRequest, ctx);
        this.proxyId = proxyId;
        this.db = DbService.getInstance().getDbAdapter();
        this.uri = uri;

        this.contentWriterClient = contentWriterClient;
        this.requestCollector = new ContentCollector(0, ContentWriterProto.RecordType.REQUEST, uri, db);
        this.responseCollector = new ContentCollector(1, ContentWriterProto.RecordType.RESPONSE, uri, db);
        this.sessionRegistry = sessionRegistry;
        this.hostResolver = hostResolver;
        this.fetchTimeStamp = ProtoUtils.getNowTs();
    }

    private synchronized ContentWriterSession getContentWriterSession() {
        if (contentWriterSession == null) {
            contentWriterSession = contentWriterClient.createSession();
        }
        return contentWriterSession;
    }

    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        try {
            if (httpObject instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) httpObject;

                // Lookup browser session, but skip if it is a robots.txt request since that is not coming from browser.
                if (!uri.endsWith("robots.txt")) {
                    browserSession = sessionRegistry.get(proxyId);
                    crawlLogEntry = browserSession.getCrawlLogs().registerProxyRequest(nextProxyRequestId.getAndIncrement(), uri);
                    executionId = browserSession.getExecutionId();
                    jobExecutionId = browserSession.getJobExecutionId();
                } else {
                    executionId = request.headers().get(EXECUTION_ID);
                    jobExecutionId = request.headers().get(JOB_EXECUTION_ID);
                }

                if (executionId == null || executionId.isEmpty()) {
                    LOG.error("Missing executionId for {}", uri);
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }

                if (jobExecutionId == null || jobExecutionId.isEmpty()) {
                    LOG.error("Missing jobExecutionId for {}", uri);
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }

                MDC.put("eid", executionId);
                MDC.put("uri", uri);

                LOG.debug("Proxy got request");

                Uri parsedUri;
                try {
                    parsedUri = UriConfigs.WHATWG.buildUri(uri);
                } catch (Exception e) {
                    crawlLog = buildCrawlLog()
                            .setIpAddress("")
                            .setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError());
                    writeCrawlLog(crawlLog);
                    LOG.debug("URI parsing failed");
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
                }

                try {
                    resolvedRemoteAddress = hostResolver.resolve(parsedUri.getHost(), parsedUri.getDecodedPort());
                } catch (UnknownHostException e) {
                    crawlLog = buildCrawlLog()
                            .setIpAddress("")
                            .setError(ExtraStatusCodes.DOMAIN_LOOKUP_FAILED.toFetchError());
                    writeCrawlLog(crawlLog);
                    LOG.debug("DNS lookup failed");
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
                }

                try (ActiveSpan span = GlobalTracer.get().makeActive(getRequestSpan())) {
                    // Fix headers before sending to final destination
                    request.headers().set("Accept-Encoding", "identity");

                    // Store request
                    requestCollector.setHeaders(ContentCollector.createRequestPreamble(request), request.headers(), getContentWriterSession());
                    LOG.debug("Proxy is sending request to final destination.");
                }
            } else if (httpObject instanceof HttpContent) {
                HttpContent request = (HttpContent) httpObject;
                requestCollector.addPayload(request.content(), getContentWriterSession());
            } else {
                LOG.debug("Got something else than http request: {}", httpObject);
            }
        } catch (Throwable t) {
            String ipAddress = "";
            if (resolvedRemoteAddress != null) {
                ipAddress = resolvedRemoteAddress.getAddress().getHostAddress();
            }
            crawlLog = buildCrawlLog()
                    .setIpAddress(ipAddress)
                    .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
            writeCrawlLog(crawlLog);
            LOG.error("Error handling request", t);
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        }
        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        if (browserSession != null && browserSession.isClosed()) {
            LOG.warn("Browser session was closed, aborting request");
            if (contentWriterSession != null && contentWriterSession.isOpen()) {
                try {
                    contentWriterSession.cancel("Session was aborted");
                } catch (InterruptedException e) {
                    LOG.info("Proxy got error while closing content writer session: {}", e.toString());
                }
            }
            return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
        }

        try {
            try (ActiveSpan span = GlobalTracer.get().makeActive(getResponseSpan())) {

                boolean handled = false;

                if (httpObject instanceof HttpResponse) {
                    try {
                        HttpResponse res = (HttpResponse) httpObject;
                        LOG.trace("Got response headers {}", res.status());

                        httpResponseStatus = res.status();
                        httpResponseProtocolVersion = res.protocolVersion();
                        responseCollector.setHeaders(ContentCollector.createResponsePreamble(res), res.headers(), getContentWriterSession());
                        span.log("Got response headers");

                        crawlLog = buildCrawlLog()
                                .setStatusCode(httpResponseStatus.code())
                                .setContentType(res.headers().get(HttpHeaderNames.CONTENT_TYPE, ""));

                        if (res.headers().get("X-Cache-Lookup", "MISS").contains("HIT")) {
                            foundInCache = true;

                            LOG.debug("Found in cache");
                            span.log("Loaded from cache");
                        }

                        handled = true;
                        LOG.trace("Handled response headers");
                    } catch (Exception ex) {
                        LOG.error("Error handling response headers", ex);
                        span.log("Error handling response headers: " + ex.toString());
                        return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                    }
                }

                if (httpObject instanceof HttpContent) {
                    LOG.trace("Got response content");
                    if (foundInCache) {
                        handled = true;
                    } else {
                        try {
                            HttpContent res = (HttpContent) httpObject;
                            span.log("Got response content. Size: " + res.content().readableBytes());
                            responseCollector.addPayload(res.content(), getContentWriterSession());

                            handled = true;
                            LOG.trace("Handled response content");
                        } catch (Exception ex) {
                            LOG.error("Error handling response content", ex);
                            span.log("Error handling response content: " + ex.toString());
                            contentWriterSession.cancel("Got error while writing response content");
                            return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                        }
                    }
                }

                if (ProxyUtils.isLastChunk(httpObject)) {
                    LOG.debug("Got last response chunk. Response status: {}", httpResponseStatus);
                    if (foundInCache) {
                        writeCrawlLog(crawlLog);
                        span.log("Last response chunk");
                        LOG.debug("Handled last response chunk");
                        try {
                            if (contentWriterSession != null && contentWriterSession.isOpen()) {
                                contentWriterSession.cancel("OK: Loaded from cache");
                            }
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    } else {
                        ContentWriterProto.WriteResponseMeta.RecordMeta responseRecordMeta = null;
                        try {
                            Duration fetchDuration = Timestamps.between(fetchTimeStamp, ProtoUtils.getNowTs());

                            String ipAddress = resolvedRemoteAddress.getAddress().getHostAddress();

                            ContentWriterProto.WriteRequestMeta meta = ContentWriterProto.WriteRequestMeta.newBuilder()
                                    .setExecutionId(executionId)
                                    .setFetchTimeStamp(fetchTimeStamp)
                                    .setTargetUri(uri)
                                    .setStatusCode(httpResponseStatus.code())
                                    .setIpAddress(ipAddress)
                                    .putRecordMeta(requestCollector.getRecordNum(), requestCollector.getRecordMeta())
                                    .putRecordMeta(responseCollector.getRecordNum(), responseCollector.getRecordMeta())
                                    .build();

                            // Finish ContentWriter session
                            getContentWriterSession().sendMetadata(meta);
                            ContentWriterProto.WriteResponseMeta writeResponse = getContentWriterSession().finish();
                            contentWriterSession = null;

                            // Write CrawlLog
                            responseRecordMeta = writeResponse.getRecordMetaOrDefault(1, null);
                            crawlLog.setIpAddress(ipAddress)
                                    .setWarcId(responseRecordMeta.getWarcId())
                                    .setStorageRef(responseRecordMeta.getStorageRef())
                                    .setRecordType(responseRecordMeta.getType().name().toLowerCase())
                                    .setBlockDigest(responseRecordMeta.getBlockDigest())
                                    .setPayloadDigest(responseRecordMeta.getPayloadDigest())
                                    .setFetchTimeMs(Durations.toMillis(fetchDuration))
                                    .setSize(responseCollector.getSize())
                                    .setWarcRefersTo(responseRecordMeta.getWarcRefersTo());

                            writeCrawlLog(crawlLog);

                        } catch (Exception ex) {
                            LOG.error("Error writing response", ex);
                            span.log("Error writing response: " + ex.toString());
                            if (contentWriterSession != null && contentWriterSession.isOpen()) {
                                contentWriterSession.cancel("Got error while writing response metadata");
                            }
                            crawlLog.setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(ex.toString()));
                            writeCrawlLog(crawlLog);
                            return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                        }

                        span.log("Last response chunk");
                        LOG.debug("Handled last response chunk");
                    }
                }

                if (!handled) {
                    // If we get here, handling for the response type should be added
                    LOG.error("Got unknown response type '{}', this is a bug", httpObject.getClass());
                }
            }
        } catch (Exception ex) {
            LOG.error("Error handling response", ex);
        }
        return httpObject;
    }

    @Override
    public void proxyToServerResolutionSucceeded(String serverHostAndPort, InetSocketAddress resolvedRemoteAddress) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);
        LOG.debug("Resolved {} to {}", serverHostAndPort, resolvedRemoteAddress);
        this.resolvedRemoteAddress = resolvedRemoteAddress;
    }

    @Override
    public void proxyToServerResolutionFailed(String hostAndPort) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        CrawlLog.Builder crawlLog = buildCrawlLog()
                .setRecordType("response")
                .setStatusCode(ExtraStatusCodes.FAILED_DNS.getCode());
        writeCrawlLog(crawlLog);

//        if (db != null) {
//            db.saveCrawlLog(crawlLog.build());
//        }

        LOG.debug("DNS lookup failed for {}", hostAndPort);
    }

    @Override
    public void proxyToServerConnectionFailed() {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        CrawlLog.Builder crawlLog = buildCrawlLog()
                .setRecordType("response")
                .setStatusCode(ExtraStatusCodes.CONNECT_FAILED.getCode());
        writeCrawlLog(crawlLog);

//        if (db != null) {
//            db.saveCrawlLog(crawlLog.build());
//        }

        LOG.info("Http connect failed");
//        finishSpan(uriRequest);
    }

    @Override
    public void serverToProxyResponseTimedOut() {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        CrawlLog.Builder crawlLog = buildCrawlLog()
                .setRecordType("response")
                .setStatusCode(ExtraStatusCodes.HTTP_TIMEOUT.getCode());
        writeCrawlLog(crawlLog);

//        if (db != null) {
//            db.saveCrawlLog(crawlLog.build());
//        }

        LOG.info("Http connect timed out");
//        finishSpan(uriRequest);
    }

    private void writeCrawlLog(CrawlLog.Builder crawlLog) {
        if (uri.endsWith("robots.txt")) {
            crawlLog.setDiscoveryPath("P");
            try {
                db.saveCrawlLog(crawlLog.build());
            } catch (DbException e) {
                LOG.warn("Could not write crawl log for robots.txt entry", e);
            }
        } else if (browserSession != null) {
            crawlLogEntry.setCrawlLog(crawlLog);
        } else {
            LOG.error("Browser session missing");
        }
    }

    private CrawlLog.Builder buildCrawlLog() {
        Uri surtUri = UriConfigs.SURT_KEY.buildUri(uri);
        Timestamp now = ProtoUtils.getNowTs();
        Duration fetchDuration = Timestamps.between(fetchTimeStamp, now);

        CrawlLog.Builder crawlLog = CrawlLog.newBuilder()
                .setTimeStamp(now)
                .setExecutionId(executionId)
                .setJobExecutionId(jobExecutionId)
                .setRequestedUri(uri)
                .setSurt(surtUri.toString())
                .setFetchTimeStamp(fetchTimeStamp)
                .setFetchTimeMs(Durations.toMillis(fetchDuration));
        return crawlLog;
    }

    private Span getRequestSpan() {
        if (requestSpan != null) {
            return requestSpan;
        }
        return buildSpan("clientToProxyRequest");
    }

    private Span getResponseSpan() {
        if (responseSpan != null) {
            return responseSpan;
        }
        return buildSpan("serverToProxyResponse");
    }

    private Span buildSpan(String operationName) {
        Tracer.SpanBuilder newSpan = GlobalTracer.get()
                .buildSpan(operationName)
                .withTag(Tags.COMPONENT.getKey(), "RecorderFilter")
                .withTag("executionId", executionId)
                .withTag("uri", uri);

        if (browserSession != null && browserSession.getUriRequests().getPageSpan() != null) {
            return newSpan.asChildOf(browserSession.getUriRequests().getPageSpan()).startManual();
        } else {
            return newSpan.ignoreActiveSpan().startManual();
        }
    }

    @Override
    protected void finalize() {
        if (contentWriterSession != null && contentWriterSession.isOpen()) {
            try {
                contentWriterSession.cancel("Session was not completed");
            } catch (InterruptedException e) {
                LOG.info("Finalizer got error while closing content writer session: {}", e.toString());
            }
        }
    }
}
