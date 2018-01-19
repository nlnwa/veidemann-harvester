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
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.opentracing.ActiveSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ContentWriterProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.commons.AlreadyCrawledCache;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.ContentWriterClient.ContentWriterSession;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import no.nb.nna.veidemann.harvester.browsercontroller.CrawlLogRegistry;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private static final AtomicLong nextProxyRequestId = new AtomicLong();

    private final String uri;

    private CrawlLogRegistry.Entry crawlLogEntry;

    private final BrowserSessionRegistry sessionRegistry;

    private BrowserSession browserSession;

    private final AlreadyCrawledCache cache;

    private ContentCollector requestCollector;

    private ContentCollector responseCollector;

    private final DbAdapter db;

    private String executionId;

    private Timestamp fetchTimeStamp;

    private InetSocketAddress resolvedRemoteAddress;

    private HttpResponseStatus httpResponseStatus;

    private HttpVersion httpResponseProtocolVersion;

    private CrawlLog.Builder crawlLog;

    private Span requestSpan;

    private Span responseSpan;

    private final ContentWriterClient contentWriterClient;

    private ContentWriterSession contentWriterSession;

    public RecorderFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx,
                          final DbAdapter db, final ContentWriterClient contentWriterClient,
                          final BrowserSessionRegistry sessionRegistry, final AlreadyCrawledCache cache) {

        super(originalRequest, ctx);
        this.db = db;
        this.uri = uri;

        this.contentWriterClient = contentWriterClient;
        this.requestCollector = new ContentCollector(0, ContentWriterProto.RecordType.REQUEST, uri, cache, db);
        this.responseCollector = new ContentCollector(1, ContentWriterProto.RecordType.RESPONSE, uri, cache, db);
        this.sessionRegistry = sessionRegistry;
        this.cache = cache;
        this.fetchTimeStamp = ProtoUtils.getNowTs();
        if (ctx.pipeline().get("streamer") == null) {
            ctx.pipeline().addBefore("handler", "streamer", new ChunkedWriteHandler());
        }
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

                executionId = request.headers().get(EXECUTION_ID);
                if (executionId == null) {
                    LOG.error("Missing executionId for {}", uri);
                    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }

                // Lookup browser session, but skip if it is a robots.txt request since that is not coming from browser.
                if (!uri.endsWith("robots.txt")) {
                    browserSession = sessionRegistry.get(executionId);
                    crawlLogEntry = browserSession.getCrawlLogs().registerProxyRequest(nextProxyRequestId.getAndIncrement(), uri);
                }

                MDC.put("eid", executionId);
                MDC.put("uri", uri);

                LOG.debug("Proxy got request");

                requestSpan = buildSpan("clientToProxyRequest");
                try (ActiveSpan span = GlobalTracer.get().makeActive(requestSpan)) {

                    FullHttpResponse cachedResponse = cache.get(uri, executionId);
                    if (cachedResponse != null) {
                        LOG.debug("Found in cache");
                        requestSpan.log("Loaded from cache");
                        CrawlLog.Builder crawlLog = buildCrawlLog()
                                .setStatusCode(cachedResponse.status().code());

                        writeCrawlLog(crawlLog);

                        if (cachedResponse.content().readableBytes() < (1024 * 64)) {
                            return cachedResponse;
                        }

                        return new ChunkedHttpResponse(executionId, uri, cachedResponse);
                    } else {
                        responseCollector.setShouldCache(true);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("Error handling request", t);
        }
        return null;
    }

    @Override
    public HttpResponse proxyToServerRequest(HttpObject httpObject) {
        try {
            if (httpObject instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) httpObject;

                MDC.put("eid", executionId);
                MDC.put("uri", uri);

                LOG.trace("Proxy sending request");

                try (ActiveSpan span = GlobalTracer.get().makeActive(requestSpan)) {
                    // Fix headers before sending to final destination
                    request.headers().set("Accept-Encoding", "identity");
                    request.headers().remove(EXECUTION_ID);

                    // Store request
                    requestCollector.setHeaders(ContentCollector.createRequestPreamble(request), request.headers(), getContentWriterSession());
                    LOG.debug("Proxy is sending request to final destination.");
                }
                return null;
            } else if (httpObject instanceof HttpContent) {
                HttpContent request = (HttpContent) httpObject;
                requestCollector.addPayload(request.content(), getContentWriterSession());
            } else {
                LOG.debug("Got something else than http request: {}", httpObject);
            }
        } catch (Throwable t) {
            LOG.error("Error handling request", t);
        }
        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        if (browserSession != null && browserSession.isClosed()) {
            LOG.warn("Browser session was closed, aborting request");
            if (contentWriterSession != null) {
                contentWriterSession.cancel("Session was aborted");
            }
            return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
        }

        try {
            responseSpan = buildSpan("serverToProxyResponse");
            GlobalTracer.get().makeActive(requestSpan);

            boolean handled = false;

            if (httpObject instanceof HttpResponse) {
                try {
                    HttpResponse res = (HttpResponse) httpObject;
                    LOG.trace("Got response headers {}", res.status());

                    if (res.headers().contains(HttpHeaderNames.SET_COOKIE)
                            || res.headers().contains(HttpHeaderNames.SET_COOKIE2)
                            || res.status().equals(HttpResponseStatus.PARTIAL_CONTENT)) {
                        responseCollector.setShouldCache(false);
                    }

                    httpResponseStatus = res.status();
                    httpResponseProtocolVersion = res.protocolVersion();
                    responseCollector.setHeaders(ContentCollector.createResponsePreamble(res), res.headers(), getContentWriterSession());
                    responseSpan.log("Got response headers");

                    crawlLog = buildCrawlLog()
                            .setStatusCode(httpResponseStatus.code())
                            .setContentType(res.headers().get(HttpHeaderNames.CONTENT_TYPE, ""));

                    handled = true;
                    LOG.trace("Handled response headers");
                } catch (Exception ex) {
                    LOG.error("Error handling response headers", ex);
                    responseSpan.log("Error handling response headers: " + ex.toString());
                    return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }
            }

            if (httpObject instanceof HttpContent) {
                LOG.trace("Got response content");
                try {
                    HttpContent res = (HttpContent) httpObject;
                    responseSpan.log("Got response content. Size: " + res.content().readableBytes());
                    responseCollector.addPayload(res.content(), getContentWriterSession());

                    handled = true;
                    LOG.trace("Handled response content");
                } catch (Exception ex) {
                    LOG.error("Error handling response content", ex);
                    responseSpan.log("Error handling response content: " + ex.toString());
                    contentWriterSession.cancel("Got error while writing response content");
                    return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }
            }

            if (ProxyUtils.isLastChunk(httpObject)) {
                LOG.debug("Got last response chunk. Response status: {}", httpResponseStatus);
                ContentWriterProto.WriteResponseMeta.RecordMeta responseRecordMeta = null;
                try {
                    Duration fetchDuration = Timestamps.between(fetchTimeStamp, ProtoUtils.getNowTs());

                    responseCollector.writeCache(executionId, httpResponseStatus, httpResponseProtocolVersion);

                    String ipAddress = "";
                    if (resolvedRemoteAddress != null) {
                        ipAddress = resolvedRemoteAddress.getAddress().getHostAddress();
                    }

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
                            .setSize(responseCollector.getSize());

                    writeCrawlLog(crawlLog);

                } catch (Exception ex) {
                    LOG.error("Error writing response", ex);
                    responseSpan.log("Error writing response: " + ex.toString());
                    contentWriterSession.cancel("Got error while writing response metadata");
                    return ProxyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
                }

                responseSpan.log("Last response chunk");
                responseSpan.finish();
                LOG.debug("Handled last response chunk");
            }

            if (!handled) {
                // If we get here, handling for the response type should be added
                LOG.error("Got unknown response type '{}', this is a bug", httpObject.getClass());
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
            db.saveCrawlLog(crawlLog.build());
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
                .setRequestedUri(uri)
                .setSurt(surtUri.toString())
                .setFetchTimeStamp(fetchTimeStamp)
                .setFetchTimeMs(Durations.toMillis(fetchDuration));
        return crawlLog;
    }

    private Span buildSpan(String operationName) {
        Tracer.SpanBuilder newSpan = GlobalTracer.get()
                .buildSpan(operationName)
                .withTag(Tags.COMPONENT.getKey(), "RecorderFilter")
                .withTag("executionId", executionId)
                .withTag("uri", uri);

        return newSpan.startManual();
    }

    @Override
    protected void finalize() {
        if (contentWriterSession != null) {
            contentWriterSession.cancel("Session was not completed");
        }
    }
}
