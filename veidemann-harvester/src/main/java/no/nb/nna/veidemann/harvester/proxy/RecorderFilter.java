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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.opentracing.ActiveSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.commons.AlreadyCrawledCache;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import no.nb.nna.veidemann.harvester.browsercontroller.UriRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private final String uri;

    private final BrowserSessionRegistry sessionRegistry;

    private final AlreadyCrawledCache cache;

    private final CrawlLog.Builder crawlLog;

    private final ContentCollector requestCollector;

    private final ContentCollector responseCollector;

    private final DbAdapter db;

    private String executionId;

    private UriRequest uriRequest;

    private BrowserSession session;

    private String discoveryPath;

    private String referrer;

    private Span requestSpan;

    private Span responseSpan;

    public RecorderFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx,
                          final DbAdapter db, final ContentWriterClient contentWriterClient,
                          final BrowserSessionRegistry sessionRegistry, final AlreadyCrawledCache cache) {

        super(originalRequest, ctx);
        this.db = db;
        this.uri = uri;

        Uri surtUri = UriConfigs.SURT_KEY.buildUri(uri);
        this.crawlLog = CrawlLog.newBuilder()
                .setRequestedUri(uri)
                .setSurt(surtUri.toString());
        this.requestCollector = new ContentCollector(db, contentWriterClient);
        this.responseCollector = new ContentCollector(db, contentWriterClient);
        this.sessionRegistry = sessionRegistry;
        this.cache = cache;
    }

    @Override
    public HttpResponse proxyToServerRequest(HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) httpObject;

            executionId = request.headers().get(EXECUTION_ID);
            if (executionId == null) {
                executionId = MANUAL_EXID;
                LOG.info("Manual download of {}", uri);
            }

            MDC.put("eid", executionId);
            MDC.put("uri", uri);

            LOG.debug("Proxy got request");

            initDiscoveryPathAndReferrer(request);

            requestSpan = buildSpan("clientToProxyRequest", uriRequest);
            try (ActiveSpan span = GlobalTracer.get().makeActive(requestSpan)) {

                if (discoveryPath.endsWith("E")) {
                    FullHttpResponse cachedResponse = cache.get(uri, executionId);
                    if (cachedResponse != null) {
                        LOG.debug("Found in cache");
                        requestSpan.log("Loaded from cache");
                        if (uriRequest != null) {
                            uriRequest.setFromCache(true);
                        }
                        return cachedResponse;
                    } else {
                        responseCollector.setShouldCache(true);
                    }
                }

                // Fix headers before sending to final destination
                request.headers().set("Accept-Encoding", "identity");
                request.headers().remove(EXECUTION_ID);

                crawlLog.setFetchTimeStamp(ProtoUtils.getNowTs())
                        .setReferrer(request.headers().get("referer", referrer))
                        .setDiscoveryPath(discoveryPath)
                        .setExecutionId(executionId);

                // Store request
                requestCollector.setRequestHeaders(request, crawlLog);
                requestCollector.writeRequest(crawlLog.build());

                LOG.debug("Proxy is sending request to final destination.");
            }
            return null;
        }
        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {

        responseSpan = buildSpan("serverToProxyResponse", uriRequest);
        GlobalTracer.get().makeActive(requestSpan);

        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        boolean handled = false;

        if (httpObject instanceof HttpResponse) {
            LOG.debug("Got http response");

            HttpResponse res = (HttpResponse) httpObject;
            responseCollector.setResponseHeaders(res, crawlLog);

            responseSpan.log("Got response headers");
            handled = true;
        }

        if (httpObject instanceof HttpContent) {
            LOG.debug("Got http content");

            HttpContent res = (HttpContent) httpObject;
            responseSpan.log("Got http content. Size: " + res.content().readableBytes());
            responseCollector.addPayload(res.content());

            handled = true;
        }

        if (ProxyUtils.isLastChunk(httpObject)) {
            responseCollector.writeCache(cache, uri, executionId);

            String warcId = responseCollector.writeResponse(crawlLog.build()).getWarcId();

            responseSpan.log("Last chunk");
            if (uriRequest != null) {
                uriRequest.setSize(responseCollector.getSize());
                responseSpan.finish();
                finishSpan(uriRequest);
                uriRequest.setWarcId(warcId);
            }
        }

        if (!handled) {
            // If we get here, handling for the response type should be added
            LOG.error("Got unknown response type '{}', this is a bug", httpObject.getClass());
        }
        return httpObject;
    }

    @Override
    public void proxyToServerResolutionSucceeded(String serverHostAndPort, InetSocketAddress resolvedRemoteAddress) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);
        LOG.debug("Resolved {} to {}", serverHostAndPort, resolvedRemoteAddress);
        crawlLog.setIpAddress(resolvedRemoteAddress.getAddress().getHostAddress());
    }

    @Override
    public void proxyToServerResolutionFailed(String hostAndPort) {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        crawlLog.setRecordType("response")
                .setStatusCode(ExtraStatusCodes.FAILED_DNS.getCode())
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.saveCrawlLog(crawlLog.build());
        }

        LOG.debug("DNS lookup failed for {}", hostAndPort);
    }

    @Override
    public void proxyToServerConnectionFailed() {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        crawlLog.setRecordType("response")
                .setStatusCode(ExtraStatusCodes.CONNECT_FAILED.getCode())
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.saveCrawlLog(crawlLog.build());
        }

        LOG.debug("Http connect failed");
        finishSpan(uriRequest);
    }

    @Override
    public void serverToProxyResponseTimedOut() {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        crawlLog.setRecordType("response")
                .setStatusCode(ExtraStatusCodes.HTTP_TIMEOUT.getCode())
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.saveCrawlLog(crawlLog.build());
        }

        LOG.debug("Http connect timed out");
        finishSpan(uriRequest);
    }

    private void initDiscoveryPathAndReferrer(HttpRequest request) {
        if (uri.endsWith("robots.txt")) {
            referrer = "";
            discoveryPath = "P";
        } else if (executionId == MANUAL_EXID) {
            referrer = request.headers().get("Referer", "");
            discoveryPath = "";
        } else {
            session = sessionRegistry.get(executionId);

            if (session == null) {
                LOG.error("Could not find session. Probably a bug");
                referrer = request.headers().get("Referer", "");
                discoveryPath = "";
            } else {
                referrer = session.getReferrer();
                try {
                    uriRequest = session.getUriRequests().getByUrl(uri)
                            .get(session.getProtocolTimeout(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    LOG.error("Failed getting page request from session: {}", ex.toString(), ex);
                } catch (Throwable ex) {
                    LOG.error(ex.toString(), ex);
                }

                if (uriRequest != null) {
                    discoveryPath = uriRequest.getDiscoveryPath();
                } else {
                    discoveryPath = "";
                    LOG.info("Could not find page request in session. Probably a bug");
                }
            }
        }
    }

    private Span buildSpan(String operationName, UriRequest uriRequest) {
        Tracer.SpanBuilder newSpan = GlobalTracer.get()
                .buildSpan(operationName)
                .withTag(Tags.COMPONENT.getKey(), "RecorderFilter")
                .withTag("executionId", executionId)
                .withTag("uri", uri);

        if (uriRequest != null && uriRequest.getSpan() == null) {
            LOG.error("Span is not initilized", new IllegalStateException());
        }

        if (uriRequest != null) {
            newSpan = newSpan.asChildOf(uriRequest.getSpan());
        }

        return newSpan.startManual();
    }

    private void finishSpan(UriRequest uriRequest) {
        if (uriRequest != null) {
            uriRequest.getSpan().finish();
        }
    }

}
