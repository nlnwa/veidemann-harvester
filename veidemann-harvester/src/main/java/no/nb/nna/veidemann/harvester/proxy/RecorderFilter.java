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
import io.netty.handler.codec.http.*;
import io.opentracing.ActiveSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
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
import no.nb.nna.veidemann.harvester.browsercontroller.UriRequest;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetSocketAddress;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private final String uri;

    private final BrowserSessionRegistry sessionRegistry;

    private final AlreadyCrawledCache cache;

    private CrawlLog.Builder crawlLog;

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
        ContentWriterSession contentWriterSession = contentWriterClient.createSession();
        this.requestCollector = new ContentCollector(db, contentWriterSession);
        this.responseCollector = new ContentCollector(db, contentWriterSession);
        this.sessionRegistry = sessionRegistry;
        this.cache = cache;
    }

    @Override
    public HttpResponse proxyToServerRequest(HttpObject httpObject) {
        try {
            if (httpObject instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) httpObject;

                executionId = request.headers().get(EXECUTION_ID);
                if (executionId == null) {
                    executionId = MANUAL_EXID;
                    LOG.info("Manual download of {}", uri);
                }

                MDC.put("eid", executionId);
                MDC.put("uri", uri);

                LOG.debug("Proxy sending request");

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
                    request.headers().remove(CHROME_INTERCEPTION_ID);

                    crawlLog.setFetchTimeStamp(ProtoUtils.getNowTs())
                            .setReferrer(referrer)
                            .setDiscoveryPath(discoveryPath)
                            .setExecutionId(executionId);

                    // Store request
                    requestCollector.setRequestHeaders(request, crawlLog);
                    crawlLog = requestCollector.writeRequest(crawlLog.build()).toBuilder();
                    LOG.debug("Proxy is sending request to final destination.");
                }
                return null;
            } else if (httpObject instanceof HttpContent) {
                // TODO: Request body is not handled yet
                LOG.debug("TODO: Request body is not handled yet");
                HttpContent request = (HttpContent) httpObject;
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
        try {
            responseSpan = buildSpan("serverToProxyResponse", uriRequest);
            GlobalTracer.get().makeActive(requestSpan);

            boolean handled = false;

            if (httpObject instanceof HttpResponse) {
                LOG.debug("Got http response");

                HttpResponse res = (HttpResponse) httpObject;
                responseCollector.setResponseHeaders(res, crawlLog);

                if (uriRequest != null) {
                    res.headers().add(CHROME_INTERCEPTION_ID, uriRequest.getInterceptionId());
                }

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
                    uriRequest.setWarcId(warcId);
                    finishSpan(uriRequest);
                }
            }

            if (!handled) {
                // If we get here, handling for the response type should be added
                LOG.error("Got unknown response type '{}', this is a bug", httpObject.getClass());
            }
        } catch (Throwable t) {
            LOG.error("Error handling response", t);
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

        LOG.info("Http connect failed");
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

        LOG.info("Http connect timed out");
        finishSpan(uriRequest);
    }

    @Override
    public void proxyToServerConnectionSSLHandshakeStarted() {
        LOG.info("proxyToServerConnectionSSLHandshakeStarted");
    }

    private void initDiscoveryPathAndReferrer(HttpRequest request) {
        referrer = request.headers().get("Referer", "");
        if (uri.endsWith("robots.txt")) {
            referrer = "";
            discoveryPath = "P";
        } else if (executionId == MANUAL_EXID) {
            discoveryPath = "";
        } else {
            session = sessionRegistry.get(executionId);

            if (session == null) {
                LOG.error("Could not find session. Probably a bug");
                discoveryPath = "";
            } else {
                String interceptionId = request.headers().get(CHROME_INTERCEPTION_ID);

                if (interceptionId != null) {
                    uriRequest = session.getUriRequests().getByInterceptionId(interceptionId);
                } else {
                    uriRequest = session.getUriRequests().getByUrl(uri, true);
                }

                if (uriRequest != null) {
                    discoveryPath = uriRequest.getDiscoveryPath();
                    if (referrer.isEmpty()) {
                        referrer = uriRequest.getReferrer();
                    }
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
            uriRequest.finish();
        }
    }

}
