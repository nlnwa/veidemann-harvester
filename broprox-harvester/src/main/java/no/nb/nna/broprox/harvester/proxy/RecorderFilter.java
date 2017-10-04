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
package no.nb.nna.broprox.harvester.proxy;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import no.nb.nna.broprox.commons.AlreadyCrawledCache;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.commons.ExtraStatusCodes;
import no.nb.nna.broprox.commons.client.ContentWriterClient;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.harvester.BrowserSessionRegistry;
import no.nb.nna.broprox.harvester.OpenTracingSpans;
import no.nb.nna.broprox.harvester.browsercontroller.BrowserSession;
import no.nb.nna.broprox.harvester.browsercontroller.PageRequest;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements BroproxHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private final String uri;

    private final BrowserSessionRegistry sessionRegistry;

    private final AlreadyCrawledCache cache;

    private final CrawlLog.Builder crawlLog;

    private final ContentCollector requestCollector;

    private final ContentCollector responseCollector;

    private final DbAdapter db;

    private String executionId;

    private PageRequest pageRequest;

    private BrowserSession session;

    public RecorderFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx,
            final DbAdapter db, final ContentWriterClient contentWriterClient,
            final BrowserSessionRegistry sessionRegistry, final AlreadyCrawledCache cache) {

        super(originalRequest, ctx);
        this.db = db;
        this.uri = uri;

        this.crawlLog = CrawlLog.newBuilder()
                .setRequestedUri(uri)
                .setSurt(UriConfigs.SURT_KEY.buildUri(uri).toString());
        this.requestCollector = new ContentCollector(db, contentWriterClient);
        this.responseCollector = new ContentCollector(db, contentWriterClient);
        this.sessionRegistry = sessionRegistry;
        this.cache = cache;
    }

    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
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

            String discoveryPath;
            String referrer;

            if (uri.endsWith("robots.txt")) {
                referrer = "";
                discoveryPath = "P";
            } else if (executionId != MANUAL_EXID) {
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
                        pageRequest = session.getPageRequests().getByUrl(uri)
                                .get(session.getProtocolTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                        LOG.error("Failed getting page request from session: {}", ex.toString(), ex);
                    } catch (Throwable ex) {
                        LOG.error(ex.toString(), ex);
                    }

                    if (pageRequest != null) {
                        discoveryPath = pageRequest.getDiscoveryPath();
                    } else {
                        discoveryPath = "";
                        LOG.info("Could not find page request in session. Probably a bug");
                    }
                }
            }

            OpenTracingWrapper otw = new OpenTracingWrapper("RecorderFilter").setParentSpan(OpenTracingSpans
                    .get(executionId));

            return otw.map("clientToProxyRequest", req -> {

                if (discoveryPath.endsWith("E")) {
                    FullHttpResponse cachedResponse = cache.get(uri, executionId);
                    if (cachedResponse != null) {
                        LOG.debug("Found in cache");
                        if (pageRequest != null) {
                            pageRequest.setFromCache(true);
                        }
                        return cachedResponse;
                    } else {
                        responseCollector.setShouldCache(true);
                    }
                }

                // Fix headers before sending to final destination
                req.headers().set("Accept-Encoding", "identity");
                req.headers().remove(EXECUTION_ID);

                crawlLog.setFetchTimeStamp(ProtoUtils.getNowTs())
                        .setReferrer(req.headers().get("referer", referrer))
                        .setDiscoveryPath(discoveryPath)
                        .setExecutionId(executionId);

                // Store request
                requestCollector.setRequestHeaders(req);
                requestCollector.writeRequest(crawlLog.build());

                LOG.debug("Proxy is sending request to final destination.");
                return null;
            }, request);
        }

        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        OpenTracingWrapper otw = new OpenTracingWrapper("RecorderFilter").setParentSpan(OpenTracingSpans
                .get(executionId));

        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        return otw.map("serverToProxyResponse", response -> {
            boolean handled = false;

            if (response instanceof HttpResponse) {
                LOG.debug("Got http response");

                HttpResponse res = (HttpResponse) response;
                responseCollector.setResponseHeaders(res, crawlLog);
                handled = true;
            }

            if (response instanceof HttpContent) {
                LOG.debug("Got http content");

                HttpContent res = (HttpContent) response;
                responseCollector.addPayload(res.content());

                if (ProxyUtils.isLastChunk(response)) {
                    responseCollector.writeCache(cache, uri, executionId);

                    responseCollector.writeResponse(crawlLog.build());
                    if (pageRequest != null) {
                        pageRequest.setSize(responseCollector.getSize());
                    }
                }
                handled = true;
            }

            if (!handled) {
                // If we get here, handling for the response type should be added
                LOG.error("Got unknown response type '{}', this is a bug", response.getClass());
            }

            return response;
        }, httpObject);
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
    }

}
