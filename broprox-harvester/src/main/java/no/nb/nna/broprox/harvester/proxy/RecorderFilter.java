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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.harvester.OpenTracingSpans;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter implements BroproxHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilter.class);

    private final String uri;

    private final AlreadyCrawledCache cache;

    private final CrawlLog.Builder crawlLog;

    private final ContentCollector requestCollector;

    private final ContentCollector responseCollector;

    private final ContentWriterClient contentWriterClient;

    private final DbAdapter db;

    private long payloadSizeField;

    private String executionId;

    private boolean toBeCached = false;

    private HttpResponseStatus responseStatus;

    private HttpVersion httpVersion;

    public RecorderFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx,
            final DbAdapter db, final ContentWriterClient contentWriterClient, final AlreadyCrawledCache cache) {

        super(originalRequest.setUri(uri), ctx);
        this.db = db;
        this.uri = uri;
        this.contentWriterClient = contentWriterClient;

        this.crawlLog = CrawlLog.newBuilder()
                .setRequestedUri(uri)
                .setSurt(UriConfigs.SURT_KEY.buildUri(uri).toString());
        this.requestCollector = new ContentCollector(db, ctx, contentWriterClient);
        this.responseCollector = new ContentCollector(db, ctx, contentWriterClient);
        this.cache = cache;
    }

    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) httpObject;

            executionId = request.headers().get(EXECUTION_ID);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proxy got request for {} from execution {}", uri, executionId);
            }

            OpenTracingWrapper otw = new OpenTracingWrapper("RecorderFilter").setParentSpan(OpenTracingSpans
                    .get(executionId));

            return otw.map("clientToProxyRequest", req -> {

                if (req.headers().get(DISCOVERY_PATH).endsWith("E")) {
                    FullHttpResponse cachedResponse = cache.get(uri, req.headers().get(EXECUTION_ID), req.headers()
                            .get(ALL_EXECUTION_IDS));
                    if (cachedResponse != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Found {} in cache", uri);
                        }
                        cachedResponse.headers().add("Connection", "close");
                        return cachedResponse;
                    } else {
                        toBeCached = true;
                    }
                }

                // Fix headers before sending to final destination
                req.headers().set("Accept-Encoding", "identity");
                crawlLog.setFetchTimeStamp(ProtoUtils.getNowTs())
                        .setReferrer(req.headers().get("referer", ""))
                        .setDiscoveryPath(req.headers().get(DISCOVERY_PATH, ""));

                req.headers()
                        .remove(DISCOVERY_PATH)
                        .remove(EXECUTION_ID)
                        .remove(ALL_EXECUTION_IDS);

                // Store request
                requestCollector.addHeader(req.headers());
                requestCollector.writeRequest(crawlLog.build());

                return null;
            }, request);
        }

        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        OpenTracingWrapper otw = new OpenTracingWrapper("RecorderFilter").setParentSpan(OpenTracingSpans
                .get(executionId));

        return otw.map("serverToProxyResponse", response -> {

            if (response instanceof HttpResponse) {
                HttpResponse res = (HttpResponse) response;
                responseStatus = res.status();
                httpVersion = res.protocolVersion();

                crawlLog.setStatusCode(responseStatus.code())
                        .setContentType(res.headers().get("Content-Type"));
                responseCollector.addHeader(res.headers());

                try {
                    payloadSizeField = res.headers().getInt("Content-Length");
                } catch (NullPointerException ex) {
                    payloadSizeField = 0L;
                }

            } else if (response instanceof HttpContent) {
                HttpContent res = (HttpContent) response;
                responseCollector.addPayload(res.content());

                if (ProxyUtils.isLastChunk(response)) {
                    if (toBeCached) {
                        cache.put(httpVersion,
                                responseStatus,
                                uri,
                                executionId,
                                responseCollector.getHeaderBuf(),
                                responseCollector.getPayloadBuf());
                    }
                    responseCollector.writeResponse(crawlLog.build());
                }
            } else {
                System.out.println(this.hashCode() + " :: RESP: " + response.getClass());
            }

            return response;
        }, httpObject);
    }

    @Override
    public void proxyToServerResolutionSucceeded(String serverHostAndPort, InetSocketAddress resolvedRemoteAddress) {
        crawlLog.setIpAddress(resolvedRemoteAddress.getAddress().getHostAddress());
    }

    @Override
    public void proxyToServerResolutionFailed(String hostAndPort) {
        crawlLog.setRecordType("response")
                .setStatusCode(-1)
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.addCrawlLog(crawlLog.build());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("DNS lookup failed for {}", hostAndPort);
        }
    }

    @Override
    public void proxyToServerConnectionFailed() {
        crawlLog.setRecordType("response")
                .setStatusCode(-2)
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.addCrawlLog(crawlLog.build());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Http connect failed for {}", uri);
        }
    }

    @Override
    public void serverToProxyResponseTimedOut() {
        crawlLog.setRecordType("response")
                .setStatusCode(-4)
                .setFetchTimeStamp(ProtoUtils.getNowTs());

        if (db != null) {
            db.addCrawlLog(crawlLog.build());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Http connect timed out for {}", uri);
        }
    }

}
