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
package no.nb.nna.broprox.harvester.browsercontroller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.opentracing.ActiveSpan;
import io.opentracing.BaseSpan;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.chrome.client.NetworkDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class UriRequestRegistry implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequestRegistry.class);

    // List of all requests since redirects reuses requestId
    private final List<UriRequest> allRequests = new ArrayList<>();

    private final Map<String, UriRequest> requestsById = new HashMap<>();

    private final Map<String, CompletableFuture<UriRequest>> requestsByUrl = new ConcurrentHashMap<>();

    private UriRequest rootRequest;

    private BaseSpan span;

    public UriRequestRegistry(BaseSpan span) {
        this.span = span;
    }

    private UriRequest getById(String requestId) {
        return requestsById.get(requestId);
    }

    public synchronized CompletableFuture<UriRequest> getByUrl(String url) {
        return requestsByUrl.compute(url, (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();
            }
            return v;
        });
    }

    public synchronized void add(UriRequest pageRequest) {
        if (pageRequest.isRootResource()) {
            rootRequest = pageRequest;
        }

        requestsById.put(pageRequest.getRequestId(), pageRequest);

        requestsByUrl.compute(pageRequest.getUrl(), (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();
            }
            v.complete(pageRequest);
            return v;
        });
    }

    public UriRequest getRootRequest() {
        return rootRequest;
    }

    public long getBytesDownloaded() {
        return allRequests.stream().mapToLong(r -> r.getSize()).sum();
    }

    public int getUriDownloadedCount() {
        return (int) allRequests.stream().filter(r -> !r.isFromCache()).count();
    }

    void onRequestWillBeSent(NetworkDomain.RequestWillBeSent request, String rootDiscoveryPath) {
        UriRequest uriRequest = getById(request.requestId);

            if (uriRequest != null) {
                // Already got request for this id
                if (request.redirectResponse == null) {
                    return;
                } else {
                    // Redirect response
                    // TODO: write crawl log
                    //this.responseReceived(requestId, loaderId, time, Protocol.Page.ResourceType.Other, redirectResponse, frameId);
                    //networkRequest = this._appendRedirect(requestId, time, request.url);
                    uriRequest = new UriRequest(request, uriRequest, span);
                    allRequests.add(uriRequest);
                }
            } else {
                String referrer = (String) request.request.headers.get("Referer");
                if (referrer != null) {
                    UriRequest parent = getByUrl(referrer).getNow(null);
                    uriRequest = new UriRequest(request, parent, span);
                } else {
                    // New request
                    uriRequest = new UriRequest(request, rootDiscoveryPath, span);
                }
                allRequests.add(uriRequest);
            }
            add(uriRequest);
    }

    void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
//        MDC.put("eid", queuedUri.getExecutionId());
//        MDC.put("uri", queuedUri.getUri());
        UriRequest request = getById(f.requestId);
        if (!f.canceled) {
            LOG.error(
                    "Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Req: {}",
                    f.errorText, f.blockedReason, f.type, f.canceled, getById(f.requestId).getUrl());
        }
        MDC.clear();

        request.getSpan().finish();
    }

    void onResponseReceived(NetworkDomain.ResponseReceived r) {
        UriRequest request = getById(r.requestId);
        if (request == null) {
            LOG.error(
                    "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                    r.requestId, r.loaderId, r.timestamp, r.type, r.response, r.frameId);
        } else {
            request.addResponse(r);
        }
    }

    @Override
    public void close() {
        // Ensure that potensially unfinished spans are finshed
        allRequests.forEach(r -> r.getSpan().finish());
    }

}
