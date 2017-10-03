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

import no.nb.nna.broprox.chrome.client.NetworkDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class PageRequestRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PageRequestRegistry.class);

    // List of all requests since redirects reuses requestId
    private final List<PageRequest> allRequests = new ArrayList<>();

    private final Map<String, PageRequest> pageRequestsById = new HashMap<>();

    private final Map<String, CompletableFuture<PageRequest>> pageRequestsByUrl = new ConcurrentHashMap<>();

    private PageRequest rootRequest;

    private PageRequest getById(String requestId) {
        return pageRequestsById.get(requestId);
    }

    public synchronized CompletableFuture<PageRequest> getByUrl(String url) {
        return pageRequestsByUrl.compute(url, (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();
            }
            return v;
        });
    }

    public synchronized void add(PageRequest pageRequest) {
        if (pageRequest.isRootResource()) {
            rootRequest = pageRequest;
        }

        pageRequestsById.put(pageRequest.getRequestId(), pageRequest);

        pageRequestsByUrl.compute(pageRequest.getUrl(), (k, v) -> {
            if (v == null) {
                v = new CompletableFuture<>();
            }
            v.complete(pageRequest);
            return v;
        });
    }

    public PageRequest getRootRequest() {
        return rootRequest;
    }

    public long getBytesDownloaded() {
        return allRequests.stream().mapToLong(r -> r.getSize()).sum();
    }

    public int getUriDownloadedCount() {
        return (int) allRequests.stream().filter(r -> !r.isFromCache()).count();
    }

    void onRequestWillBeSent(NetworkDomain.RequestWillBeSent request, String rootDiscoveryPath) {
        PageRequest pageRequest = getById(request.requestId);

        if (pageRequest != null) {
            // Already got request for this id
            if (request.redirectResponse == null) {
                return;
            } else {
                // Redirect response
                // TODO: write crawl log
                //this.responseReceived(requestId, loaderId, time, Protocol.Page.ResourceType.Other, redirectResponse, frameId);
                //networkRequest = this._appendRedirect(requestId, time, request.url);
                pageRequest = new PageRequest(request, pageRequest);
                allRequests.add(pageRequest);
            }
        } else {
            String referrer = (String) request.request.headers.get("Referer");
            if (referrer != null) {
                PageRequest parent = getByUrl(referrer).getNow(null);
                pageRequest = new PageRequest(request, parent);
            } else {
                // New request
                pageRequest = new PageRequest(request, rootDiscoveryPath);
            }
            allRequests.add(pageRequest);
        }
        add(pageRequest);
    }

    void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
//        MDC.put("eid", queuedUri.getExecutionId());
//        MDC.put("uri", queuedUri.getUri());
        if (!f.canceled) {
            LOG.error(
                    "Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Req: {}",
                    f.errorText, f.blockedReason, f.type, f.canceled, getById(f.requestId).getUrl());
            PageRequest request = getById(f.requestId);
        }
        MDC.clear();
    }

    void onResponseReceived(NetworkDomain.ResponseReceived r) {
        PageRequest request = getById(r.requestId);
        if (request == null) {
            LOG.error(
                    "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                    r.requestId, r.loaderId, r.timestamp, r.type, r.response, r.frameId);
        } else {
            request.addResponse(r);
        }
    }

}
