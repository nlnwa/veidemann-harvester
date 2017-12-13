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
package no.nb.nna.veidemann.harvester.browsercontroller;

import io.opentracing.BaseSpan;
import no.nb.nna.veidemann.api.MessagesProto.PageLog.Resource;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.NetworkDomain;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class UriRequestRegistry implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequestRegistry.class);

    private final String executionId;

    // List of all requests since redirects reuses requestId
    private final List<UriRequest> allRequests = new ArrayList<>();

    private final Map<String, String> requestIdToUrl = new HashMap<>();

    private final Map<String, UriRequest> requestsByInterceptionId = new HashMap<>();

    private final Map<String, UriRequest> requestsByUrl = new HashMap<>();

    /**
     * The request initializing the page load
     */
    private UriRequest initialRequest;

    /**
     * The root request for the page. Usually the same as initialRequest,
     * but in case of an initial redirect, this will point to the request with content.
     */
    private UriRequest rootRequest;

    private final BaseSpan span;

    public UriRequestRegistry(final String executionId, final BaseSpan span) {
        this.executionId = executionId;
        this.span = span;
    }

    private synchronized String getUrlForRequestId(String requestId) {
        return requestIdToUrl.get(requestId);
    }

    public synchronized UriRequest getByInterceptionId(String interceptionId) {
        return requestsByInterceptionId.get(interceptionId);
    }

    public synchronized UriRequest getByUrl(String url) {
        return requestsByUrl.get(url);
    }

    public synchronized void add(UriRequest pageRequest) {
        if (!requestsByUrl.containsKey(pageRequest.getUrl())) {
            if (pageRequest.isRootResource()) {
                rootRequest = pageRequest;
            }
            allRequests.add(pageRequest);
            requestsByInterceptionId.put(pageRequest.getInterceptionId(), pageRequest);
            requestsByUrl.put(pageRequest.getUrl(), pageRequest);

            pageRequest.start();
        }
    }

    public synchronized UriRequest getRootRequest() {
        return rootRequest;
    }

    public synchronized UriRequest getInitialRequest() {
        return initialRequest;
    }

    public synchronized long getBytesDownloaded() {
        return allRequests.stream().mapToLong(r -> r.getSize()).sum();
    }

    public synchronized int getUriDownloadedCount() {
        return (int) allRequests.stream().filter(r -> !r.isFromCache()).count();
    }

    public synchronized Stream<Resource> getPageLogResources() {
        return allRequests.stream()
                .filter(r -> r.getParent() != null)
                .map(r -> {
                    Resource.Builder b = Resource.newBuilder()
                            .setUri(r.getUrl())
                            .setFromCache(r.isFromCache())
                            .setRenderable(r.isRenderable())
                            .setMimeType(r.getMimeType())
                            .setStatusCode(r.getStatusCode())
                            .setDiscoveryPath(r.getDiscoveryPath())
                            .setWarcId(r.getWarcId())
                            .setReferrer(r.getReferrer());

                    if (r.getResourceType() != null) {
                        b.setResourceType(r.getResourceType().category.shortTitle + "/" + r.getResourceType().title);
                    }
                    return b.build();
                });
    }

    void onRequestIntercepted(NetworkDomain.RequestIntercepted request, QueuedUri queuedUri, boolean aborted) {
        MDC.put("eid", executionId);
        MDC.put("uri", request.request.url);

        UriRequest newRequest;
        UriRequest parent = getByInterceptionId(request.interceptionId);
        if (parent == null) {
            newRequest = new UriRequest(request, queuedUri.getReferrer(), span);
        } else if (request.redirectUrl != null) {
            newRequest = new UriRequest(parent, request, span);
        } else {
            throw new RuntimeException("TODO: Handle auth challenge");
        }
        if (allRequests.isEmpty()) {
            initialRequest = newRequest;
            newRequest.setDiscoveryPath(queuedUri.getDiscoveryPath());
        }
        add(newRequest);
    }

    synchronized void onRequestWillBeSent(NetworkDomain.RequestWillBeSent request) {
        MDC.put("eid", executionId);
        MDC.put("uri", request.request.url);

        if (request.redirectResponse == null) {
            UriRequest initiator = getByUrl(request.initiator.url);
            UriRequest req = getByUrl(request.request.url);
            if (req == null) {
                LOG.error("Could not find request: {}" + request.request.url);
            } else {
                if (initiator != null) {
                    req.setParent(initiator);
                }
                req.addRequest(request);
                requestIdToUrl.put(request.requestId, request.request.url);
            }
        } else if (!request.redirectResponse.fromDiskCache) {
            UriRequest req = getByUrl(request.redirectResponse.url);
            req.addRedirectResponse(request.redirectResponse);
            requestIdToUrl.put(request.requestId, request.redirectResponse.url);
        }
    }

    synchronized void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
        UriRequest request = getByUrl(getUrlForRequestId(f.requestId));
        MDC.put("eid", executionId);
        MDC.put("uri", request.getUrl());
        if (!f.canceled) {
            LOG.error(
                    "Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Req: {}",
                    f.errorText, f.blockedReason, f.type, f.canceled, request.getUrl());
        }
        MDC.clear();

        request.finish();
    }

    synchronized void onResponseReceived(NetworkDomain.ResponseReceived r) {
        MDC.put("eid", executionId);
        MDC.put("uri", r.response.url);

        System.out.println("XXXX: " + r.response.headers.get(CHROME_INTERCEPTION_ID));
        UriRequest request = getByUrl(r.response.url);
        if (request == null) {
            LOG.error(
                    "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                    r.requestId, r.loaderId, r.timestamp, r.type, r.response, r.frameId);
        } else {
            if (!r.response.fromDiskCache) {
                request.addResponse(r);
            }
        }

    }

    @Override
    public synchronized void close() {
        // Ensure that potensially unfinished spans are finshed
        allRequests.forEach(r -> r.finish());
    }

}
