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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 *
 */
public class UriRequestRegistry implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequestRegistry.class);

    private final String executionId;

    // List of all requests since redirects reuses requestId
    private final List<UriRequest> allRequests = new ArrayList<>();

    private final Map<String, UriRequest> requestsByInterceptionId = new HashMap<>();

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

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private final Lock allRequestsLock = new ReentrantLock();
    private final Condition allRequestsUpdate = allRequestsLock.newCondition();

    public UriRequestRegistry(final String executionId, final BaseSpan span) {
        this.executionId = executionId;
        this.span = span;
    }

    public synchronized UriRequest getByInterceptionId(String interceptionId) {
        return requestsByInterceptionId.get(interceptionId);
    }

    public UriRequest getByRequestId(String requestId) {
        allRequestsLock.lock();
        try {
            long timeLimit = System.currentTimeMillis() + 5000L;
            while (System.currentTimeMillis() < timeLimit) {
                for (UriRequest r : allRequests) {
                    if (r.getRequestId().contains(requestId) && r.getChildren().isEmpty()) {
                        return r;
                    }
                }
                allRequestsUpdate.await(1, TimeUnit.SECONDS);
            }

            LOG.error("Request for id {} not found", requestId);
            throw new RuntimeException("Request for id " + requestId + " not found");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            allRequestsLock.unlock();
        }
    }

    public UriRequest getByUrl(String url, boolean onlyLeafNodes) {
        allRequestsLock.lock();
        try {
            long timeLimit = System.currentTimeMillis() + 5000L;
            while (System.currentTimeMillis() < timeLimit) {
                for (UriRequest r : allRequests) {
                    if (r.getUrl().equals(url) && (!onlyLeafNodes || r.getChildren().isEmpty())) {
                        return r;
                    }
                }
                allRequestsUpdate.await(1, TimeUnit.SECONDS);
            }
            LOG.error("Request for url {} not found", url, new RuntimeException());
            throw new RuntimeException("Request for url " + url + " not found");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            allRequestsLock.unlock();
        }
    }

    public UriRequest getByUrlWithEqualOrEmptyRequestId(String url, String requestId) {
        allRequestsLock.lock();
        try {
            long timeLimit = System.currentTimeMillis() + 5000L;
            while (System.currentTimeMillis() < timeLimit) {
                for (UriRequest r : allRequests) {
                    if (r.getUrl().equals(url) && r.getChildren().isEmpty()) {
                        if (r.getRequestId().contains(requestId) || r.getRequestId().isEmpty()) {
                            return r;
                        }
                    }
                }
                allRequestsUpdate.await(1, TimeUnit.SECONDS);
            }
            LOG.error("Request for url {} not found", url, new RuntimeException());
            throw new RuntimeException("Request for url " + url + " not found");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            allRequestsLock.unlock();
        }
    }

    public void add(UriRequest pageRequest) {
        allRequestsLock.lock();
        try {
            if (pageRequest.isRootResource()) {
                rootRequest = pageRequest;
            }
            allRequests.add(pageRequest);
            requestsByInterceptionId.put(pageRequest.getInterceptionId(), pageRequest);

            pageRequest.start();
            allRequestsUpdate.signalAll();
        } finally {
            allRequestsLock.unlock();
        }
    }

    public UriRequest getRootRequest() {
        return rootRequest;
    }

    public UriRequest getInitialRequest() {
        return initialRequest;
    }

    public List<UriRequest> getAllRequests() {
        return allRequests;
    }

    public long getBytesDownloaded() {
        return allRequests.stream().mapToLong(r -> r.getSize()).sum();
    }

    public int getUriDownloadedCount() {
        return (int) allRequests.stream().filter(r -> !r.isFromCache()).count();
    }

    public Stream<Resource> getPageLogResources() {
        return allRequests.stream()
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

        if (request.redirectUrl == null && (request.responseHeaders != null || request.responseStatusCode != null || request.responseErrorReason != null)) {
            String responseInterceptionId = (String) request.responseHeaders.get(CHROME_INTERCEPTION_ID);
            if (!request.interceptionId.equals(responseInterceptionId)) {
                allRequestsLock.lock();
                try {
                    UriRequest old = getByInterceptionId(request.interceptionId);
                    old.getParent().getChildren().remove(old);
                    old.getParent().getChildren().add(getByInterceptionId(responseInterceptionId));
                    allRequests.remove(old);
                    old.finish();
                    allRequestsUpdate.signalAll();
                } finally {
                    allRequestsLock.unlock();
                }
            }
            return;
        }

        UriRequest newRequest;
        UriRequest parent = getByInterceptionId(request.interceptionId);
        if (parent == null) {
            newRequest = new UriRequest(request, aborted, span);
        } else if (request.redirectUrl != null) {
            newRequest = new UriRequest(parent, request, aborted, span);
        } else if (request.authChallenge != null) {
            newRequest = new UriRequest(parent, request, aborted, span);
            // TODO: Handle auth challenge
            LOG.error("TODO: Handle auth challenge");
        } else {
            newRequest = new UriRequest(parent, request, aborted, span);
        }

        allRequestsLock.lock();
        try {
            String referrer;
            if (allRequests.isEmpty()) {
                initialRequest = newRequest;
                newRequest.setDiscoveryPath(queuedUri.getDiscoveryPath());
                referrer = queuedUri.getReferrer();
            } else {
                referrer = rootRequest.getUrl();
            }
            newRequest.setReferrer((String) request.request.headers.getOrDefault("Referer", referrer));
            add(newRequest);
        } finally {
            allRequestsLock.unlock();
        }
    }

    private class RequestWillBeSentHandler implements Runnable {
        final NetworkDomain.RequestWillBeSent request;

        public RequestWillBeSentHandler(NetworkDomain.RequestWillBeSent request) {
            this.request = request;
        }

        @Override
        public void run() {
            MDC.put("eid", executionId);
            MDC.put("uri", request.request.url);
            if (request.redirectResponse == null) {
                UriRequest r = getByUrlWithEqualOrEmptyRequestId(request.request.url, request.requestId);
                r.setRequestId(request.requestId);
                if (r.getParent() == null) {
                    r.addRequest(request, rootRequest.getDiscoveryPath());
                } else {
                    r.addRequest(request, r.getParent().getDiscoveryPath());
                }
            } else if (!request.redirectResponse.fromDiskCache) {
                UriRequest r = getByUrl(request.redirectResponse.url, false);
                r.setRequestId(request.requestId);
                r.addRedirectResponse(request.redirectResponse);
            }
        }
    }

    void onRequestWillBeSent(NetworkDomain.RequestWillBeSent request) {
        // The order of events from Chrome is a bit random. In case this event is triggered before requestIntercepted,
        // we have to wait until that event is triggered. This is done in a separate thread.
        executorService.submit(new RequestWillBeSentHandler(request));
    }

    void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
        UriRequest request = getByRequestId(f.requestId);
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

    void onResponseReceived(NetworkDomain.ResponseReceived r) {
        MDC.put("eid", executionId);
        MDC.put("uri", r.response.url);

        if (!r.response.fromDiskCache) {
            allRequestsLock.lock();
            try {
                UriRequest request = getByInterceptionId((String) r.response.headers.get(CHROME_INTERCEPTION_ID));
                if (request == null) {
                    LOG.error(
                            "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                            r.requestId, r.loaderId, r.timestamp, r.type, r.response, r.frameId);
                } else {
                    request.addResponse(r);
                }
            } finally {
                allRequestsLock.unlock();
            }
        }

    }

    @Override
    public synchronized void close() {
        // Ensure that potentially unfinished spans are finshed
        allRequests.forEach(r -> r.finish());
    }

    public String printAllRequests() {
        StringBuilder sb = new StringBuilder();
        allRequests.forEach(r -> sb.append("- " + r).append('\n'));
        return sb.toString();
    }
}
