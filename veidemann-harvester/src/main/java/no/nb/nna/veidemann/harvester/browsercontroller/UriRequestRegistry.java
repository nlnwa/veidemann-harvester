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
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class UriRequestRegistry implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequestRegistry.class);

    private final CrawlLogRegistry crawlLogRegistry;

    private final QueuedUri rootRequestUri;

    private final String executionId;

    // List of all requests since redirects reuses requestId
    private final List<UriRequest> allRequests = new ArrayList<>();

    private final Map<String, UriRequest> requestsByRequestId = new HashMap<>();

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

    public UriRequestRegistry(final CrawlLogRegistry crawlLogRegistry, QueuedUri rootRequestUri, final BaseSpan span) {
        this.crawlLogRegistry = crawlLogRegistry;
        this.rootRequestUri = rootRequestUri;
        this.executionId = rootRequestUri.getExecutionId();
        this.span = span;
    }

    public UriRequest getByRequestId(String requestId) {
        allRequestsLock.lock();
        try {
            return requestsByRequestId.get(requestId);
        } finally {
            allRequestsLock.unlock();
        }
    }

    public void add(UriRequest pageRequest) {
        allRequestsLock.lock();
        try {
            if (initialRequest == null) {
                initialRequest = pageRequest;
            }
            if (pageRequest.isRootResource()) {
                rootRequest = pageRequest;
            }
            if (allRequests.contains(pageRequest)) {
                LOG.error("Request {} already added to allRequests", pageRequest.getRequestId());
            }
            allRequests.add(pageRequest);
            requestsByRequestId.put(pageRequest.getRequestId(), pageRequest);

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

    public BaseSpan getPageSpan() {
        return span;
    }

    void onRequestWillBeSent(NetworkDomain.RequestWillBeSent request) {
        LOG.debug("Request will be sent: {}", request.requestId);
        resolveCurrentUriRequest(request.requestId).ifPresent(parent -> {
            // Already got request for this id
            if (request.redirectResponse == null) {
                LOG.error("Already got request, but no redirect");
                return;
            } else {
                LOG.debug("Redirect response: {}, url: {}, cache: {}, redirUrl: {}",
                        request.requestId, request.request.url, request.redirectResponse.fromDiskCache, request.redirectResponse.url);
                UriRequest uriRequest = UriRequest.create(request, parent, span);
                add(uriRequest);
            }
        }).otherwise(() -> {
            UriRequest uriRequest;
            if (getRootRequest() == null) {
                // New request
                uriRequest = UriRequest.createRoot(request, rootRequestUri.getDiscoveryPath(), span);
            } else {
                UriRequest parent = getRootRequest();
                uriRequest = UriRequest.create(request, parent, span);
            }
            add(uriRequest);
        });
    }

    void onLoadingFinished(NetworkDomain.LoadingFinished f) {
        resolveCurrentUriRequest(f.requestId)
                .ifPresent(request -> {
                    LOG.debug("Loading finished. rId{}, size: {}, chunksSize: {}", f.requestId, f.encodedDataLength, request.getSize());
                    request.finish(crawlLogRegistry);
                })
                .otherwise(() -> LOG.error("Could not find request for finished id {}.", f.requestId));
    }

    void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
        // net::ERR_ABORTED
        resolveCurrentUriRequest(f.requestId)
                .ifPresent(request -> {
                    if (!f.canceled) {
                        LOG.error(
                                "Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Req: {}",
                                f.errorText, f.blockedReason, f.type, f.canceled, request.getUrl());
                    }

                    // Only set status code if not set from proxy already
                    if (request.getStatusCode() == 0) {
                        if ("mixed-content".equals(f.blockedReason)) {
                            request.setStatusCode(ExtraStatusCodes.BLOCKED_MIXED_CONTENT.getCode());
                        } else {
                            request.setStatusCode(ExtraStatusCodes.BLOCKED_BY_CUSTOM_PROCESSOR.getCode());
                        }
                    }

                    // TODO: Add information to pagelog

                    request.finish(crawlLogRegistry);
                })
                .otherwise(() ->
                        LOG.error("Could not find request for failed id {}. Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}",
                                f.requestId, f.errorText, f.blockedReason, f.type, f.canceled));

        LOG.debug("Loading failed. rId{}, blockedReason: {}, canceled: {}, error: {}", f.requestId, f.blockedReason, f.canceled, f);
    }

    void onResponseReceived(NetworkDomain.ResponseReceived r) {
        resolveCurrentUriRequest(r.requestId);

        LOG.debug("Response received. rId{}, size: {}, status: {}", r.requestId, r.response.encodedDataLength, r.response.status);
        allRequestsLock.lock();
        try {
            UriRequest request = getByRequestId(r.requestId);
            if (request == null) {
                LOG.error(
                        "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                        r.requestId, r.loaderId, r.timestamp, r.type, r.response, r.frameId);
                LOG.trace("Registry state:\n" + toString("  "));
                crawlLogRegistry.signalActivity();
            } else {
                request.addResponse(r);
                crawlLogRegistry.signalRequestsUpdated();
            }
        } finally {
            allRequestsLock.unlock();
        }
    }

    void onDataReceived(NetworkDomain.DataReceived d) {
        resolveCurrentUriRequest(d.requestId).ifPresent(r -> r.incrementSize(d.dataLength));
        LOG.trace("Data received. rId{}, encodedDataLength: {}, dataLength: {}", d.requestId, d.encodedDataLength, d.dataLength);
        crawlLogRegistry.signalActivity();
    }

    @Override
    public synchronized void close() {
        // Ensure that potentially unfinished spans are finshed
        allRequests.forEach(r -> r.finish(crawlLogRegistry));
    }

    public void printAllRequests() {
        System.out.println(toString());
    }

    public String toString() {
        return toString("");
    }

    public String toString(String indent) {
        return initialRequest.toString(indent);
    }

    public Optional<UriRequest> resolveCurrentUriRequest(String requestId) {
        MDC.put("eid", executionId);
        UriRequest request = getByRequestId(requestId);
        if (request == null) {
            MDC.remove("uri");
            return optional(null);
        } else {
            MDC.put("uri", request.getUrl());
            return optional(request);
        }
    }

    public static <T> Optional<T> optional(T optional) {
        return ifPresent -> otherwise -> {
            if (optional != null) {
                ifPresent.accept(optional);
            } else {
                otherwise.run();
            }
        };
    }

    public interface Otherwise {
        void otherwise(Runnable action);
    }

    public interface Optional<T> {
        Otherwise ifPresent(Consumer<T> consumer);
    }

}
