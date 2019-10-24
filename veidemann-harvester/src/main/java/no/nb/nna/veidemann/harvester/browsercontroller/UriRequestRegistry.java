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
import no.nb.nna.veidemann.api.frontier.v1.PageLog.Resource;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.chrome.client.NetworkDomain;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
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

    public Stream<UriRequest> getRequestStream() {
        Stream.Builder<UriRequest> streamBuilder = Stream.builder();
        buildStream(initialRequest, streamBuilder);
        return streamBuilder.build();
    }

    private void buildStream(UriRequest r, Stream.Builder<UriRequest> streamBuilder) {
        if (r == null) {
            return;
        }

        streamBuilder.add(r);
        for (UriRequest c : r.getChildren()) {
            buildStream(c, streamBuilder);
        }
    }

    public long getBytesDownloaded() {
        return getRequestStream().filter(r -> (!r.isFromCache() && r.isFromProxy())).mapToLong(r -> r.getSize()).sum();
    }

    public int getUriDownloadedCount() {
        return (int) getRequestStream().filter(r -> (!r.isFromCache() && r.isFromProxy())).count();
    }

    public Stream<Resource> getPageLogResources() {
        return getRequestStream()
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
        resolveCurrentUriRequest(request.requestId()).ifPresent(parent -> {
            LOG.debug("Request will be sent: {}", request.requestId());

            // Already got request for this id
            if (request.redirectResponse() == null) {
                LOG.error("Already got request, but no redirect");
                return;
            } else {
                LOG.debug("Redirect response: {}, url: {}, cache: {}, redirUrl: {}",
                        request.requestId(), request.request().url(), request.redirectResponse().fromDiskCache(), request.redirectResponse().url());
                UriRequest uriRequest = UriRequest.create(request, parent, span);
                add(uriRequest);
            }
        }).otherwise(() -> {
            MDC.put("uri", request.request().url());
            LOG.debug("Request will be sent: {} {} {}, priority: {}", request.requestId(), request.request().method(), request.request().url(), request.request().initialPriority());

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
        resolveCurrentUriRequest(f.requestId())
                .ifPresent(request -> {
                    LOG.debug("Loading finished. rId{}, size: {}, chunksSize: {}", f.requestId(), f.encodedDataLength(), request.getSize());
                    request.finish(crawlLogRegistry);
                })
                .otherwise(() -> LOG.error("Could not find request for finished id {}.", f.requestId()));
    }

    void onLoadingFailed(NetworkDomain.LoadingFailed f) {
        // net::ERR_EMPTY_RESPONSE
        // net::ERR_CONNECTION_TIMED_OUT
        // net::ERR_CONTENT_LENGTH_MISMATCH
        // net::ERR_TUNNEL_CONNECTION_FAILED
        // net::ERR_ABORTED
        // net::ERR_UNKNOWN_URL_SCHEME
        // net::ERR_FAILED
        // net::ERR_INCOMPLETE_CHUNKED_ENCODING
        // net::ERR_TIMED_OUT

        resolveCurrentUriRequest(f.requestId())
                .ifPresent(request -> {
                    // Only set status code if not set from proxy already
                    if (request.getStatusCode() == 0) {
                        if ("mixed-content".equals(f.blockedReason())) {
                            LOG.debug("Resource blocked due to mixed-content");
                            request.setStatusCode(ExtraStatusCodes.BLOCKED_MIXED_CONTENT.getCode());
                        } else if (f.canceled()) {
                            LOG.debug("Resource canceled by browser: Error '{}', Blocked reason '{}'", f.errorText(), f.blockedReason());
                            request.setStatusCode(ExtraStatusCodes.CANCELED_BY_BROWSER.getCode());
                        } else if ("net::ERR_UNKNOWN_URL_SCHEME".equals(f.errorText())) {
                            LOG.debug("Resource has unknown URL Scheme: Error '{}', Blocked reason '{}'", f.errorText(), f.blockedReason());
                            request.setStatusCode(ExtraStatusCodes.ILLEGAL_URI.getCode());
                        } else if ("net::ERR_TUNNEL_CONNECTION_FAILED".equals(f.errorText())) {
                            LOG.debug("Could not create tls tunnel for resource: Error '{}', Blocked reason '{}'", f.errorText(), f.blockedReason());
                            request.setStatusCode(ExtraStatusCodes.CONNECT_FAILED.getCode());
                        } else {
                            LOG.error("Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Req: {}, Req Id: {}",
                                    f.errorText(), f.blockedReason(), f.type(), f.canceled(), request.getUrl(), f.requestId());
                        }
                    } else {
                        LOG.error(
                                "Failed fetching page: Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}, Status from proxy: {}",
                                f.errorText(), f.blockedReason(), f.type(), f.canceled(), request.getStatusCode());
                    }

                    // TODO: Add information to pagelog

                    request.finish(crawlLogRegistry);
                })
                .otherwise(() ->
                        LOG.error("Could not find request for failed id {}. Error '{}', Blocked reason '{}', Resource type: '{}', Canceled: {}",
                                f.requestId(), f.errorText(), f.blockedReason(), f.type(), f.canceled()));

        LOG.debug("Loading failed. rId{}, blockedReason: {}, canceled: {}, error: {}", f.requestId(), f.blockedReason(), f.canceled(), f);
    }

    void onResponseReceived(NetworkDomain.ResponseReceived r) {
        resolveCurrentUriRequest(r.requestId());

        LOG.debug("Response received. rId{}, size: {}, status: {}, cache: {}", r.requestId(), r.response().encodedDataLength(), r.response().status(), r.response().fromDiskCache());
        allRequestsLock.lock();
        try {
            UriRequest request = getByRequestId(r.requestId());
            if (request == null) {
                LOG.error(
                        "Response received, but we missed the request: reqId '{}', loaderId '{}', ts '{}', type '{}', resp '{}', frameId '{}'",
                        r.requestId(), r.loaderId(), r.timestamp(), r.type(), r.response(), r.frameId());
                LOG.trace("Registry state:\n{}", toString("  "));
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
        resolveCurrentUriRequest(d.requestId()).ifPresent(r -> r.incrementSize(d.dataLength()));
        LOG.trace("Data received. rId{}, encodedDataLength: {}, dataLength: {}", d.requestId(), d.encodedDataLength(), d.dataLength());
        crawlLogRegistry.signalActivity();
    }

    @Override
    public synchronized void close() {
        // Ensure that potentially unfinished spans are finshed
        getRequestStream().forEach(r -> r.finish(crawlLogRegistry));
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
