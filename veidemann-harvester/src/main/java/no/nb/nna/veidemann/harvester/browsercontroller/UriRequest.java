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
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.chrome.client.NetworkDomain;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class UriRequest {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequest.class);

    private String url;

    private String requestId;

    private ResourceType resourceType;

    private String mimeType = "";

    private int statusCode;

    private String referrer;

    private String discoveryPath = null;

    private UriRequest parent;

    private List<UriRequest> children = new ArrayList<>();

    private boolean renderable = false;

    private long size = 0L;

    private long responseSize = 0L;

    private boolean fromCache = false;

    /**
     * Is this request's response from the proxy.
     */
    private boolean fromProxy = true;

    private String warcId = "";

    private final BaseSpan parentSpan;

    private Span span;

    private boolean finished = false;

    private CrawlLog crawlLog;

    /**
     * True if this request is for the top level request.
     * <p>
     * It is also true if the request is a redirect from the top level request.
     */
    private boolean rootResource = false;

    private final Lock referrerLock = new ReentrantLock();
    private final Lock discoveryPathLock = new ReentrantLock();
    private final Condition noReferrer = referrerLock.newCondition();
    private final Condition noDiscoveryPath = discoveryPathLock.newCondition();

    private UriRequest(String requestId, String url, String referrer, ResourceType type, BaseSpan parentSpan) {
        this.requestId = requestId;
        this.url = url;
        this.referrer = referrer;
        this.resourceType = type;

        this.parentSpan = parentSpan;
    }

    private UriRequest(NetworkDomain.RequestWillBeSent request, BaseSpan parentSpan) {
        this(request.requestId(),
                request.request().url(),
                (String) request.request().headers().getOrDefault("referer", ""),
                ResourceType.forName(request.type()),
                parentSpan);

        LOG.debug("RequestId: {}, documentUrl: {}, URL: {}, initiator: {} {}, redirectResponse: {}, referer: {}",
                request.requestId(), request.documentURL(), request.request().url(), request.initiator().type(), request.initiator().url(),
                request.redirectResponse(), request.request().headers().get("Referer"));
    }

    public static UriRequest createRoot(NetworkDomain.RequestWillBeSent request, String initialDiscoveryPath, BaseSpan parentSpan) {
        UriRequest result = new UriRequest(request, parentSpan);
        result.discoveryPath = initialDiscoveryPath;
        result.rootResource = true;
        return result;
    }

    public static UriRequest createRoot(String requestId, String url, String referrer, ResourceType type, String initialDiscoveryPath, BaseSpan parentSpan) {
        UriRequest result = new UriRequest(requestId, url, referrer, type, parentSpan);
        result.discoveryPath = initialDiscoveryPath;
        result.rootResource = true;
        return result;
    }

    public static UriRequest create(String requestId, String url, String referrer, ResourceType type, char discoveryType, UriRequest parent, BaseSpan parentSpan) {
        UriRequest result = new UriRequest(requestId, url, referrer, type, parentSpan);
        result.setParent(parent);
        result.discoveryPath = parent.discoveryPath + discoveryType;
        return result;
    }

    public static UriRequest create(NetworkDomain.RequestWillBeSent request, UriRequest parent, BaseSpan parentSpan) {
        UriRequest result;
        char discoveryType;

        if (request.redirectResponse() != null) {
            discoveryType = 'R';
            parent.statusCode = request.redirectResponse().status().intValue();
            parent.setFromCache(request.redirectResponse().fromDiskCache());
            parent.setMimeType(request.redirectResponse().mimeType());
        } else {
            if ("script".equals(request.initiator().type())) {
                // Resource is loaded by a script
                discoveryType = 'X';
            } else {
                discoveryType = 'E';
            }
        }

        result = create(request.requestId(), request.request().url(), parent.getUrl(),
                ResourceType.forName(request.type()), discoveryType, parent, parentSpan);

        return result;
    }

    void addResponse(NetworkDomain.ResponseReceived response) {
        if (getMimeType() != null) {
            LOG.trace("Already got response, previous length: {}, new length: {}, Referrer: {}, DiscoveryPath: {}",
                    this.responseSize, response.response().encodedDataLength(), referrer, discoveryPath);
        }

        resourceType = ResourceType.forName(response.type());
        setMimeType(response.response().mimeType());
        statusCode = response.response().status().intValue();
        setFromCache(response.response().fromDiskCache());

        if (response.response().fromDiskCache() || response.response().protocol().equals("data")) {
            setFromProxy(false);
        }

//        if (!mimeTypeIsConsistentWithType(this)) {
//            LOG.error("Resource interpreted as {} but transferred with MIME type {}: \"{}\".", resourceType.title,
//                    mimeType, getUrl());
//        }
    }

    public String getRequestId() {
        return requestId;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
        renderable = MimeTypes.forType(mimeType)
                .filter(m -> m.resourceType.category == ResourceType.Category.Document)
                .isPresent();
    }

    public String getUrl() {
        return url;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getDiscoveryPath() {
        discoveryPathLock.lock();
        try {
            if (discoveryPath == null) {
                noDiscoveryPath.await(20, TimeUnit.SECONDS);
            }
            if (discoveryPath == null) {
                LOG.error("Browser event for updating discovery path was not fired within the time limits");
                throw new IllegalStateException("Browser event for updating discovery path was not fired within the time limits");
            }
            return discoveryPath;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            discoveryPathLock.unlock();
        }
    }

    public void setDiscoveryPath(String discoveryPath) {
        discoveryPathLock.lock();
        try {
            this.discoveryPath = discoveryPath;
            noDiscoveryPath.signalAll();
        } finally {
            discoveryPathLock.unlock();
        }
    }

    public String getReferrer() {
        referrerLock.lock();
        try {
            if (referrer == null) {
                noReferrer.await(20, TimeUnit.SECONDS);
            }
            if (referrer == null) {
                LOG.error("Browser event for updating referrer was not fired within the time limits");
                throw new IllegalStateException("Browser event for updating referrer was not fired within the time limits");
            }
            return referrer;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            referrerLock.unlock();
        }
    }

    public void setReferrer(String referrer) {
        referrerLock.lock();
        try {
            this.referrer = referrer;
            noReferrer.signalAll();
        } finally {
            referrerLock.unlock();
        }
    }

    public UriRequest getParent() {
        return parent;
    }

    public void setParent(UriRequest parent) {
        if (parent.getRequestId().equals(getRequestId())) {
            rootResource = parent.rootResource;
        } else {
            rootResource = false;
        }
        setReferrer(parent.url);
        this.parent = parent;
        parent.children.add(this);
    }

    public List<UriRequest> getChildren() {
        return children;
    }

    public boolean isRenderable() {
        return renderable;
    }

    public boolean isRootResource() {
        return rootResource;
    }

    public long getSize() {
        return size;
    }

    public void incrementSize(long size) {
        this.size += size;
    }

    public boolean isFromCache() {
        return fromCache;
    }

    public void setFromCache(boolean fromCache) {
        this.fromCache = fromCache;
    }

    public boolean isFromProxy() {
        return fromProxy;
    }

    public void setFromProxy(boolean fromProxy) {
        this.fromProxy = fromProxy;
    }

    public String getWarcId() {
        return warcId;
    }

    public void setWarcId(String warcId) {
        this.warcId = warcId;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public CrawlLog setCrawlLog(CrawlLog.Builder crawlLogBuilder, boolean isFromCache) {
        // If request fails in client to proxy step, then status code is not set and we pick it up from the error
        if (crawlLogBuilder.getStatusCode() == 0) {
            crawlLogBuilder.setStatusCode(crawlLogBuilder.getError().getCode());
        }

        this.crawlLog = crawlLogBuilder
                .setReferrer(referrer)
                .setDiscoveryPath(discoveryPath)
                .build();
        this.warcId = crawlLog.getWarcId();
        this.size = crawlLog.getSize();
        setFromCache(isFromCache);
        if (this.crawlLog.getStatusCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
            setFromProxy(false);
        }
        return this.crawlLog;
    }

    public CrawlLog getCrawlLog() {
        return crawlLog;
    }

    private static boolean mimeTypeIsConsistentWithType(UriRequest request) {
        // If status is an error, content is likely to be of an inconsistent type,
        // as it's going to be an error message. We do not want to emit a warning
        // for this, though, as this will already be reported as resource loading failure.
        // Also, if a URL like http://localhost/wiki/load.php?debug=true&lang=en produces text/css and gets reloaded,
        // it is 304 Not Modified and its guessed mime-type is text/php, which is wrong.
        // Don't check for mime-types in 304-resources.
        if (request.getStatusCode() >= 400 || request.getStatusCode() == 304 || request.getStatusCode() == 204) {
            return true;
        }

        ResourceType resourceType = request.getResourceType();
        if (resourceType != ResourceType.Stylesheet && resourceType != ResourceType.Document
                && resourceType != ResourceType.TextTrack) {
            return true;
        }

        if (request.getMimeType() == null) {
            return true;  // Might be not known for cached resources with null responses.
        }

        return MimeTypes.forType(request.getMimeType()).filter(m -> m.resourceType == resourceType).isPresent();
    }

    public void start() {
        span = buildSpan(parentSpan, url);
        LOG.debug("Request {} started", requestId);
    }

    public void finish(CrawlLogRegistry crawlLogRegistry) {
        if (!finished) {
            span.finish();
            LOG.debug("Request {} finished", requestId);
            finished = true;
            crawlLogRegistry.signalRequestsUpdated();
        }
    }

    public Span getSpan() {
        return span;
    }

    private Span buildSpan(BaseSpan parentSpan, String uri) {
        Tracer.SpanBuilder newSpan = GlobalTracer.get()
                .buildSpan("newUriRequest")
                .withTag(Tags.COMPONENT.getKey(), "UriRequest")
                .withTag("uri", uri);
        if (parentSpan != null) {
            newSpan.asChildOf(parentSpan);
        }
        return newSpan.startManual();
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(String indent) {
        final StringBuffer sb = new StringBuffer(indent + "- UriRequest{");
        sb.append("rId='").append(requestId).append('\'');
        sb.append(", status=").append(statusCode);
        sb.append(", path='").append(discoveryPath).append('\'');
        sb.append(", renderable=").append(renderable);
        sb.append(", url='").append(url).append('\'');
        sb.append(", referrer=").append(referrer);
        sb.append(", fromCache=").append(isFromCache());
        sb.append(", warcId=").append(warcId);
        if (crawlLog != null) {
            sb.append(", log=").append(crawlLog.getStatusCode() + "::" + crawlLog.getRequestedUri());
        }
        if (!children.isEmpty()) {
            for (UriRequest c : children) {
                sb.append("\n  ").append(indent).append(c.toString(indent + "  "));
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
