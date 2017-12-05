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
import no.nb.nna.veidemann.chrome.client.NetworkDomain;
import no.nb.nna.veidemann.chrome.client.NetworkDomain.RequestIntercepted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private String interceptionId;

    private ResourceType resourceType;

    private String mimeType = "";

    private int statusCode;

    private String referrer;

    private String discoveryPath = null;

    private UriRequest parent;

    private boolean renderable = false;

    private long size = 0L;

    private Double responseSize = null;

    private boolean fromCache = false;

    private String warcId = "";

    private final BaseSpan parentSpan;

    private Span span;

    /**
     * True if this request is for the top level request.
     * <p>
     * It is also true if the request is a redirect from the top level request.
     */
    private boolean rootResource = false;

    private final Lock lock = new ReentrantLock();
    private final Condition noReferrer = lock.newCondition();
    private final Condition noDiscoveryPath = lock.newCondition();

    public UriRequest(RequestIntercepted request, String referrer, BaseSpan parentSpan) {
        this.interceptionId = request.interceptionId;
        this.url = request.request.url;
        if (request.isNavigationRequest) {
            this.referrer = referrer;
        }
        this.resourceType = ResourceType.forName(request.resourceType);

        if (request.isNavigationRequest) {
            this.rootResource = true;
        }

        this.parentSpan = parentSpan;
    }

    public UriRequest(UriRequest parent, RequestIntercepted request, BaseSpan parentSpan) {
        this.parent = parent;
        this.interceptionId = parent.interceptionId;
        this.url = request.redirectUrl;
        this.statusCode = request.redirectStatusCode;
        this.discoveryPath = parent.discoveryPath + "R";
        this.referrer = parent.url;
        this.resourceType = ResourceType.forName(request.resourceType);
        this.rootResource = parent.rootResource;

        this.parentSpan = parentSpan;
    }

    public void addRequest(NetworkDomain.RequestWillBeSent request) {
        if (this.discoveryPath == null && parent != null) {
            if (request.redirectResponse != null) {
                setDiscoveryPath(parent.discoveryPath + "R");
            } else if ("script".equals(request.initiator.type)) {
                // Resource is loaded by a script
                setDiscoveryPath(parent.discoveryPath + "X");
            } else {
                setDiscoveryPath(parent.discoveryPath + "E");
            }
        }
    }

    void addRedirectResponse(NetworkDomain.Response response) {
        mimeType = response.mimeType;
        statusCode = response.status.intValue();
        this.responseSize = response.encodedDataLength;
    }

    void addResponse(NetworkDomain.ResponseReceived response) {
        if (this.responseSize != null) {
            LOG.debug("Already got response, previous length: {}, new length: {}, Referrer: {}, DiscoveryPath: {}",
                    this.responseSize, response.response.encodedDataLength, referrer, discoveryPath);
            return;
        }

        this.responseSize = response.response.encodedDataLength;
        resourceType = ResourceType.forName(response.type);
        mimeType = response.response.mimeType;
        statusCode = response.response.status.intValue();

        renderable = MimeTypes.forType(mimeType)
                .filter(m -> m.resourceType.category == ResourceType.Category.Document)
                .isPresent();

//        if (!mimeTypeIsConsistentWithType(this)) {
//            LOG.error("Resource interpreted as {} but transferred with MIME type {}: \"{}\".", resourceType.title,
//                    mimeType, getUrl());
//        }
    }

    public String getInterceptionId() {
        return interceptionId;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getUrl() {
        return url;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getDiscoveryPath() {
        lock.lock();
        try {
            if (discoveryPath == null) {
                noDiscoveryPath.await(2, TimeUnit.SECONDS);
            }
            if (discoveryPath == null) {
                LOG.error("Bowser event for updating discovery path was not fired within the time limits");
                throw new IllegalStateException("Bowser event for updating discovery path was not fired within the time limits");
            }
            return discoveryPath;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void setDiscoveryPath(String discoveryPath) {
        lock.lock();
        try {
            this.discoveryPath = discoveryPath;
            noDiscoveryPath.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public String getReferrer() {
        lock.lock();
        try {
            if (referrer == null) {
                noReferrer.await(2, TimeUnit.SECONDS);
            }
            if (referrer == null) {
                LOG.error("Bowser event for updating referrer was not fired within the time limits");
                throw new IllegalStateException("Bowser event for updating referrer was not fired within the time limits");
            }
            return referrer;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void setReferrer(String referrer) {
        lock.lock();
        try {
            this.referrer = referrer;
            noReferrer.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public UriRequest getParent() {
        return parent;
    }

    public void setParent(UriRequest parent) {
        setReferrer(parent.url);
        this.parent = parent;
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

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isFromCache() {
        return fromCache;
    }

    public void setFromCache(boolean fromCache) {
        this.fromCache = fromCache;
    }

    public String getWarcId() {
        return warcId;
    }

    public void setWarcId(String warcId) {
        this.warcId = warcId;
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
    }

    public void finish() {
        span.finish();;
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

}
