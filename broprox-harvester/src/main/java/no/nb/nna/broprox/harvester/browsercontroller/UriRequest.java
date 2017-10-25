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

import io.opentracing.BaseSpan;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.chrome.client.NetworkDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class UriRequest {

    private static final Logger LOG = LoggerFactory.getLogger(UriRequest.class);

    NetworkDomain.RequestWillBeSent request;

    NetworkDomain.ResponseReceived response;

    private ResourceType resourceType;

    private String mimeType = "";

    private int statusCode;

    private String discoveryPath = "";

    private UriRequest parent;

    private boolean renderable = false;

    private long size = 0L;

    private boolean fromCache = false;

    private String warcId = "";

    private Span span;

    /**
     * True if this request is for the top level request.
     * <p>
     * It is also true if the request is a redirect from the top level request.
     */
    private boolean rootResource = false;

    private UriRequest(NetworkDomain.RequestWillBeSent request, BaseSpan parentSpan) {
        this.request = request;
        this.resourceType = ResourceType.forName(request.type);
        LOG.debug("RequestId: {}, documentUrl: {}, URL: {}, initiator: {}, redirectResponse: {}, referer: {}",
                request.requestId, request.documentURL, request.request.url, request.initiator,
                request.redirectResponse, request.request.headers.get("Referer"));

        span = buildSpan(parentSpan, request.request.url);
    }

    public UriRequest(NetworkDomain.RequestWillBeSent request, String initialDiscoveryPath,
            BaseSpan parentSpan) {

        this(request, parentSpan);
        this.discoveryPath = initialDiscoveryPath;
        this.rootResource = true;
    }

    public UriRequest(NetworkDomain.RequestWillBeSent request, UriRequest parent,
            BaseSpan parentSpan) {

        this(request, parentSpan);
        this.parent = parent;
        if (request.redirectResponse != null) {
            this.rootResource = parent.rootResource;
            this.statusCode = request.redirectResponse.status.intValue();
            this.discoveryPath = parent.discoveryPath + "R";
        } else if ("script".equals(request.initiator.type)) {
            // Resource is loaded by a script
            this.discoveryPath = parent.discoveryPath + "X";
        } else {
            this.discoveryPath = parent.discoveryPath + "E";
        }
    }

    public String getRequestId() {
        return request.requestId;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getUrl() {
        return request.request.url;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getDiscoveryPath() {
        return discoveryPath;
    }

    public UriRequest getParent() {
        return parent;
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

    public Span getSpan() {
        return span;
    }

    void addResponse(NetworkDomain.ResponseReceived response) {
        if (this.response != null) {
            LOG.error("Already got response, previous length: {}, new length: {}", this.response.response.encodedDataLength, response.response.encodedDataLength);
        }

        this.response = response;
        if (!request.request.url.equals(response.response.url)) {
            LOG.warn("URL {} - {}", request.request.url, response.response.url);
        }

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
