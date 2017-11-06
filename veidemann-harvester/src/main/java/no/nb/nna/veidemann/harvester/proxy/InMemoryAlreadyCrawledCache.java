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
package no.nb.nna.veidemann.harvester.proxy;

import no.nb.nna.veidemann.commons.AlreadyCrawledCache;

import java.util.Objects;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.CacheEntry;
import org.cache2k.expiry.ExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class InMemoryAlreadyCrawledCache implements AlreadyCrawledCache {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryAlreadyCrawledCache.class);

    private final Cache<CacheKey, FullHttpResponse> cache;

    public InMemoryAlreadyCrawledCache() {
        cache = new Cache2kBuilder<CacheKey, FullHttpResponse>() {
        }
                .name("embedsCache")
                .entryCapacity(10000)
                .expiryPolicy(new ExpiryPolicy<CacheKey, FullHttpResponse>() {
                    @Override
                    public long calculateExpiryTime(CacheKey key, FullHttpResponse value,
                            long loadTime, CacheEntry<CacheKey, FullHttpResponse> oldEntry) {
                        if (value == null || value.content().readableBytes() > 256 * 1024) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Won't cache {} content too big", key);
                            }
                            return NO_CACHE;
                        }
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Caching {}", key);
                        }
                        return ETERNAL;
                    }

                })
                .build();
    }

    @Override
    public FullHttpResponse get(String uri, String executionId) {
        if (executionId == null || VeidemannHeaderConstants.MANUAL_EXID.equals(executionId)) {
            return null;
        }

        CacheKey key = new CacheKey(uri, executionId);
        FullHttpResponse cacheValue = cache.peek(key);

        if (cacheValue != null) {
            return cacheValue.retainedDuplicate();
        } else {
            return null;
        }
    }

    @Override
    public void put(HttpVersion httpVersion,
            HttpResponseStatus status,
            String uri,
            String executionId,
            HttpHeaders headers,
            ByteString cacheValue) {

        if (executionId == null) {
            return;
        }

        ByteBuf data = Unpooled.wrappedBuffer(cacheValue.toByteArray());
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(httpVersion, status, data);
        httpResponse.headers().set(headers);

        CacheKey key = new CacheKey(uri, executionId);
        cache.put(key, httpResponse);
    }

    @Override
    public void cleanExecution(String executionId) {
        for (CacheKey key : cache.keys()) {
            if (key.executionId.equals(executionId)) {
                FullHttpResponse removed = cache.peekAndRemove(key);
                if (removed != null) {
                    removed.release();
                }
            }
        }
    }

    @Override
    public void close() {
        cache.close();
    }

    public static final class CacheKey {

        private final String uri;

        private final String executionId;

        public CacheKey(final String uri, final String executionId) {
            this.uri = uri;
            this.executionId = executionId;
        }

        public String getUri() {
            return uri;
        }

        public String getExecutionId() {
            return executionId;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 71 * hash + Objects.hashCode(this.uri);
            hash = 71 * hash + Objects.hashCode(this.executionId);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final CacheKey other = (CacheKey) obj;
            if (!Objects.equals(this.uri, other.uri)) {
                return false;
            }
            if (!Objects.equals(this.executionId, other.executionId)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "CacheKey{" + "uri=" + uri + ", executionId=" + executionId + '}';
        }

    }
}
