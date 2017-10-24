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
package no.nb.nna.broprox.harvester.proxy;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import no.nb.nna.broprox.commons.AlreadyCrawledCache;
import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.commons.client.ContentWriterClient;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
public class ContentCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ContentCollector.class);

    static final byte[] CRLF = {CR, LF};

    private final MessageDigest digest;

    private final DbAdapter db;

    private final ContentWriterClient.ContentWriterSession contentWriterClient;

    private long size;

    private boolean shouldAddSeparator = false;

    private boolean shouldCache = false;

    private HttpResponseStatus httpResponseStatus;

    private HttpVersion httpResponseProtocolVersion;

    private HttpHeaders cacheHeaders;

    private ByteString cacheValue;

    public ContentCollector(final DbAdapter db, final ContentWriterClient contentWriterClient) {
        this.db = db;
        this.contentWriterClient = contentWriterClient.createSession();

        try {
            this.digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setRequestHeaders(HttpRequest request) {
        StringBuilder headers = new StringBuilder(512)
                .append(request.method().toString())
                .append(" ")
                .append(request.uri())
                .append(" ")
                .append(request.protocolVersion().text())
                .append(CRLF);

        addHeaders(request.headers(), headers);

        ByteString data = ByteString.copyFromUtf8(headers.toString());
        digest.update(data.asReadOnlyByteBuffer());
        contentWriterClient.sendHeader(data);
        shouldAddSeparator = true;
        size = data.size();
    }

    public void setResponseHeaders(HttpResponse response, CrawlLog.Builder crawlLog) {
        httpResponseStatus = response.status();
        httpResponseProtocolVersion = response.protocolVersion();

        crawlLog.setStatusCode(httpResponseStatus.code())
                .setContentType(response.headers().get("Content-Type", ""));

        StringBuilder headers = new StringBuilder(512)
                .append(response.protocolVersion().text())
                .append(" ")
                .append(response.status().toString())
                .append(CRLF);

        addHeaders(response.headers(), headers);

        ByteString data = ByteString.copyFromUtf8(headers.toString());
        digest.update(data.asReadOnlyByteBuffer());
        contentWriterClient.sendHeader(data);
        shouldAddSeparator = true;
        size = data.size();

        if (shouldCache) {
            cacheHeaders = response.headers();
        }
    }

    private void addHeaders(HttpHeaders headers, StringBuilder buf) {
        Iterator<Map.Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
        while (iter.hasNext()) {
            Map.Entry<CharSequence, CharSequence> header = iter.next();
            buf.append(header.getKey())
                    .append(": ")
                    .append(header.getValue())
                    .append(CRLF);
        }
    }

    public void addPayload(ByteBuf payload) {
        if (shouldAddSeparator) {
            digest.update(CRLF);
            size += 2;
            shouldAddSeparator = false;
        }
        ByteString data = ByteString.copyFrom(payload.nioBuffer());
        digest.update(data.asReadOnlyByteBuffer());
        contentWriterClient.sendPayload(data);
        size += data.size();

        if (shouldCache) {
            if (cacheValue == null) {
                cacheValue = data;
            } else {
                cacheValue = cacheValue.concat(data);
            }
        }
    }

    public String getDigest() {
        return "sha1:" + new BigInteger(1, digest.digest()).toString(16);
    }

    public long getSize() {
        return size;
    }

    public void setShouldCache(boolean shouldCache) {
        this.shouldCache = shouldCache;
    }

    public boolean isShouldCache() {
        return shouldCache;
    }

    public ByteString getCacheValue() {
        return cacheValue;
    }

    public void writeCache(AlreadyCrawledCache cache, String uri, String executionId) {
        if (shouldCache) {
            cacheHeaders.set("Content-Length", getCacheValue().size());
            cacheHeaders.remove("Transfer-Encoding");
            LOG.trace("Cached headers: {}", getCacheValue().size(), cacheHeaders.entries());

            cache.put(httpResponseProtocolVersion,
                    httpResponseStatus,
                    uri,
                    executionId,
                    cacheHeaders,
                    getCacheValue());
        }
    }

    public void writeRequest(CrawlLog logEntry) {
        CrawlLog.Builder logEntryBuilder = logEntry.toBuilder();
        logEntryBuilder.setRecordType("request")
                .setBlockDigest(getDigest());
        logEntry = db.saveCrawlLog(logEntryBuilder.build());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing request {}", logEntryBuilder.getRequestedUri());
        }
        contentWriterClient.sendCrawlLog(logEntry);
        try {
            contentWriterClient.finish();
        } catch (InterruptedException | StatusException ex) {
            // TODO: Do something reasonable with the exception
            throw new RuntimeException(ex);
        }
    }

    public void writeResponse(CrawlLog logEntry) {
        CrawlLog.Builder logEntryBuilder = logEntry.toBuilder();
        logEntryBuilder.setRecordType("response")
                .setFetchTimeMs(Duration.between(ProtoUtils.tsToOdt(
                        logEntryBuilder.getFetchTimeStamp()), ProtoUtils.getNowOdt()).toMillis())
                .setBlockDigest(getDigest());
        logEntry = db.saveCrawlLog(logEntryBuilder.build());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing response {}", logEntryBuilder.getRequestedUri());
        }
        contentWriterClient.sendCrawlLog(logEntry);
        try {
            contentWriterClient.finish();
        } catch (InterruptedException | StatusException ex) {
            // TODO: Do something reasonable with the exception
            throw new RuntimeException(ex);
        }
    }

}
