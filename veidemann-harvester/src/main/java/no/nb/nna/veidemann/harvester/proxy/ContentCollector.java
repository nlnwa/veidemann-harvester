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

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import no.nb.nna.veidemann.api.ContentWriterProto;
import no.nb.nna.veidemann.commons.AlreadyCrawledCache;
import no.nb.nna.veidemann.commons.client.ContentWriterClient.ContentWriterSession;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.util.Sha1Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
public class ContentCollector {
    public static final String RECORD_CONTENT_TYPE_REQUEST = "application/http; msgtype=request";

    public static final String RECORD_CONTENT_TYPE_RESPONSE = "application/http; msgtype=response";

    private static final Logger LOG = LoggerFactory.getLogger(ContentCollector.class);

    static final char[] CRLF = {CR, LF};

    private final Sha1Digest digest;

    private final DbAdapter db;

    private final int recordNum;

    private final ContentWriterProto.RecordType type;

    private final String targetUri;

    private final ContentWriterProto.WriteRequestMeta.RecordMeta.Builder recordMeta;

    private long size;

    private boolean shouldAddSeparator = false;

    private boolean shouldCache = false;

    private HttpHeaders cacheHeaders;

    private ByteString cacheValue;

    public ContentCollector(int recordNum, ContentWriterProto.RecordType type, final String targetUri, final DbAdapter db) {
        this.recordNum = recordNum;
        this.type = type;
        this.targetUri = targetUri;
        this.db = db;
        this.digest = new Sha1Digest();
        this.recordMeta = ContentWriterProto.WriteRequestMeta.RecordMeta.newBuilder()
                .setRecordNum(recordNum)
                .setType(type);
        switch (type) {
            case REQUEST:
                this.recordMeta.setRecordContentType(RECORD_CONTENT_TYPE_REQUEST);
                break;
            case RESPONSE:
                this.recordMeta.setRecordContentType(RECORD_CONTENT_TYPE_RESPONSE);
                break;
        }
    }

    public static String createRequestPreamble(HttpRequest request) {
        return new StringBuilder()
                .append(request.method().toString())
                .append(" ")
                .append(request.uri())
                .append(" ")
                .append(request.protocolVersion().text())
                .append(CRLF).toString();
    }

    public static String createResponsePreamble(HttpResponse response) {
        return new StringBuilder()
                .append(response.protocolVersion().text())
                .append(" ")
                .append(response.status().toString())
                .append(CRLF).toString();
    }

    public void setHeaders(String preamble, HttpHeaders headers, ContentWriterSession contentWriterSession) {
        recordMeta.setPayloadContentType(headers.get("Content-Type", ""));
        ByteString headerData = ByteString.copyFromUtf8(preamble).concat(serializeHeaders(headers));

        digest.update(headerData);
        contentWriterSession.sendHeader(ContentWriterProto.Data.newBuilder().setRecordNum(recordNum).setData(headerData).build());
        shouldAddSeparator = true;
        size = headerData.size();

        if (shouldCache) {
            cacheHeaders = headers;
        }
    }

    private ByteString serializeHeaders(HttpHeaders headers) {
        StringBuilder buf = new StringBuilder();
        Iterator<Map.Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
        while (iter.hasNext()) {
            Map.Entry<CharSequence, CharSequence> header = iter.next();
            buf.append(header.getKey())
                    .append(": ")
                    .append(header.getValue())
                    .append(CRLF);
        }
        return ByteString.copyFromUtf8(buf.toString());
    }

    public void addPayload(ByteBuf payload, ContentWriterSession contentWriterSession) {
        if (shouldAddSeparator) {
            digest.update(CRLF);
            size += 2;
            shouldAddSeparator = false;
        }

        if (payload.readableBytes() == 0) {
            ByteString data = ByteString.copyFrom(payload.nioBuffer());
            digest.update(data);
            contentWriterSession.sendPayload(ContentWriterProto.Data.newBuilder().setRecordNum(recordNum).setData(data).build());
            size += data.size();
        } else {
            ByteBuffer payloadBuf = payload.nioBuffer();
            while (payloadBuf.hasRemaining()) {
                int length = Math.min(32 * 1024, payloadBuf.remaining());
                ByteString data = ByteString.copyFrom(payloadBuf, length);
                digest.update(data);
                contentWriterSession.sendPayload(ContentWriterProto.Data.newBuilder().setRecordNum(recordNum).setData(data).build());
                size += data.size();

                if (shouldCache) {
                    if (cacheValue == null) {
                        cacheValue = data;
                    } else {
                        cacheValue = cacheValue.concat(data);
                    }
                }
            }
        }
    }

    public String getDigest() {
        return digest.getPrefixedDigestString();
    }

    public long getSize() {
        return size;
    }

    public int getRecordNum() {
        return recordNum;
    }

    public ContentWriterProto.WriteRequestMeta.RecordMeta getRecordMeta() {
        return recordMeta
                .setBlockDigest(digest.getPrefixedDigestString())
                .setSize(size)
                .build();
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

    public void writeCache(AlreadyCrawledCache cache, String uri, String executionId,
                           HttpResponseStatus httpResponseStatus, HttpVersion httpResponseProtocolVersion) {
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

}
