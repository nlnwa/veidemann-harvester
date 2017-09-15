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
package no.nb.nna.broprox.contentwriter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import com.google.protobuf.ByteString;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
public class ContentBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(ContentBuffer.class);

    static final byte[] CRLF = {CR, LF};

    private final MessageDigest blockDigest;

    private final MessageDigest payloadDigest;

    private MessageDigest headerDigest;

    private final DbAdapter db;

    private ByteString headerBuf;

    private ByteString payloadBuf;

    public ContentBuffer(final DbAdapter db) {
        this.db = db;

        try {
            this.blockDigest = MessageDigest.getInstance("SHA-1");
            this.payloadDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setHeader(ByteString header) {
        this.headerBuf = header;
        updateDigest(headerBuf, blockDigest);

        // Get the partial result after creating a digest of the headers
        try {
            headerDigest = (MessageDigest) blockDigest.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException("Couldn't make digest of partial content");
        }
    }

    public void addPayload(ByteString payload) {
        if (payloadBuf == null) {
            payloadBuf = payload;
            // Add the payload separator to the digest
            blockDigest.update(CRLF);
        } else {
            payloadBuf = payloadBuf.concat(payload);
        }
        updateDigest(payload, blockDigest, payloadDigest);
    }

    private void updateDigest(ByteString buf, MessageDigest... digests) {
        for (MessageDigest d : digests) {
            for (ByteBuffer b : buf.asReadOnlyByteBufferList()) {
                d.update(b);
            }
        }
    }

    public String getBlockDigest() {
        return "sha1:" + new BigInteger(1, blockDigest.digest()).toString(16);
    }

    public String getPayloadDigest() {
        return "sha1:" + new BigInteger(1, payloadDigest.digest()).toString(16);
    }

    public String getHeaderDigest() {
        return "sha1:" + new BigInteger(1, headerDigest.digest()).toString(16);
    }

    public long getPayloadSize() {
        return payloadBuf == null ? 0 : payloadBuf.size();
    }

    public long getHeaderSize() {
        return headerBuf == null ? 0 : headerBuf.size();
    }

    public long getTotalSize() {
        return getHeaderSize() + (getPayloadSize() == 0L ? 0L : 2L + getPayloadSize());
    }

    public void writeRequest(CrawlLog logEntry) {
            CrawlLog.Builder logEntryBuilder = logEntry.toBuilder();
            String payloadDigestString = getPayloadDigest();
            if (getPayloadSize() == 0L) {
                logEntryBuilder.setRecordType("request")
                        .setBlockDigest(getHeaderDigest())
                        .setSize(getTotalSize());
            } else {
                logEntryBuilder.setRecordType("request")
                        .setBlockDigest(getBlockDigest())
                        .setPayloadDigest(payloadDigestString)
                        .setSize(getTotalSize());
            }
            logEntry = db.addCrawlLog(logEntryBuilder.build());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing request {}", logEntryBuilder.getRequestedUri());
            }
    }

    public void writeResponse(CrawlLog logEntry) {
            // Storing the logEntry fills in WARC-ID
            logEntry = db.addCrawlLog(logEntry);

            CrawlLog.Builder logEntryBuilder = logEntry.toBuilder();
            String payloadDigestString = getPayloadDigest();
            logEntryBuilder.setFetchTimeMs(Duration.between(ProtoUtils.tsToOdt(
                    logEntryBuilder.getFetchTimeStamp()), OffsetDateTime.now(ZoneOffset.UTC)).toMillis());

            Optional<CrawledContent> isDuplicate = db.hasCrawledContent(CrawledContent.newBuilder()
                    .setDigest(payloadDigestString)
                    .setWarcId(logEntry.getWarcId())
                    .build());

            if (isDuplicate.isPresent()) {
                logEntryBuilder.setRecordType("revisit")
                        .setBlockDigest(getHeaderDigest())
                        .setSize(getHeaderSize())
                        .setWarcRefersTo(isDuplicate.get().getWarcId());

                logEntry = db.updateCrawlLog(logEntryBuilder.build());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Writing {} as a revisit of {}",
                            logEntryBuilder.getRequestedUri(), logEntryBuilder.getWarcRefersTo());
                }
            } else {
                logEntryBuilder.setRecordType("response")
                        .setBlockDigest(getBlockDigest())
                        .setPayloadDigest(payloadDigestString)
                        .setSize(getTotalSize());

                logEntry = db.updateCrawlLog(logEntryBuilder.build());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Writing {}", logEntryBuilder.getRequestedUri());
                }
            }
    }

    public ByteString getHeader() {
        return headerBuf;
    }

    public ByteString getPayload() {
        return payloadBuf;
    }

    public void removeHeader() {
        payloadBuf = null;
    }

    public void removePayload() {
        payloadBuf = null;
    }

    public boolean hasHeader() {
        return headerBuf != null;
    }

    public boolean hasPayload() {
        return payloadBuf != null;
    }
}
