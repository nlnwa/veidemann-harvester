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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.AsciiString;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;
import static io.netty.util.AsciiString.c2b;

/**
 *
 */
public class ContentInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(ContentInterceptor.class);

    static final byte[] CRLF = {CR, LF};

    private final MessageDigest blockDigest;

    private final MessageDigest payloadDigest;

    private MessageDigest headerDigest;

    private long headerSize = 0L;

    private long payloadSize = 0L;

    private final ChannelHandlerContext ctx;

    private final DbAdapter db;

    private final ContentWriterClient contentWriterClient;

    private ByteBuf headerBuf;

    private CompositeByteBuf payloadBuf;

    public ContentInterceptor(final DbAdapter db, final ChannelHandlerContext ctx,
            final ContentWriterClient contentWriterClient) {
        this.db = db;
        this.ctx = ctx;
        this.contentWriterClient = contentWriterClient;

        try {
            this.blockDigest = MessageDigest.getInstance("SHA-1");
            this.payloadDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void addHeader(HttpHeaders headers) {
        headerBuf = ctx.alloc().buffer();
        Iterator<Map.Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
        while (iter.hasNext()) {
            Map.Entry<CharSequence, CharSequence> header = iter.next();
            encoderHeader(header.getKey(), header.getValue(), headerBuf);
        }
        headerSize = headerBuf.readableBytes();
        updateDigest(headerBuf, blockDigest);

        // Get the partial result after creating a digest of the headers
        try {
            headerDigest = (MessageDigest) blockDigest.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException("Couldn't make digest of partial content");
        }
    }

    public void addPayload(ByteBuf payload) {
        if (payloadBuf == null) {
            payloadBuf = ctx.alloc().compositeBuffer();
            // Add the payload separator to the digest
            blockDigest.update(CRLF);
        }

        payloadSize += payload.readableBytes();

        payloadBuf.addComponent(true, payload.slice().retain());
        updateDigest(payload, blockDigest, payloadDigest);
    }

    private void updateDigest(ByteBuf buf, MessageDigest... digests) {
        if (buf.readableBytes() > 0) {
            byte[] b = new byte[1024 * 16];
            int idx = buf.readerIndex();
            while (idx < buf.writerIndex()) {
                int len = Math.min(b.length, buf.writerIndex() - idx);
                buf.getBytes(idx, b, 0, len);
                for (MessageDigest d : digests) {
                    d.update(b, 0, len);
                }
                idx += len;
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
        return payloadSize;
    }

    public long getHeaderSize() {
        return headerSize;
    }

    public void writeData(CrawlLog.Builder logEntry) {
        String payloadDigestString = getPayloadDigest();
        logEntry.setFetchTimeMillis(Duration.between(ProtoUtils.tsToOdt(
                logEntry.getFetchTimeStamp()), OffsetDateTime.now(ZoneOffset.UTC)).toMillis());

        Optional<CrawledContent> isDuplicate = db.isDuplicateContent(payloadDigestString);

        if (isDuplicate.isPresent()) {
            logEntry.setRecordType("revisit")
                    .setBlockDigest(getHeaderDigest())
                    .setSize(headerSize)
                    .setWarcRefersTo(isDuplicate.get().getWarcId());

            CrawlLog crawlLog = db.addCrawlLog(logEntry.build());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing {} as a revisit of {}", logEntry.getRequestedUri(), logEntry.getWarcRefersTo());
            }
            contentWriterClient.writeRecord(crawlLog, headerBuf, null);
        } else {
            logEntry.setRecordType("response")
                    .setBlockDigest(getBlockDigest())
                    .setPayloadDigest(payloadDigestString)
                    .setSize(headerSize + 2 + payloadSize);

            CrawlLog crawlLog = db.addCrawlLog(logEntry.build());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing {}", logEntry.getRequestedUri());
            }
            contentWriterClient.writeRecord(crawlLog, headerBuf, payloadBuf);
        }

//        headerBuf.release();
//        payloadBuf.release();
        if (!isDuplicate.isPresent()) {
            db.addCrawledContent(CrawledContent.newBuilder()
                    .setDigest(payloadDigestString)
                    .setWarcId(logEntry.getWarcId())
                    .build());
        }
    }

    public void release() {
        headerBuf.release();
        payloadBuf.release();
    }

    public ByteBuf getHeaderBuf() {
        return headerBuf;
    }

    public CompositeByteBuf getPayloadBuf() {
        return payloadBuf;
    }

    private static void encoderHeader(CharSequence name, CharSequence value, ByteBuf buf) {
        final int nameLen = name.length();
        final int valueLen = value.length();
        final int entryLen = nameLen + valueLen + 4;
        buf.ensureWritable(entryLen);
        int offset = buf.writerIndex();
        writeAscii(buf, offset, name, nameLen);
        offset += nameLen;
        buf.setByte(offset++, ':');
        buf.setByte(offset++, ' ');
        writeAscii(buf, offset, value, valueLen);
        offset += valueLen;
        buf.setByte(offset++, '\r');
        buf.setByte(offset++, '\n');
        buf.writerIndex(offset);
    }

    private static void writeAscii(ByteBuf buf, int offset, CharSequence value, int valueLen) {
        if (value instanceof AsciiString) {
            ByteBufUtil.copy((AsciiString) value, 0, buf, offset, valueLen);
        } else {
            writeCharSequence(buf, offset, value, valueLen);
        }
    }

    private static void writeCharSequence(ByteBuf buf, int offset, CharSequence value, int valueLen) {
        for (int i = 0; i < valueLen; ++i) {
            buf.setByte(offset++, c2b(value.charAt(i)));
        }
    }

}
