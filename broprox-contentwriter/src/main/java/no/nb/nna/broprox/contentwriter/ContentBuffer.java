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

import com.google.protobuf.ByteString;
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

    private ByteString headerBuf;

    private ByteString payloadBuf;

    public ContentBuffer() {
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
            if (hasHeader()) {
                // Add the payload separator to the digest
                blockDigest.update(CRLF);
            }
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
        return getHeaderSize() + getPayloadSize() + (hasHeader() && hasPayload() ? 2L : 0L);
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
