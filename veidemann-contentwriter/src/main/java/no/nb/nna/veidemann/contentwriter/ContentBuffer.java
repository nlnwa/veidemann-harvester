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
package no.nb.nna.veidemann.contentwriter;

import com.google.protobuf.ByteString;
import no.nb.nna.veidemann.commons.util.Sha1Digest;
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

    private final static String EMPTY_DIGEST_STRING = "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709";

    private final Sha1Digest blockDigest;
    private final Sha1Digest payloadDigest;
    private Sha1Digest headerDigest;

    private ByteString headerBuf;
    private ByteString payloadBuf;

    private final String warcId;

    public ContentBuffer() {
        this.blockDigest = new Sha1Digest();
        this.payloadDigest = new Sha1Digest();
        this.warcId = Util.createIdentifier();
    }

    public void setHeader(ByteString header) {
        this.headerBuf = header;
        updateDigest(headerBuf, blockDigest);

        // Get the partial result after creating a digest of the headers
        headerDigest = blockDigest.clone();
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

    private void updateDigest(ByteString buf, Sha1Digest... digests) {
        for (Sha1Digest d : digests) {
            d.update(buf);
        }
    }

    public String getBlockDigest() {
        return blockDigest.getPrefixedDigestString();
    }

    public String getPayloadDigest() {
        return payloadDigest.getPrefixedDigestString();
    }

    public String getHeaderDigest() {
        if (headerDigest == null) {
            return EMPTY_DIGEST_STRING;
        }
        return headerDigest.getPrefixedDigestString();
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

    public String getWarcId() {
        return warcId;
    }

    public void close() {
        // Clean up resources
    }
}
