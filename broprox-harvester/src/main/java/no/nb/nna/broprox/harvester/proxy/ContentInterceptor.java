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

import io.netty.buffer.ByteBuf;

/**
 *
 */
public class ContentInterceptor {

    private final MessageDigest digest;

    private long size = 0L;

    public ContentInterceptor() {
        try {
            this.digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(ByteBuf buf) {
        if (buf.readableBytes() > 0) {
            byte[] b = new byte[1024 * 16];
            int idx = buf.readerIndex();
            while (idx < buf.writerIndex()) {
                int len = Math.min(b.length, buf.writerIndex() - idx);
                buf.getBytes(idx, b, 0, len);
                digest.update(b, 0, len);
                idx += len;
                size += len;
            }
        }
    }

    public String getDigest() {
        return "sha1:" + new BigInteger(1, digest.digest()).toString(16);
    }

    public long getSize() {
        return size;
    }

}
