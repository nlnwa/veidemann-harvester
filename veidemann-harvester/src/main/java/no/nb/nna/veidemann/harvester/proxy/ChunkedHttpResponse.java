/*
 * Copyright 2018 National Library of Norway.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ChunkedHttpResponse implements ChunkedInput<HttpObject>, HttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkedHttpResponse.class);

    private final String executionId;
    private final String uri;
    private final HttpResponse head;
    private final ByteBuf content;
    private final long length;
    private final int maxChunkSize = 1024 * 64;
    private int offset;
    private final int end;
    private int chunkNum = 0;
    private boolean sentHeaders;
    private boolean sentLastChunk;

    public ChunkedHttpResponse(final String executionId, final String uri, final FullHttpResponse cachedResponse) {
        this.executionId = executionId;
        this.uri = uri;
        this.content = cachedResponse.content();
        this.length = content.readableBytes();
        this.offset = content.readerIndex();
        this.end = content.writerIndex();

        head = ProxyUtils.duplicateHttpResponse(cachedResponse);
        head.headers()
                .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED)
                .remove(HttpHeaderNames.CONTENT_LENGTH);
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return sentLastChunk;
    }

    @Override
    public void close() throws Exception {
        ReferenceCountUtil.release(content);
    }

    @Override
    public HttpObject readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }

    @Override
    public HttpObject readChunk(ByteBufAllocator allocator) throws Exception {
        MDC.put("eid", executionId);
        MDC.put("uri", uri);

        if (!sentHeaders) {
            sentHeaders = true;
            LOG.trace("Sending header");
            return head;
        }

        if (end <= offset) {
            if (sentLastChunk) {
                return null;
            } else {
                // Send last chunk for this input
                sentLastChunk = true;
                LOG.trace("Sending empty last content");
                return LastHttpContent.EMPTY_LAST_CONTENT;
            }
        } else {
            int chunkSize = Math.min(maxChunkSize, end - offset);
            ByteBuf chunk = content.copy(offset, chunkSize);
            LOG.trace("Created chunk: #{}, chunk size: {}, offset: {}", chunkNum, chunkSize, offset);
            offset += chunkSize;
            chunkNum++;
            return new DefaultHttpContent(chunk);
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long progress() {
        return offset;
    }

    @Override
    public HttpResponseStatus getStatus() {
        return head.status();
    }

    @Override
    public HttpResponseStatus status() {
        return head.status();
    }

    @Override
    public HttpResponse setStatus(HttpResponseStatus status) {
        return head.setStatus(status);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return head.protocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return head.protocolVersion();
    }

    @Override
    public HttpResponse setProtocolVersion(HttpVersion version) {
        return head.setProtocolVersion(version);
    }

    @Override
    public HttpHeaders headers() {
        return head.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return head.decoderResult();
    }

    @Override
    public DecoderResult decoderResult() {
        return head.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        head.setDecoderResult(result);
    }

}
