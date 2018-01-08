package no.nb.nna.veidemann.harvester.proxy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCounted;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ChunkedCacheContentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkedCacheContentWriter.class);

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    private final String executionId;
    private final String uri;
    private final ChannelHandlerContext ctx;
    private final ByteBuf content;

    final int maxChunkSize = 1024 * 63;
    final long maxWriteTime = 20000;

    long start;

    int offset;
    final int end;
    int chunkNum = 0;
    final CompletableFuture<HttpResponse> result = new CompletableFuture<>();
    final HttpResponse head;
    boolean done = false;
    Lock lock = new ReentrantLock();
    Condition newObject = lock.newCondition();
    Condition readyToWrite = lock.newCondition();
    HttpObject objectToWrite;

    public ChunkedCacheContentWriter(final String executionId,
                                     final String uri,
                                     final ChannelHandlerContext ctx, FullHttpResponse cachedResponse) {

        this.executionId = executionId;
        this.uri = uri;
        this.ctx = ctx;

        content = cachedResponse.content();
        offset = content.readerIndex();
        end = content.writerIndex();
        head = ProxyUtils.duplicateHttpResponse(cachedResponse);
        head.headers()
                .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED)
                .remove(HttpHeaderNames.CONTENT_LENGTH);
    }

    public CompletableFuture<HttpResponse> sendResponse() {
        start = System.currentTimeMillis();

        EXECUTOR.submit(new Runnable() {
            @Override
            public void run() {
                MDC.put("eid", executionId);
                MDC.put("uri", uri);
                writeObject(head);
                LOG.trace("Header written");

                HttpObject nextObject = getNextChunk();
                while (nextObject != null) {
                    writeObject(nextObject);
                    nextObject = getNextChunk();
                }
                LOG.trace("Done writing chunks from cache");

                writeObject(new DefaultLastHttpContent());
                done = true;

                LOG.debug("Done writing from cache");
            }
        });

        write();
        result.complete(head);
        return result;
    }

    private void writeObject(HttpObject object) {
        lock.lock();
        try {
            objectToWrite = object;
            newObject.signalAll();
            while (!done && objectToWrite != null) {
                readyToWrite.await(1, TimeUnit.SECONDS);
                if ((System.currentTimeMillis() - start) > maxWriteTime) {
                    LOG.error("Timeout");
                    done = true;
                    return;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private HttpObject getNextChunk() {
        if (end > offset) {
            int chunkSize = Math.min(maxChunkSize, end - offset);
            HttpContent chunk = new DefaultHttpContent(content.copy(offset, chunkSize));
            LOG.trace("Created chunk: #{}, chunk size: {}, offset: {}", chunkNum, chunkSize, offset);
            offset += chunkSize;
            chunkNum++;
            return chunk;
        } else {
            return null;
        }
    }

    private void write() {
        lock.lock();
        try {
            while (true) {
                newObject.await(1, TimeUnit.SECONDS);
                if (done) {
                    return;
                }

                if (objectToWrite != null) {
//                    if (objectToWrite instanceof ReferenceCounted) {
//                        LOG.trace("Retaining reference counted message");
//                        ((ReferenceCounted) objectToWrite).retain();
//                    }

                    if (ctx.channel().isActive()) {
                        LOG.trace("Channel bytes before unwriteable: {}", ctx.channel().bytesBeforeUnwritable());
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        while (ctx.channel().bytesBeforeUnwritable() < 1024 * 64) {
                            LOG.debug("Channel not ready, waiting 10ms. Bytes before unwriteable: {}", ctx.channel().bytesBeforeUnwritable());
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            ctx.channel().flush();
                        }
                        LOG.trace("Writing....");
                        HttpObject next = objectToWrite;
                        ctx.channel().writeAndFlush(next).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) {
                                if (future.isSuccess()) {
                                    LOG.trace("Write success");
                                    objectToWrite = null;
                                    readyToWrite.signalAll();
                                    // was able to flush out data, start to read the next chunk
                                } else {
                                    LOG.error("Writing chunk failed, closing channel", future.cause());
                                    future.channel().close();
                                    done = true;
                                    readyToWrite.signalAll();
                                }
                            }
                        });
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
