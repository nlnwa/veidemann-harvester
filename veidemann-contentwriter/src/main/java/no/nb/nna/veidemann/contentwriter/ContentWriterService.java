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

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteReply;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.contentwriter.text.TextExtracter;
import no.nb.nna.veidemann.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.veidemann.contentwriter.warc.WarcWriterPool;
import no.nb.nna.veidemann.api.MessagesProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
public class ContentWriterService extends ContentWriterGrpc.ContentWriterImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriterService.class);

    static final byte[] CRLF = {CR, LF};

    private final DbAdapter db;

    private final WarcWriterPool warcWriterPool;

    private final TextExtracter textExtracter;

    public ContentWriterService(DbAdapter db, WarcWriterPool warcWriterPool, TextExtracter textExtracter) {
        this.db = db;
        this.warcWriterPool = warcWriterPool;
        this.textExtracter = textExtracter;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new StreamObserver<WriteRequest>() {
            private final ContentBuffer contentBuffer = new ContentBuffer();

            private MessagesProto.CrawlLog.Builder crawlLog;

            @Override
            public void onNext(WriteRequest value) {
                switch (value.getValueCase()) {
                    case CRAWL_LOG:
                        crawlLog = value.getCrawlLog().toBuilder();
                        MDC.put("eid", crawlLog.getExecutionId());
                        MDC.put("uri", crawlLog.getRequestedUri());
                        break;
                    case HEADER:
                        if (contentBuffer.hasHeader()) {
                            LOG.error("Header received twice");
                            Status status = Status.INVALID_ARGUMENT.withDescription("Header received twice");
                            responseObserver.onError(status.asException());
                            break;
                        }
                        contentBuffer.setHeader(value.getHeader());
                        break;
                    case PAYLOAD:
                        contentBuffer.addPayload(value.getPayload());
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                if (status.getCode().equals(Code.CANCELLED)) {
                    if (crawlLog != null) {
                        MDC.put("uri", crawlLog.getRequestedUri());
                        MDC.put("recordType", crawlLog.getRecordType());
                    }
                    LOG.info("Request cancelled before WARC record written");
                } else {
                    LOG.error("Error caught: {}", t.getMessage(), t);
                }
            }

            @Override
            public void onCompleted() {
                if (crawlLog == null) {
                    LOG.error("Missing CrawlLog object");
                    Status status = Status.INVALID_ARGUMENT.withDescription("Missing CrawlLog object");
                    responseObserver.onError(status.asException());
                    return;
                }
                if (contentBuffer.getTotalSize() == 0L) {
                    MDC.put("uri", crawlLog.getRequestedUri());
                    LOG.error("Nothing to store");
                    Status status = Status.INVALID_ARGUMENT.withDescription("Nothing to store");
                    responseObserver.onError(status.asException());
                    return;
                }

                try {
                    detectRevisit(contentBuffer, crawlLog);

                    URI ref = writeRecord(contentBuffer, crawlLog);

                    db.saveCrawlLog(crawlLog.build());

                    responseObserver.onNext(WriteReply.newBuilder()
                            .setStorageRef(ref.toString())
                            .build());
                    responseObserver.onCompleted();
                } catch (StatusException ex) {
                    LOG.error("Failed write: {}", ex.getMessage(), ex);
                    responseObserver.onError(ex);
                } catch (Throwable ex) {
                    LOG.error("Failed write: {}", ex.getMessage(), ex);
                    responseObserver.onError(Status.fromThrowable(ex).asException());
                }
            }

        };
    }

    @Override
    public void flush(Empty request, StreamObserver<Empty> responseObserver) {
        try {
            warcWriterPool.restart(false);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error("Failed flush: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.UNKNOWN.withDescription(ex.toString()).asException());
        }
    }

    @Override
    public void delete(Empty request, StreamObserver<Empty> responseObserver) {
        try {
            if (ContentWriter.getSettings().isUnsafe()) {
                warcWriterPool.restart(true);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.PERMISSION_DENIED.withDescription("Deletion not allowed").asException());
            }
        } catch (Exception ex) {
            LOG.error("Failed delete: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.UNKNOWN.withDescription(ex.toString()).asException());
        }
    }

    private void detectRevisit(final ContentBuffer contentBuffer, final MessagesProto.CrawlLog.Builder crawlLog) {
        switch (crawlLog.getRecordType()) {
            case "":
            case "response":
                Optional<MessagesProto.CrawledContent> isDuplicate = db
                        .hasCrawledContent(MessagesProto.CrawledContent.newBuilder()
                                .setDigest(contentBuffer.getPayloadDigest())
                                .setWarcId(crawlLog.getWarcId())
                                .build());

                if (isDuplicate.isPresent()) {
                    crawlLog.setRecordType("revisit")
                            .setRecordContentType("application/http")
                            .setBlockDigest(contentBuffer.getHeaderDigest())
                            .setPayloadDigest(isDuplicate.get().getDigest())
                            .setSize(contentBuffer.getHeaderSize())
                            .setWarcRefersTo(isDuplicate.get().getWarcId());

                    contentBuffer.removePayload();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Writing {} as a revisit of {}",
                                crawlLog.getRequestedUri(), crawlLog.getWarcRefersTo());
                    }
                } else {
                    crawlLog.setRecordType("response")
                            .setBlockDigest(contentBuffer.getBlockDigest())
                            .setPayloadDigest(contentBuffer.getPayloadDigest())
                            .setSize(contentBuffer.getTotalSize());

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Writing {}", crawlLog.getRequestedUri());
                    }
                }
                break;

            default:
                crawlLog.setBlockDigest(contentBuffer.getBlockDigest())
                        .setPayloadDigest(contentBuffer.getPayloadDigest())
                        .setSize(contentBuffer.getTotalSize());
                break;
        }
    }

    private URI writeRecord(final ContentBuffer contentBuffer, final MessagesProto.CrawlLog.Builder crawlLog)
            throws StatusException {

        try (WarcWriterPool.PooledWarcWriter pooledWarcWriter = warcWriterPool.borrow()) {
            long size = 0L;

            SingleWarcWriter warcWriter = pooledWarcWriter.getWarcWriter();

            URI ref = warcWriter.writeWarcHeader(crawlLog.build());
            crawlLog.setStorageRef(ref.toString());

            if (contentBuffer.hasHeader()) {
                size += warcWriter.addPayload(contentBuffer.getHeader().newInput());
            }

            if (contentBuffer.hasPayload()) {
                // If both headers and payload are present, add separator
                if (contentBuffer.hasHeader()) {
                    size += warcWriter.addPayload(CRLF);
                }

                ForkJoinTask<Long> writeWarcJob = ForkJoinPool.commonPool().submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return warcWriter.addPayload(contentBuffer.getPayload().newInput());
                    }

                });

                ForkJoinTask<Void> extractTextJob = ForkJoinPool.commonPool().submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        textExtracter.analyze(contentBuffer.getPayload().newInput(), crawlLog.build(), db);
                        return null;
                    }

                });

                long payloadSize = writeWarcJob.get();
                LOG.debug("Payload of size {}b written for {}", payloadSize, crawlLog.getRequestedUri());
                size += payloadSize;
                extractTextJob.get();
            }

            try {
                warcWriter.closeRecord();
            } catch (IOException ex) {
                if (crawlLog.getSize() != size) {
                    Status status = Status.OUT_OF_RANGE.withDescription("Size doesn't match metadata. Expected "
                            + crawlLog.getSize() + ", but was " + size);
                    LOG.error(status.getDescription());
                    throw status.asException();
                } else {
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    LOG.error(ex.getMessage(), ex);
                    throw status.asException();
                }
            }

            return ref;
        } catch (Exception ex) {
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            LOG.error(ex.getMessage(), ex);
            throw status.asException();
        }
    }

}
