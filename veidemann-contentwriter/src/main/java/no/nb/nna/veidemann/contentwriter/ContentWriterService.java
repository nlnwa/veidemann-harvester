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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import no.nb.nna.veidemann.api.ContentWriterProto.RecordType;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteReply;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequest;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequestMeta;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteResponseMeta;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
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

    private final TextExtractor textExtractor;

    public ContentWriterService(DbAdapter db, WarcWriterPool warcWriterPool, TextExtractor textExtractor) {
        this.db = db;
        this.warcWriterPool = warcWriterPool;
        this.textExtractor = textExtractor;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new StreamObserver<WriteRequest>() {
            private final Map<Integer, ContentBuffer> contentBuffers = new HashMap<>();

            private WriteRequestMeta writeRequestMeta;

            private ContentBuffer getContentBuffer(Integer recordNum) {
                return contentBuffers.computeIfAbsent(recordNum, n -> new ContentBuffer());
            }

            private WriteRequestMeta.RecordMeta getRecordMeta(int recordNum) throws StatusException {
                for (WriteRequestMeta.RecordMeta m : writeRequestMeta.getRecordMetaList()) {
                    if (m.getRecordNum() == recordNum) {
                        return m;
                    }
                }
                throw Status.INVALID_ARGUMENT.withDescription("Missing metadata").asException();
            }

            @Override
            public void onNext(WriteRequest value) {
                ContentBuffer contentBuffer;
                switch (value.getValueCase()) {
                    case META:
                        writeRequestMeta = value.getMeta();
                        MDC.put("eid", writeRequestMeta.getExecutionId());
                        MDC.put("uri", writeRequestMeta.getTargetUri());
                        break;
                    case HEADER:
                        contentBuffer = getContentBuffer(value.getHeader().getRecordNum());
                        if (contentBuffer.hasHeader()) {
                            LOG.error("Header received twice");
                            Status status = Status.INVALID_ARGUMENT.withDescription("Header received twice");
                            responseObserver.onError(status.asException());
                            break;
                        }
                        contentBuffer.setHeader(value.getHeader().getData());
                        break;
                    case PAYLOAD:
                        contentBuffer = getContentBuffer(value.getHeader().getRecordNum());
                        contentBuffer.addPayload(value.getPayload().getData());
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                if (status.getCode().equals(Code.CANCELLED)) {
                    if (writeRequestMeta != null) {
                        MDC.put("uri", writeRequestMeta.getTargetUri());
                        MDC.put("eid", writeRequestMeta.getExecutionId());
                    }
                    LOG.info("Request cancelled before WARC record written");
                } else {
                    LOG.error("Error caught: {}", t.getMessage(), t);
                }
            }

            @Override
            public void onCompleted() {
                if (writeRequestMeta == null) {
                    LOG.error("Missing metadata object");
                    Status status = Status.INVALID_ARGUMENT.withDescription("Missing metadata object");
                    responseObserver.onError(status.asException());
                    return;
                }

                MDC.put("uri", writeRequestMeta.getTargetUri());
                MDC.put("eid", writeRequestMeta.getExecutionId());

                WriteReply.Builder reply = WriteReply.newBuilder();
                // Validate
                for (Entry<Integer, ContentBuffer> recordEntry : contentBuffers.entrySet()) {
                    try {
                        ContentBuffer contentBuffer = recordEntry.getValue();
                        WriteRequestMeta.RecordMeta recordMeta = getRecordMeta(recordEntry.getKey());

                        if (contentBuffer.getTotalSize() == 0L) {
                            LOG.error("Nothing to store");
                            throw Status.INVALID_ARGUMENT.withDescription("Nothing to store").asException();
                        }

                        if (!contentBuffer.getBlockDigest().equals(recordMeta.getBlockDigest())) {
                            LOG.error("Block digest mismatch. Expected {}, but was {}",
                                    recordMeta.getBlockDigest(), contentBuffer.getBlockDigest());
                            throw Status.INVALID_ARGUMENT.withDescription("Block digest mismatch").asException();
                        }

                        if (contentBuffer.getTotalSize() != recordMeta.getSize()) {
                            LOG.error("Size mismatch. Expected {}, but was {}",
                                    recordMeta.getSize(), contentBuffer.getTotalSize());
                            throw Status.INVALID_ARGUMENT.withDescription("Size mismatch").asException();
                        }
                    } catch (StatusException ex) {
                        responseObserver.onError(ex);
                    }
                }

                // Write
                for (Entry<Integer, ContentBuffer> recordEntry : contentBuffers.entrySet()) {
                    ContentBuffer contentBuffer = recordEntry.getValue();
                    try {
                        String recordId = Util.createIdentifier();
                        WriteRequestMeta.RecordMeta recordMeta = getRecordMeta(recordEntry.getKey());

                        if (recordMeta.getType() == RecordType.RESPONSE) {
                            recordMeta = detectRevisit(recordId, contentBuffer, recordMeta);
                        }

                        URI ref = writeRecord(recordId, contentBuffer, writeRequestMeta, recordMeta);

                        WriteResponseMeta.RecordMeta.Builder responseMeta = WriteResponseMeta.RecordMeta.newBuilder()
                                .setRecordNum(recordMeta.getRecordNum())
                                .setType(recordMeta.getType())
                                .setWarcId(recordId)
                                .setStorageRef(ref.toString());

                        reply.getMetaBuilder().addRecordMeta(responseMeta);
                    } catch (StatusException ex) {
                        LOG.error("Failed write: {}", ex.getMessage(), ex);
                        responseObserver.onError(ex);
                    } catch (Throwable ex) {
                        LOG.error("Failed write: {}", ex.getMessage(), ex);
                        responseObserver.onError(Status.fromThrowable(ex).asException());
                    } finally {
                        contentBuffer.close();
                    }
                }
                responseObserver.onNext(reply.build());
                responseObserver.onCompleted();
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

    private WriteRequestMeta.RecordMeta detectRevisit(String warcId, final ContentBuffer contentBuffer,
                                                      final WriteRequestMeta.RecordMeta recordMeta) {
        Optional<MessagesProto.CrawledContent> isDuplicate = db
                .hasCrawledContent(MessagesProto.CrawledContent.newBuilder()
                        .setDigest(contentBuffer.getPayloadDigest())
                        .setWarcId(warcId)
                        .build());

        if (isDuplicate.isPresent()) {
            WriteRequestMeta.RecordMeta.Builder recordMetaBuilder = recordMeta.toBuilder();
            recordMetaBuilder.setType(RecordType.REVISIT)
                    .setRecordContentType("application/http")
                    .setBlockDigest(contentBuffer.getHeaderDigest())
                    .setPayloadDigest(isDuplicate.get().getDigest())
                    .setSize(contentBuffer.getHeaderSize())
                    .setWarcRefersTo(isDuplicate.get().getWarcId());

            contentBuffer.removePayload();
            LOG.debug("Detected {} as a revisit of {}",
                    MDC.get("uri"), recordMeta.getWarcRefersTo());
            return recordMetaBuilder.build();
        }
        return recordMeta;
    }

    private URI writeRecord(final String warcId, final ContentBuffer contentBuffer, final WriteRequestMeta request, final WriteRequestMeta.RecordMeta recordMeta)
            throws StatusException {

        try (WarcWriterPool.PooledWarcWriter pooledWarcWriter = warcWriterPool.borrow()) {
            long size = 0L;

            SingleWarcWriter warcWriter = pooledWarcWriter.getWarcWriter();

            URI ref = warcWriter.writeWarcHeader(warcId, request, recordMeta);

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

                ForkJoinTask<Void> extractTextJob = null;
                if (recordMeta.getType() == RecordType.RESPONSE) {
                    extractTextJob = ForkJoinPool.commonPool().submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            textExtractor.analyze(warcId, request.getTargetUri(), recordMeta.getPayloadContentType(),
                                    request.getStatusCode(), contentBuffer.getPayload().newInput(), db);
                            return null;
                        }

                    });
                }

                long payloadSize = writeWarcJob.get();
                LOG.debug("Payload of size {}b written for {}", payloadSize, request.getTargetUri());
                size += payloadSize;
                if (extractTextJob != null) {
                    extractTextJob.get();
                }
            }

            try {
                warcWriter.closeRecord();
            } catch (IOException ex) {
                if (recordMeta.getSize() != size) {
                    Status status = Status.OUT_OF_RANGE.withDescription("Size doesn't match metadata. Expected "
                            + recordMeta.getSize() + ", but was " + size);
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
