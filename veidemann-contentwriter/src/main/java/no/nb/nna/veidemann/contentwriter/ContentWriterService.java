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

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ContentWriterProto.RecordType;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteReply;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequest;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequestMeta;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteResponseMeta;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.veidemann.contentwriter.warc.WarcWriterPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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

            private boolean canceled = false;

            private ContentBuffer getContentBuffer(Integer recordNum) {
                return contentBuffers.computeIfAbsent(recordNum, n -> new ContentBuffer());
            }

            private WriteRequestMeta.RecordMeta getRecordMeta(int recordNum) throws StatusException {
                WriteRequestMeta.RecordMeta m = writeRequestMeta.getRecordMetaOrDefault(recordNum, null);
                if (m == null) {
                    throw Status.INVALID_ARGUMENT.withDescription("Missing metadata").asException();
                }
                return m;
            }

            @Override
            public void onNext(WriteRequest value) {
                if (writeRequestMeta != null) {
                    MDC.put("eid", writeRequestMeta.getExecutionId());
                    MDC.put("uri", writeRequestMeta.getTargetUri());
                }

                ContentBuffer contentBuffer;
                switch (value.getValueCase()) {
                    case META:
                        writeRequestMeta = value.getMeta();
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
                        contentBuffer = getContentBuffer(value.getPayload().getRecordNum());
                        contentBuffer.addPayload(value.getPayload().getData());
                        break;
                    case CANCEL:
                        canceled = true;
                        String cancelReason = value.getCancel();
                        LOG.info("Request cancelled before WARC record written. Reason {}", cancelReason);
                        for (ContentBuffer cb : contentBuffers.values()) {
                            cb.close();
                        }
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                if (writeRequestMeta != null) {
                    MDC.put("uri", writeRequestMeta.getTargetUri());
                    MDC.put("eid", writeRequestMeta.getExecutionId());
                }
                LOG.error("Error caught: {}", t.getMessage(), t);
                for (ContentBuffer contentBuffer : contentBuffers.values()) {
                    contentBuffer.close();
                }
            }

            @Override
            public void onCompleted() {
                if (canceled) {
                    responseObserver.onNext(WriteReply.getDefaultInstance());
                    responseObserver.onCompleted();
                    return;
                }

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

                        if (contentBuffer.getTotalSize() != recordMeta.getSize()) {
                            LOG.error("Size mismatch. Expected {}, but was {}",
                                    recordMeta.getSize(), contentBuffer.getTotalSize());
                            throw Status.INVALID_ARGUMENT.withDescription("Size mismatch").asException();
                        }

                        if (!contentBuffer.getBlockDigest().equals(recordMeta.getBlockDigest())) {
                            LOG.error("Block digest mismatch. Expected {}, but was {}",
                                    recordMeta.getBlockDigest(), contentBuffer.getBlockDigest());
                            throw Status.INVALID_ARGUMENT.withDescription("Block digest mismatch").asException();
                        }
                    } catch (StatusException ex) {
                        responseObserver.onError(ex);
                        return;
                    }
                }

                // Write
                List<String> allRecordIds = contentBuffers.values().stream()
                        .map(cb -> Util.formatIdentifierAsUrn(cb.getWarcId()))
                        .collect(Collectors.toList());

                try (WarcWriterPool.PooledWarcWriter pooledWarcWriter = warcWriterPool.borrow()) {
                    for (Entry<Integer, ContentBuffer> recordEntry : contentBuffers.entrySet()) {
                        ContentBuffer contentBuffer = recordEntry.getValue();
                        try {
                            WriteRequestMeta.RecordMeta recordMeta = getRecordMeta(recordEntry.getKey());

                            recordMeta = recordMeta.toBuilder().setPayloadDigest(contentBuffer.getPayloadDigest()).build();

                            if (recordMeta.getType() == RecordType.RESPONSE) {
                                recordMeta = detectRevisit(contentBuffer, recordMeta);
                            }

                            URI ref = writeRecord(pooledWarcWriter, contentBuffer, writeRequestMeta, recordMeta, allRecordIds);

                            WriteResponseMeta.RecordMeta.Builder responseMeta = WriteResponseMeta.RecordMeta.newBuilder()
                                    .setRecordNum(recordMeta.getRecordNum())
                                    .setType(recordMeta.getType())
                                    .setWarcId(contentBuffer.getWarcId())
                                    .setStorageRef(ref.toString())
                                    .setBlockDigest(contentBuffer.getBlockDigest())
                                    .setPayloadDigest(contentBuffer.getPayloadDigest());

                            reply.getMetaBuilder().putRecordMeta(responseMeta.getRecordNum(), responseMeta.build());
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
                } catch (Exception ex) {
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    LOG.error(ex.getMessage(), ex);
                    responseObserver.onError(status.asException());
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

    private WriteRequestMeta.RecordMeta detectRevisit(final ContentBuffer contentBuffer,
                                                      final WriteRequestMeta.RecordMeta recordMeta) {
        Optional<MessagesProto.CrawledContent> isDuplicate = db
                .hasCrawledContent(MessagesProto.CrawledContent.newBuilder()
                        .setDigest(contentBuffer.getPayloadDigest())
                        .setWarcId(contentBuffer.getWarcId())
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

    private URI writeRecord(final WarcWriterPool.PooledWarcWriter pooledWarcWriter,
                            final ContentBuffer contentBuffer, final WriteRequestMeta request,
                            final WriteRequestMeta.RecordMeta recordMeta, final List<String> allRecordIds)
            throws StatusException, InterruptedException, ExecutionException, TimeoutException {

        long size = 0L;

        SingleWarcWriter warcWriter = pooledWarcWriter.getWarcWriter();

        URI ref = warcWriter.writeWarcHeader(contentBuffer.getWarcId(), request, recordMeta, allRecordIds);

        if (contentBuffer.hasHeader()) {
            size += warcWriter.addPayload(contentBuffer.getHeader().newInput());
        }

        if (contentBuffer.hasPayload()) {
            // If both headers and payload are present, add separator
            if (contentBuffer.hasHeader()) {
                size += warcWriter.addPayload(CRLF);
            }

            long payloadSize = warcWriter.addPayload(contentBuffer.getPayload().newInput());
            if (recordMeta.getType() == RecordType.RESPONSE) {
                try {
                    textExtractor.analyze(contentBuffer.getWarcId(), request.getTargetUri(), recordMeta.getPayloadContentType(),
                            request.getStatusCode(), contentBuffer.getPayload().newInput(), db);
                } catch (Exception ex) {
                    LOG.error("Failed extracting text");
                }
            }

            LOG.debug("Payload of size {}b written for {}", payloadSize, request.getTargetUri());
            size += payloadSize;
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
    }

}
