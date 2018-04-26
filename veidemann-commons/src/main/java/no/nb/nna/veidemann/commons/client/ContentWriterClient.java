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
package no.nb.nna.veidemann.commons.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ContentWriterProto.Data;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteReply;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequest;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequestMeta;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteResponseMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 *
 */
public class ContentWriterClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriterClient.class);

    private final ManagedChannel channel;

    private final ContentWriterGrpc.ContentWriterBlockingStub blockingStub;

    private final ContentWriterGrpc.ContentWriterStub asyncStub;

    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicInteger responseCount = new AtomicInteger(0);
    private final AtomicInteger sessionCount = new AtomicInteger(0);

    public ContentWriterClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("ContentWriter client pointing to " + host + ":" + port);
    }

    public ContentWriterClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up ContentWriter client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = ContentWriterGrpc.newBlockingStub(channel);
        asyncStub = ContentWriterGrpc.newStub(channel);
    }

    public ContentWriterSession createSession() {
        sessionCount.incrementAndGet();
        return new ContentWriterSession();
    }

    @Override
    public void close() {
        LOG.trace("Session count: {}, Request count: {}, Response count: {}", sessionCount.get(), requestCount.get(), responseCount.get());
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    public class ContentWriterSession {

        private final StreamObserver<WriteRequest> requestObserver;

        private final CountDownLatch finishLatch = new CountDownLatch(1);

        private WriteResponseMeta responseMeta;

        private StatusException error;

        private ContentWriterSession() {

            StreamObserver<WriteReply> responseObserver = new StreamObserver<WriteReply>() {
                @Override
                public void onNext(WriteReply reply) {
                    responseMeta = reply.getMeta();
                }

                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    if (status.toString().contains(" OK:")) {
                        LOG.debug("ContentWriter threw expected exception: {}", status.toString());
                    } else {
                        LOG.warn("ContentWriter Failed: {}", status);
                    }
                    if (t instanceof StatusException) {
                        error = (StatusException) t;
                    } else {
                        error = status.asException();
                    }
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    finishLatch.countDown();
                }

            };

            this.requestObserver = asyncStub.write(responseObserver);
        }

        public synchronized ContentWriterSession sendMetadata(WriteRequestMeta meta) {
            sendRequest(() -> WriteRequest.newBuilder().setMeta(meta).build());
            requestCount.incrementAndGet();
            return this;
        }

        public synchronized ContentWriterSession sendHeader(Data data) {
            sendRequest(() -> WriteRequest.newBuilder().setHeader(data).build());
            return this;
        }

        public synchronized ContentWriterSession sendPayload(Data data) {
            sendRequest(() -> WriteRequest.newBuilder().setPayload(data).build());
            return this;
        }

        public synchronized WriteResponseMeta finish() throws InterruptedException, StatusException {
            requestObserver.onCompleted();
            // Receiving happens asynchronously
            finishLatch.await(1, TimeUnit.MINUTES);
            if (error != null) {
                throw error;
            }
            responseCount.incrementAndGet();
            LOG.trace("Session count: {}, Request count: {}, Response count: {}", sessionCount.get(), requestCount.get(), responseCount.get());
            return responseMeta;
        }

        public synchronized void cancel(String reason) throws InterruptedException {
            if (reason != null && reason.startsWith("OK:")) {
                LOG.debug("Cancelling content writer session. Reason: {}", reason);
            } else {
                LOG.info("Cancelling content writer session. Reason: {}", reason);
            }
            sendRequest(() -> WriteRequest.newBuilder().setCancel(reason).build());
            requestObserver.onCompleted();

            finishLatch.await(1, TimeUnit.MINUTES);
            if (error != null) {
                LOG.info("Cancel got error: {}", error.toString());
            }
            responseCount.incrementAndGet();
            LOG.trace("Session count: {}, Request count: {}, Response count: {}", sessionCount.get(), requestCount.get(), responseCount.get());
        }

        public boolean isOpen() {
            return finishLatch.getCount() > 0;
        }

        private void sendRequest(Supplier<WriteRequest> request) {
            if (finishLatch.getCount() == 0) {
                // RPC completed or errored before we finished sending.
                // Sending further requests won't error, but they will just be thrown away.
                LOG.info("RPC completed or errored before we finished sending.");
                return;
            }

            try {
                requestObserver.onNext(request.get());
            } catch (RuntimeException e) {
                // Cancel RPC
                requestObserver.onError(e);
                throw e;
            }
        }

    }
}
