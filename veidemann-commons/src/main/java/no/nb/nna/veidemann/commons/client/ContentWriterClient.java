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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteReply;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 *
 */
public class ContentWriterClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriterClient.class);

    private final ManagedChannel channel;

    private final ContentWriterGrpc.ContentWriterBlockingStub blockingStub;

    private final ContentWriterGrpc.ContentWriterStub asyncStub;

    public ContentWriterClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
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
        return new ContentWriterSession();
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public class ContentWriterSession {

        private final StreamObserver<WriteRequest> requestObserver;

        private final CountDownLatch finishLatch = new CountDownLatch(1);

        private String storageRef;

        private StatusException error;

        private ContentWriterSession() {

            StreamObserver<WriteReply> responseObserver = new StreamObserver<WriteReply>() {
                @Override
                public void onNext(WriteReply reply) {
                    storageRef = reply.getStorageRef();
                }

                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    LOG.warn("ContentWriter Failed: {}", status);
                    error = status.asException();
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    finishLatch.countDown();
                }

            };

            this.requestObserver = asyncStub.write(responseObserver);
        }

        public ContentWriterSession sendCrawlLog(CrawlLog crawlLog) {
            sendRequest(() -> WriteRequest.newBuilder().setCrawlLog(crawlLog).build());
            return this;
        }

        public ContentWriterSession sendHeader(ByteString header) {
            sendRequest(() -> WriteRequest.newBuilder().setHeader(header).build());
            return this;
        }

        public ContentWriterSession sendPayload(ByteString payload) {
            sendRequest(() -> WriteRequest.newBuilder().setPayload(payload).build());
            return this;
        }

        public String finish() throws InterruptedException, StatusException {
            requestObserver.onCompleted();
            // Receiving happens asynchronously
            finishLatch.await(1, TimeUnit.MINUTES);
            if (error != null) {
                throw error;
            }
            return storageRef;
        }

        private void sendRequest(Supplier<WriteRequest> request) {
            if (finishLatch.getCount() == 0) {
                // RPC completed or errored before we finished sending.
                // Sending further requests won't error, but they will just be thrown away.
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
