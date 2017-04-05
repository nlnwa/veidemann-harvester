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
package no.nb.nna.broprox.controller;

import java.io.IOException;

import com.google.protobuf.util.Timestamps;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.model.ControllerGrpc;
import no.nb.nna.broprox.model.ControllerProto;

/**
 *
 */
public class ControllerApiServer {

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new ControllerImpl())
                .build()
                .start();
        System.out.println("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                ControllerApiServer.this.stop();
                System.err.println("*** server shut down");
            }

        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final ControllerApiServer server = new ControllerApiServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class ControllerImpl extends ControllerGrpc.ControllerImplBase {

        @Override
        public void saveEntity(ControllerProto.CrawlEntity request, StreamObserver<ControllerProto.CrawlEntity> responseObserver) {
            System.out.println("Save entity called");
            ControllerProto.CrawlEntity reply = ControllerProto.CrawlEntity.newBuilder()
                    .setId("Random UID")
                    .setName("Nasjonalbiblioteket")
                    .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void listCrawlEntities(ControllerProto.CrawlEntityListRequest request, StreamObserver<ControllerProto.CrawlEntityListReply> responseObserver) {
            System.out.println("List crawl entitise called");
            ControllerProto.CrawlEntity entity1 = ControllerProto.CrawlEntity.newBuilder()
                    .setId("Random UID 1")
                    .setName("Nasjonalbiblioteket")
                    .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build();

            ControllerProto.CrawlEntity entity2 = ControllerProto.CrawlEntity.newBuilder()
                    .setId("Random UID 2")
                    .setName("VG")
                    .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build();

            ControllerProto.CrawlEntityListReply reply = ControllerProto.CrawlEntityListReply.newBuilder()
                    .addEntity(entity1)
                    .addEntity(entity2)
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

    }
}
