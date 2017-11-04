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
package no.nb.nna.veidemann.robotsservice;

import java.io.IOException;
import java.io.UncheckedIOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RobotsApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RobotsApiServer.class);

    private final Server server;

    public RobotsApiServer(int port, String proxyHost, int proxyPort) {
        this(ServerBuilder.forPort(port), proxyHost, proxyPort);
    }

    public RobotsApiServer(ServerBuilder<?> serverBuilder, String proxyHost, int proxyPort) {

        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        server = serverBuilder.addService(tracingInterceptor.intercept(new RobotsService(proxyHost, proxyPort))).build();
    }

    public RobotsApiServer start() {
        try {
            server.start();

            LOG.info("Robots Evaluator api listening on {}", server.getPort());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    RobotsApiServer.this.close();
                }

            });

            return this;
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdown();
        }
        System.err.println("*** server shut down");
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() {
        if (server != null) {
            try {
                server.awaitTermination();
            } catch (InterruptedException ex) {
                close();
                throw new RuntimeException(ex);
            }
        }
    }

}
