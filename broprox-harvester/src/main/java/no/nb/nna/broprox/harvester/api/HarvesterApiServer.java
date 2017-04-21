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
package no.nb.nna.broprox.harvester.api;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import javax.ws.rs.core.UriBuilder;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.harvester.Harvester;
import no.nb.nna.broprox.harvester.browsercontroller.BrowserController;
import no.nb.nna.broprox.harvester.proxy.RecordingProxy;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The external service REST API.
 */
public class HarvesterApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HarvesterApiServer.class);

    private final Server server;

    /**
     * Construct a new REST API server.
     */
    public HarvesterApiServer(DbAdapter db, BrowserController controller, RecordingProxy proxy) {
        final int port = Harvester.getSettings().getApiPort();
        LOG.info("Starting API server listening on port {}.", port);

                ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        server = ServerBuilder.forPort(port).addService(tracingInterceptor.intercept(new HarvesterService(db, controller, proxy))).build();

//        URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(port).build();
//        ResourceConfig resourceConfig = new ResourceConfig()
//                .register(ApiResource.class)
//                .register(new AbstractBinder() {
//                    @Override
//                    protected void configure() {
//                        bind(db).to(DbAdapter.class);
//                    }
//
//                })
//                .register(new AbstractBinder() {
//                    @Override
//                    protected void configure() {
//                        bind(controller);
//                    }
//
//                })
//                .register(new AbstractBinder() {
//                    @Override
//                    protected void configure() {
//                        bind(proxy);
//                    }
//
//                });
//        server = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }

    @Override
    public void close() {
        LOG.info("Shutting down API server.");
        if (server != null) {
            server.shutdown();
        }
    }

    public HarvesterApiServer start() {
        try {
            server.start();

            LOG.info("Broprox Harvester (v. {}) started",
                    HarvesterApiServer.class.getPackage().getImplementationVersion());
            LOG.info("Listening on {}", server.getPort());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    HarvesterApiServer.this.close();
                }

            });

            return this;
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }
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
