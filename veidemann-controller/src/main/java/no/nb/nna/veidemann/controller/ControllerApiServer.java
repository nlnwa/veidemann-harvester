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
package no.nb.nna.veidemann.controller;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.auth.AuAuServerInterceptor;
import no.nb.nna.veidemann.controller.scheduler.FrontierClient;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 *
 */
public class ControllerApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerApiServer.class);

    private final Server server;

    public ControllerApiServer(Settings settings, DbAdapter db, FrontierClient frontierClient,
                               AuAuServerInterceptor auAuServerInterceptor) {
        this(settings, ServerBuilder.forPort(settings.getApiPort()), db, frontierClient, auAuServerInterceptor);
    }

    public ControllerApiServer(Settings settings, ServerBuilder<?> serverBuilder, DbAdapter db,
                               FrontierClient frontierClient, AuAuServerInterceptor auAuServerInterceptor) {

        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        // Use secure transport if certChain and private key are available
        File certDir = new File("/veidemann/tls");
        File certChain = new File(certDir, "tls.crt");
        File privateKey = new File(certDir, "tls.key");
        if (certChain.isFile() && certChain.canRead() && privateKey.isFile() && privateKey.canRead()) {
            LOG.info("Found certificate. Setting up secure protocol.");
            serverBuilder.useTransportSecurity(certChain, privateKey);
        } else {
            LOG.warn("No CA certificate found. Protocol will use insecure plain text.");
        }

        server = serverBuilder
                .addService(tracingInterceptor.intercept(
                        auAuServerInterceptor.intercept(new ControllerService(settings, db, frontierClient))))
                .addService(tracingInterceptor.intercept(
                        auAuServerInterceptor.intercept(new StatusService(db))))
                .addService(tracingInterceptor.intercept(
                        auAuServerInterceptor.intercept(new ReportService(db))))
                .build();
    }

    public ControllerApiServer start() {
        try {
            server.start();

            LOG.info("Controller api listening on {}", server.getPort());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    ControllerApiServer.this.close();
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
