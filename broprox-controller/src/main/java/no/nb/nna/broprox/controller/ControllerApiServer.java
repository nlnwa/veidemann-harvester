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
import java.io.UncheckedIOException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.commons.TracerFactory;
import no.nb.nna.broprox.controller.settings.Settings;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ControllerApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerApiServer.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("Controller", SETTINGS.getTracerUri());
    }

    private final Server server;

    private final DbAdapter db;

    public ControllerApiServer() {
        this(SETTINGS.getApiPort());
    }

    public ControllerApiServer(int port) {
        this(ServerBuilder.forPort(port),
                new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName()));
    }

    public ControllerApiServer(ServerBuilder<?> serverBuilder, DbAdapter db) {
        this.db = db;

        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        server = serverBuilder.addService(tracingInterceptor.intercept(new ControllerService(db))).build();
    }

    public ControllerApiServer start() {
        try {
            server.start();

            LOG.info("Broprox Controller (v. {}) started",
                    ControllerApiServer.class.getPackage().getImplementationVersion());
            LOG.info("Listening on {}", server.getPort());

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
        if (db != null) {
            db.close();
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
