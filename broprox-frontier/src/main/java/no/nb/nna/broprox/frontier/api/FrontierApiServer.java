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
package no.nb.nna.broprox.frontier.api;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.frontier.FrontierService;
import no.nb.nna.broprox.frontier.worker.Frontier;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FrontierApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierApiServer.class);

    final HttpServer server;

    /**
     * Construct a new REST API server.
     */
    public FrontierApiServer(DbAdapter db, Frontier queueProcessor) {
        final int port = FrontierService.getSettings().getApiPort();

        LOG.info("Starting server listening on port {}.", port);
        URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(port).build();
        ResourceConfig resourceConfig = new ResourceConfig()
                .register(StatsResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(db).to(DbAdapter.class);
                    }

                })
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(queueProcessor);
                    }

                });

        server = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }

    @Override
    public void close() {
        LOG.info("Shutting down API server.");
        server.shutdown();
    }

}
