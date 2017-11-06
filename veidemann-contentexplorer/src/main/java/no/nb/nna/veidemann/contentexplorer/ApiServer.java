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

package no.nb.nna.veidemann.contentexplorer;

import java.io.File;
import java.net.URI;

import io.netty.channel.Channel;
import javax.ws.rs.core.UriBuilder;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ApiServer {
private static final Logger LOG = LoggerFactory.getLogger(ApiServer.class);

    /**
     * Construct a new REST API server.
     */
    public ApiServer(DbAdapter db, File warcDir) {
        final int port = ContentExplorer.getSettings().getApiPort();

        LOG.info("Starting server listening on port {}.", port);
        URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(port).build();
        ResourceConfig resourceConfig = new ResourceConfig()
                .register(ContentExplorerResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(warcDir);
                    }

                })
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(db).to(DbAdapter.class);
                    }

                });

        final Channel server = NettyHttpContainerProvider.createHttp2Server(baseUri, resourceConfig, null);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down server.");
            server.close();
        }));
    }
}
