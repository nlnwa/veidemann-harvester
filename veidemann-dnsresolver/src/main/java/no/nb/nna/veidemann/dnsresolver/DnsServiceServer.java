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
package no.nb.nna.veidemann.dnsresolver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.dnsresolver.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DnsServiceServer {

    private static final Logger LOG = LoggerFactory.getLogger(DnsServiceServer.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("DnsService");
    }

    public DnsServiceServer() {
    }

    /**
     * Start the service.
     * <p>
     * @return this instance
     */
    public DnsServiceServer start() {
        try (
                DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName(),
                        SETTINGS.getDbUser(), SETTINGS.getDbPassword());
                ContentWriterClient contentWriterClient = new ContentWriterClient(
                        SETTINGS.getContentWriterHost(), SETTINGS.getContentWriterPort());
                DnsServiceApiServer apiServer = new DnsServiceApiServer(
                        SETTINGS.getApiPort(), new DnsLookup(db, contentWriterClient, SETTINGS.getDnsServers()))
                        .start();) {

            LOG.info("Veidemann Dns Service (v. {}) started", DnsServiceServer.class.getPackage()
                    .getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException ex) {
            System.err.println("Configuration error: " + ex.getLocalizedMessage());
            System.exit(1);
        }

        return this;
    }

    /**
     * Get the settings object.
     * <p>
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
