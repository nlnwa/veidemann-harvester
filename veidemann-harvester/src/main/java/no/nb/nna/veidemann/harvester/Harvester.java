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
package no.nb.nna.veidemann.harvester;

import java.io.File;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.AlreadyCrawledCache;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.db.RethinkDbAlreadyCrawledCache;
import no.nb.nna.veidemann.harvester.api.HarvesterApiServer;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.proxy.DnsServiceHostResolver;
import no.nb.nna.veidemann.harvester.proxy.RecordingProxy;
import no.nb.nna.veidemann.harvester.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for launching the service.
 */
public class Harvester {

    private static final Logger LOG = LoggerFactory.getLogger(Harvester.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("Harvester");
    }

    /**
     * Create a new Harvester service.
     */
    public Harvester() {
    }

    /**
     * Start the service.
     * <p>
     * @return this instance
     */
    public Harvester start() {
        BrowserSessionRegistry sessionRegistry = new BrowserSessionRegistry();

        try (DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName());

             DnsServiceClient dnsServiceClient = new DnsServiceClient(
                        SETTINGS.getDnsResolverHost(), SETTINGS.getDnsResolverPort());

             BrowserController controller = new BrowserController(
                        SETTINGS.getBrowserHost(), SETTINGS.getBrowserPort(), db, sessionRegistry);

             ContentWriterClient contentWriterClient = new ContentWriterClient(
                        SETTINGS.getContentWriterHost(), SETTINGS.getContentWriterPort());

             AlreadyCrawledCache cache = new RethinkDbAlreadyCrawledCache(
                        SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName());

             RecordingProxy proxy = new RecordingProxy(
                        new File(SETTINGS.getWorkDir()),
                        SETTINGS.getProxyPort(), db, contentWriterClient,
                        new DnsServiceHostResolver(dnsServiceClient), sessionRegistry, cache);

             HarvesterApiServer apiServer = new HarvesterApiServer(controller, proxy).start();) {

            LOG.info("Veidemann harvester (v. {}) started", Harvester.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException ex) {
            System.err.println("Configuration error: " + ex.getLocalizedMessage());
            System.exit(1);
        } catch (Exception ex) {
            LOG.error("Could not start service", ex);
            throw new RuntimeException(ex);
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