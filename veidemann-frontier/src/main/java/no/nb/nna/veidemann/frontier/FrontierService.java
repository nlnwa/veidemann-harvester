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
package no.nb.nna.veidemann.frontier;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.frontier.api.FrontierApiServer;
import no.nb.nna.veidemann.frontier.settings.Settings;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.HarvesterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for launching the service.
 */
public class FrontierService {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierService.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("Frontier");
    }

    /**
     * Create a new Frontier service.
     */
    public FrontierService() {
    }

    /**
     * Start the service.
     * <p>
     *
     * @return this instance
     */
    public FrontierService start() {
        try (DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName(),
                SETTINGS.getDbUser(), SETTINGS.getDbPassword());
             HarvesterClient harvesterClient = new HarvesterClient(
                     SETTINGS.getHarvesterHost(), SETTINGS.getHarvesterPort())
                     .withMaxWaitForExhaustedHarvesterMs(SETTINGS.getMaxWaitForExhaustedHarvester());

             RobotsServiceClient robotsServiceClient = new RobotsServiceClient(
                     SETTINGS.getRobotsEvaluatorHost(), SETTINGS.getRobotsEvaluatorPort());

             DnsServiceClient dnsServiceClient = new DnsServiceClient(
                     SETTINGS.getDnsResolverHost(), SETTINGS.getDnsResolverPort());

             Frontier frontier = new Frontier((RethinkDbAdapter) db, harvesterClient, robotsServiceClient, dnsServiceClient);
             FrontierApiServer apiServer = new FrontierApiServer(SETTINGS.getApiPort(), frontier).start();) {

            LOG.info("Veidemann Frontier (v. {}) started",
                    FrontierService.class.getPackage().getImplementationVersion());

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
        }

        return this;
    }

    /**
     * Get the settings object.
     * <p>
     *
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
