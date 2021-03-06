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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

    private boolean shouldRun = true;

    /**
     * Create a new Harvester service.
     */
    public Harvester() {
    }

    /**
     * Start the service.
     * <p>
     *
     * @return this instance
     */
    public Harvester start() {
        DefaultExports.initialize();
        try {
            HTTPServer server = new HTTPServer(SETTINGS.getPrometheusPort());
        } catch (IOException ex) {
            System.err.println("Could not start Prometheus exporter: " + ex.getLocalizedMessage());
            System.exit(3);
        }

        BrowserSessionRegistry sessionRegistry = new BrowserSessionRegistry();

        try (DbService db = DbService.configure(SETTINGS);

             ContentWriterClient contentWriterClient = new ContentWriterClient(
                     SETTINGS.getContentWriterHost(), SETTINGS.getContentWriterPort());

             BrowserController controller = new BrowserController(SETTINGS.getBrowserWSEndpoint(), sessionRegistry,
                     contentWriterClient);

             FrontierClient frontierClient = new FrontierClient(controller, SETTINGS.getFrontierHost(),
                     SETTINGS.getFrontierPort(), SETTINGS.getMaxOpenSessions(), SETTINGS.getBrowserWSEndpoint(),
                     SETTINGS.getProxyHost(), SETTINGS.getProxyPort(), SETTINGS.isHeadlessBrowser());

             RobotsServiceClient robotsServiceClient = new RobotsServiceClient(SETTINGS.getRobotsTxtEvaluatorHost(), SETTINGS.getRobotsTxtEvaluatorPort());

             BrowserControllerApiServer apiServer = new BrowserControllerApiServer(SETTINGS.getBrowserControllerPort(), sessionRegistry, robotsServiceClient).start()
        ) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> shouldRun = false));

            Thread.sleep(4000);
            LOG.info("Veidemann harvester (v. {}) started", Harvester.class.getPackage().getImplementationVersion());
            while (shouldRun) {
                // Ensure that the browser gets a little time to settle before opening a new session
                Thread.sleep(2000);
                frontierClient.requestNextPage();
            }
        } catch (ConfigException ex) {
            System.err.println("Configuration error: " + ex.getLocalizedMessage());
            System.exit(2);
        } catch (Exception ex) {
            LOG.error("Could not start service", ex);
            throw new RuntimeException(ex);
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
