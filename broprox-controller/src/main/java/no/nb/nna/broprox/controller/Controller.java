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


import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.broprox.commons.auth.AuAuServerInterceptor;
import no.nb.nna.broprox.commons.auth.IdTokenAuAuServerInterceptor;
import no.nb.nna.broprox.commons.auth.IdTokenValidator;
import no.nb.nna.broprox.commons.auth.NoopAuAuServerInterceptor;
import no.nb.nna.broprox.commons.auth.UserRoleMapper;
import no.nb.nna.broprox.commons.opentracing.TracerFactory;
import no.nb.nna.broprox.controller.scheduler.CrawlJobScheduler;
import no.nb.nna.broprox.controller.scheduler.FrontierClient;
import no.nb.nna.broprox.controller.settings.Settings;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Controller {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("Controller");
    }

    public Controller() {
    }

    /**
     * Start the service.
     * <p>
     * @return this instance
     */
    public Controller start() {
        try (DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName());
             FrontierClient frontierClient = new FrontierClient(SETTINGS.getFrontierHost(), SETTINGS
                        .getFrontierPort());

             ControllerApiServer apiServer = new ControllerApiServer(SETTINGS.getApiPort(), db, frontierClient,
                     getAuAuServerInterceptor(db)).start();

             CrawlJobScheduler scheduler = new CrawlJobScheduler(db, frontierClient).start();) {

            LOG.info("Broprox Controller (v. {}) started", Controller.class.getPackage().getImplementationVersion());

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

    private AuAuServerInterceptor getAuAuServerInterceptor(DbAdapter db) {
        String issuerUrl = SETTINGS.getOpenIdConnectIssuer();
        if (issuerUrl == null || issuerUrl.isEmpty()) {
            return new NoopAuAuServerInterceptor();
        } else {
            return new IdTokenAuAuServerInterceptor(new UserRoleMapper(db), new IdTokenValidator(issuerUrl));
        }
    }
}
