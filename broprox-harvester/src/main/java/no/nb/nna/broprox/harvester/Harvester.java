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
package no.nb.nna.broprox.harvester;

import java.io.File;
import java.io.IOException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.harvester.api.ApiServer;
import no.nb.nna.broprox.harvester.proxy.RecordingProxy;
import no.nb.nna.broprox.harvester.settings.Settings;
import org.littleshoot.proxy.mitm.RootCertificateException;
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
    }

    /**
     * Create a new Broprox service.
     */
    public Harvester() {
    }

    /**
     * Start the service.
     *
     * @return this instance
     */
    public Harvester start() {
        try {
            DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName());

            RecordingProxy proxy = new RecordingProxy(new File(SETTINGS.getWorkDir()), SETTINGS.getProxyPort(), db);
            ApiServer apiServer = new ApiServer();

            LOG.info("Broprox harvester (v. {}) started", Harvester.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException ex) {
            System.err.println("Configuration error: " + ex.getLocalizedMessage());
            System.exit(1);
        } catch (RootCertificateException | IOException ex) {
            LOG.error("Could not start service", ex);
        }

        return this;
    }

    /**
     * Get the settings object.
     *
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
