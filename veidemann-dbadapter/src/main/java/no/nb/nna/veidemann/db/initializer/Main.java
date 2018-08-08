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
package no.nb.nna.veidemann.db.initializer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;

/**
 * Main class for launching the service.
 */
public final class Main {
    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        Settings settings = ConfigBeanFactory.create(config, Settings.class);
        try {
            DbService.configure(settings);
        } catch (DbConnectionException e) {
            throw new RuntimeException(e);
        }

        TracerFactory.init("DbInitializer");
    }

    /**
     * Private constructor to avoid instantiation.
     */
    private Main() {
    }

    /**
     * Start the server.
     * <p>
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) throws DbException {
        // This class intentionally doesn't do anything except for instanciating a ResourceResolverServer.
        // This is necessary to be able to replace the LogManager. The system property must be set before any other
        // logging is even loaded.
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");

        try (DbService db = DbService.getInstance()) {
            db.getDbInitializer().initialize();
        }
    }

}
