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

import com.rethinkdb.RethinkDB;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbUpgradeException;
import no.nb.nna.veidemann.commons.opentracing.TracerFactory;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DbInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DbInitializer.class);

    private static final Settings SETTINGS;

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbConnection conn;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("DbInitializer");
    }

    public DbInitializer() throws DbConnectionException {
        System.out.println("Connecting to: " + SETTINGS.getDbHost() + ":" + SETTINGS.getDbPort());
        if (!RethinkDbConnection.isConfigured()) {
            RethinkDbConnection.configure(SETTINGS);
        }
        conn = RethinkDbConnection.getInstance();
    }

    public void initialize() throws DbUpgradeException, DbQueryException, DbConnectionException {
        try {
            if (!(boolean) conn.exec(r.dbList().contains(SETTINGS.getDbName()))) {
                // No existing database, creating a new one
                LOG.info("Creating database: " + SETTINGS.getDbName());
                new CreateNewDb(SETTINGS.getDbName()).run();
                LOG.info("Populating database with default data");
                new PopulateDbWithDefaultData().run();
            } else {
                String version = conn.exec(r.table(TABLES.SYSTEM.name).get("db_version").g("db_version"));
                if (CreateNewDb.DB_VERSION.equals(version)) {
                    LOG.info("Database found and is newest version: {}", version);
                } else {
                    LOG.info("Database with version {} found, upgrading", version);
                    upgrade(version);
                }
            }
        } finally {
            conn.close();
        }
        LOG.info("DB initialized");
    }

    private void upgrade(String fromVersion) throws DbUpgradeException {
        switch (fromVersion) {
            case "0.1":
                new Upgrade0_1To0_2(SETTINGS.getDbName()).run();
                break;
            default:
                throw new DbUpgradeException("Unknown database version '" + fromVersion + "', unable to upgrade");
        }
    }

}
