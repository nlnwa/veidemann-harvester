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
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbInitializer;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbUpgradeException;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RethinkDbInitializer implements DbInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbInitializer.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbInitializer(RethinkDbConnection conn) {
        this.conn = conn;
    }

    public void initialize() throws DbUpgradeException, DbQueryException, DbConnectionException {
        String dbName = conn.getConnection().db().get();

        if (!(boolean) conn.exec(r.dbList().contains(dbName))) {
            // No existing database, creating a new one
            LOG.info("Creating database: " + dbName);
            new CreateNewDb(dbName, conn).run();
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
        LOG.info("DB initialized");
    }

    @Override
    public void delete() throws DbException {
        try {
            conn.exec(r.dbDrop("veidemann"));
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
    }

    private void upgrade(String fromVersion) throws DbUpgradeException {
        String dbName = conn.getConnection().db().get();

        switch (fromVersion) {
            case "0.1":
                new Upgrade0_1To0_2(dbName, conn).run();
                break;
            default:
                throw new DbUpgradeException("Unknown database version '" + fromVersion + "', unable to upgrade");
        }
    }

}