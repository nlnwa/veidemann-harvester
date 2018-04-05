/*
 * Copyright 2018 National Library of Norway.
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
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UpgradeDbBase implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpgradeDbBase.class);

    static final RethinkDB r = RethinkDB.r;

    RethinkDbConnection conn;

    final String dbName;

    public UpgradeDbBase(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void run() {
        LOG.info("Upgrading from {} to {}", fromVersion(), toVersion());

        conn = RethinkDbConnection.getInstance();
        String version = conn.exec(r.table(TABLES.SYSTEM.name).get("db_version").g("db_version"));
        if (!fromVersion().equals(version)) {
            throw new IllegalStateException("Expected db to be version " + fromVersion() + ", but was " + version);
        }

        upgrade();
        conn.exec(r.table(TABLES.SYSTEM.name).get("db_version").update(r.hashMap("db_version", toVersion())));
    }

    abstract void upgrade();

    abstract String fromVersion();

    abstract String toVersion();
}
