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
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DbInitializerTestIT {
    public static RethinkDbAdapter db;

    @BeforeClass
    public static void setUp() throws Exception {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        if (!RethinkDbConnection.isConfigured()) {
            RethinkDbConnection.configure(dbHost, dbPort, "veidemann", "admin", "");
        }
        db = new RethinkDbAdapter();

        RethinkDB r = RethinkDB.r;
        try {
            RethinkDbConnection.getInstance().exec(r.dbDrop("veidemann"));
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
    }

    @Test
    public void initialize() throws DbException {
        new CreateDbV0_1("veidemann").run();
        new DbInitializer().initialize();
        String version = RethinkDbConnection.getInstance().exec(RethinkDB.r.table(TABLES.SYSTEM.name).get("db_version").g("db_version"));
        assertThat(version).isEqualTo("0.2");
    }
}