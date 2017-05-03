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
package no.nb.nna.broprox.db.initializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.net.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.broprox.commons.TracerFactory;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_BROWSER_CONFIGS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_BROWSER_SCRIPTS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWLED_CONTENT;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWL_CONFIGS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWL_ENTITIES;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWL_JOBS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWL_LOG;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_CRAWL_SCHEDULE_CONFIGS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_EXECUTIONS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_EXTRACTED_TEXT;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_POLITENESS_CONFIGS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_SCREENSHOT;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_SEEDS;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_SYSTEM;
import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_URI_QUEUE;

/**
 *
 */
public class DbInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DbInitializer.class);

    private static final Settings SETTINGS;

    static final RethinkDB r = RethinkDB.r;

    final Connection conn;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        TracerFactory.init("Controller", SETTINGS.getTracerUri());
    }

    public DbInitializer() {
        System.out.println("Connecting to: " + SETTINGS.getDbHost() + ":" + SETTINGS.getDbPort());
        conn = connect();
    }

    private Connection connect() {
        Connection c = null;
        int attempts = 0;
        while (c == null) {
            attempts++;
            try {
                c = r.connection()
                        .hostname(SETTINGS.getDbHost())
                        .port(SETTINGS.getDbPort())
                        .db(SETTINGS.getDbName())
                        .connect();
            } catch (ReqlDriverError e) {
                System.err.println(e.getMessage());
                if (attempts < 30) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    System.err.println("Too many connection attempts, giving up");
                    throw e;
                }
            }
        }
        return c;
    }

    public void initialize() {
        try {
            if (!(boolean) r.dbList().contains(SETTINGS.getDbName()).run(conn)) {
                System.out.println("Creating database: " + SETTINGS.getDbName());
                createDb();
                System.out.println("Populating database with default data");
                populateDb();
            } else {
                System.out.println("Database found, nothing to initialize");
            }
        } finally {
            conn.close();
        }
        System.out.println("DB initialized");
    }

    private final void createDb() {
        r.dbCreate(SETTINGS.getDbName()).run(conn);

        r.tableCreate(TABLE_SYSTEM).run(conn);

        r.tableCreate(TABLE_CRAWL_LOG).optArg("primary_key", "warcId").run(conn);
        r.table(TABLE_CRAWL_LOG)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp")))
                .run(conn);
        r.table(TABLE_CRAWL_LOG).indexWait("surt_time").run(conn);

        r.tableCreate(TABLE_CRAWLED_CONTENT).optArg("primary_key", "digest").run(conn);

        r.tableCreate(TABLE_EXTRACTED_TEXT).optArg("primary_key", "warcId").run(conn);

        r.tableCreate(TABLE_BROWSER_SCRIPTS).run(conn);
        createMetaIndexes(TABLE_BROWSER_SCRIPTS);

        r.tableCreate(TABLE_URI_QUEUE).run(conn);
        r.table(TABLE_URI_QUEUE).indexCreate("surt").run(conn);
        r.table(TABLE_URI_QUEUE).indexCreate("executionIds", uri -> uri.g("executionIds")
                .map(eid -> r.array(eid.g("id"), eid.g("seq")))
        ).optArg("multi", true).run(conn);
        r.table(TABLE_URI_QUEUE).indexWait("surt", "executionIds").run(conn);

        r.tableCreate(TABLE_EXECUTIONS).run(conn);

        r.tableCreate(TABLE_SCREENSHOT).run(conn);

        r.tableCreate(TABLE_CRAWL_ENTITIES).run(conn);
        createMetaIndexes(TABLE_CRAWL_ENTITIES);

        r.tableCreate(TABLE_SEEDS).run(conn);
        createMetaIndexes(TABLE_SEEDS);

        r.tableCreate(TABLE_CRAWL_JOBS).run(conn);
        createMetaIndexes(TABLE_CRAWL_JOBS);

        r.tableCreate(TABLE_CRAWL_CONFIGS).run(conn);
        createMetaIndexes(TABLE_CRAWL_CONFIGS);

        r.tableCreate(TABLE_CRAWL_SCHEDULE_CONFIGS).run(conn);
        createMetaIndexes(TABLE_CRAWL_SCHEDULE_CONFIGS);

        r.tableCreate(TABLE_BROWSER_CONFIGS).run(conn);
        createMetaIndexes(TABLE_BROWSER_CONFIGS);

        r.tableCreate(TABLE_POLITENESS_CONFIGS).run(conn);
        createMetaIndexes(TABLE_POLITENESS_CONFIGS);
    }

    private final void createMetaIndexes(String tableName) {
        r.table(tableName).indexCreate("name", row -> row.g("meta").g("name").downcase()).run(conn);
        r.table(tableName).indexWait("name").run(conn);
    }

    private final void populateDb() {
        DbAdapter db = new RethinkDbAdapter(conn);
        Yaml yaml = new Yaml();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("browser-scripts/extract-outlinks.yaml")) {
            Map<String, Object> scriptDef = yaml.loadAs(in, Map.class);
            ConfigProto.BrowserScript script = ProtoUtils.rethinkToProto(scriptDef, ConfigProto.BrowserScript.class);
            db.saveBrowserScript(script);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
