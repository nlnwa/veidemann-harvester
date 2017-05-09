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

import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;

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

        r.tableCreate(TABLES.SYSTEM.name).run(conn);
        r.table(TABLES.SYSTEM.name).insert(r.hashMap("id", "db_version").with("db_version", "0.1")).run(conn);

        r.tableCreate(TABLES.CRAWL_LOG.name).optArg("primary_key", "warcId").run(conn);
        r.table(TABLES.CRAWL_LOG.name)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp")))
                .run(conn);

        r.tableCreate(TABLES.CRAWLED_CONTENT.name).optArg("primary_key", "digest").run(conn);

        r.tableCreate(TABLES.EXTRACTED_TEXT.name).optArg("primary_key", "warcId").run(conn);

        r.tableCreate(TABLES.BROWSER_SCRIPTS.name).run(conn);
        createMetaIndexes(TABLES.BROWSER_SCRIPTS);

        r.tableCreate(TABLES.URI_QUEUE.name).run(conn);
        r.table(TABLES.URI_QUEUE.name).indexCreate("surt").run(conn);
        r.table(TABLES.URI_QUEUE.name).indexCreate("executionIds", uri -> uri.g("executionIds")
                .map(eid -> r.array(eid.g("id"), eid.g("seq")))
        ).optArg("multi", true).run(conn);

        r.tableCreate(TABLES.EXECUTIONS.name).run(conn);

        r.tableCreate(TABLES.SCREENSHOT.name).run(conn);

        r.tableCreate(TABLES.CRAWL_ENTITIES.name).run(conn);
        createMetaIndexes(TABLES.CRAWL_ENTITIES);

        r.tableCreate(TABLES.SEEDS.name).run(conn);
        createMetaIndexes(TABLES.SEEDS);
        r.table(TABLES.SEEDS.name).indexCreate("jobId").optArg("multi", true).run(conn);

        r.tableCreate(TABLES.CRAWL_JOBS.name).run(conn);
        createMetaIndexes(TABLES.CRAWL_JOBS);

        r.tableCreate(TABLES.CRAWL_CONFIGS.name).run(conn);
        createMetaIndexes(TABLES.CRAWL_CONFIGS);

        r.tableCreate(TABLES.CRAWL_SCHEDULE_CONFIGS.name).run(conn);
        createMetaIndexes(TABLES.CRAWL_SCHEDULE_CONFIGS);

        r.tableCreate(TABLES.BROWSER_CONFIGS.name).run(conn);
        createMetaIndexes(TABLES.BROWSER_CONFIGS);

        r.tableCreate(TABLES.POLITENESS_CONFIGS.name).run(conn);
        createMetaIndexes(TABLES.POLITENESS_CONFIGS);

        r.table(TABLES.URI_QUEUE.name).indexWait("surt", "executionIds").run(conn);
        r.table(TABLES.CRAWL_LOG.name).indexWait("surt_time").run(conn);
        r.table(TABLES.SEEDS.name).indexWait("jobId").run(conn);
    }

    private final void createMetaIndexes(TABLES table) {
        r.table(table.name).indexCreate("name", row -> row.g("meta").g("name").downcase()).run(conn);
        r.table(table.name).indexWait("name").run(conn);
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
