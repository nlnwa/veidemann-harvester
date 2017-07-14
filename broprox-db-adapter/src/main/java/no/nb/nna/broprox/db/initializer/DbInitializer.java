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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.net.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.commons.opentracing.TracerFactory;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.Meta;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

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
        r.table(TABLES.SYSTEM.name).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.broprox").with("level", "INFO"))
                )).run(conn);

        r.tableCreate(TABLES.CRAWL_LOG.name).optArg("primary_key", "warcId").run(conn);
        r.table(TABLES.CRAWL_LOG.name)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp")))
                .run(conn);

        r.tableCreate(TABLES.CRAWLED_CONTENT.name).optArg("primary_key", "digest").run(conn);

        r.tableCreate(TABLES.EXTRACTED_TEXT.name).optArg("primary_key", "warcId").run(conn);

        r.tableCreate(TABLES.BROWSER_SCRIPTS.name).run(conn);

        r.tableCreate(TABLES.URI_QUEUE.name).run(conn);
        r.table(TABLES.URI_QUEUE.name).indexCreate("surt").run(conn);
        r.table(TABLES.URI_QUEUE.name).indexCreate("executionId",
                uri -> r.array(uri.g("executionId"), uri.g("sequence"))).run(conn);

        r.tableCreate(TABLES.EXECUTIONS.name).run(conn);

        r.tableCreate(TABLES.SCREENSHOT.name).run(conn);

        r.tableCreate(TABLES.CRAWL_ENTITIES.name).run(conn);

        r.tableCreate(TABLES.SEEDS.name).run(conn);
        r.table(TABLES.SEEDS.name).indexCreate("jobId").optArg("multi", true).run(conn);
        r.table(TABLES.SEEDS.name).indexCreate("entityId").run(conn);

        r.tableCreate(TABLES.CRAWL_JOBS.name).run(conn);

        r.tableCreate(TABLES.CRAWL_CONFIGS.name).run(conn);

        r.tableCreate(TABLES.CRAWL_SCHEDULE_CONFIGS.name).run(conn);

        r.tableCreate(TABLES.BROWSER_CONFIGS.name).run(conn);

        r.tableCreate(TABLES.POLITENESS_CONFIGS.name).run(conn);

        createMetaIndexes(TABLES.BROWSER_SCRIPTS,
                TABLES.CRAWL_ENTITIES,
                TABLES.SEEDS,
                TABLES.CRAWL_JOBS,
                TABLES.CRAWL_CONFIGS,
                TABLES.CRAWL_SCHEDULE_CONFIGS,
                TABLES.BROWSER_CONFIGS,
                TABLES.POLITENESS_CONFIGS
        );

        r.table(TABLES.URI_QUEUE.name).indexWait("surt", "executionId").run(conn);
        r.table(TABLES.CRAWL_LOG.name).indexWait("surt_time").run(conn);
        r.table(TABLES.SEEDS.name).indexWait("jobId", "entityId").run(conn);
    }

    private final void createMetaIndexes(TABLES... tables) {
        for (TABLES table : tables) {
            r.table(table.name).indexCreate("name", row -> row.g("meta").g("name").downcase()).run(conn);
            r.table(table.name)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true)
                    .run(conn);
            r.table(table.name)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true)
                    .run(conn);
        }
        for (TABLES table : tables) {
            r.table(table.name).indexWait("name", "label", "label_value").run(conn);
        }
    }

    private final void populateDb() {
        DbAdapter db = new RethinkDbAdapter(conn);
        try {
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/browser-scripts.yaml")) {
                readYamlFile(in, BrowserScript.class)
                        .forEach(o -> db.saveBrowserScript(o));
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/crawl-jobs.yaml")) {
                readYamlFile(in, CrawlJob.class)
                        .forEach(o -> db.saveCrawlJob(o));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    <T extends Message> Stream<T> readYamlFile(InputStream in, Class<T> type) {
        Yaml yaml = new Yaml();
        return StreamSupport.stream(yaml.loadAll(in).spliterator(), false)
                .map(o -> ProtoUtils.rethinkToProto((Map) o, type));
    }

}
