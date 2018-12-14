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
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDbV0_1 implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateDbV0_1.class);

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbAdapter db;

    final String dbName;

    public CreateDbV0_1(RethinkDbAdapter db, String dbName) {
        this.db = db;
        this.dbName = dbName;
    }

    @Override
    public void run() {
        try {
            createDb();
            new PopulateDbWithTestData().run();
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }

    private final void createDb() throws DbException {
        db.executeRequest("create-db", r.dbCreate(dbName));

        db.executeRequest("", r.tableCreate(Tables.SYSTEM.name));
        db.executeRequest("", r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "db_version").with("db_version", "0.1")));
        db.executeRequest("", r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));

        db.executeRequest("", r.tableCreate(Tables.CRAWL_LOG.name).optArg("primary_key", "warcId"));
        db.executeRequest("", r.table(Tables.CRAWL_LOG.name)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))));
        db.executeRequest("", r.table(Tables.CRAWL_LOG.name).indexCreate("executeRequestutionId"));

        db.executeRequest("", r.tableCreate(Tables.PAGE_LOG.name).optArg("primary_key", "warcId"));
        db.executeRequest("", r.table(Tables.PAGE_LOG.name).indexCreate("executeRequestutionId"));

        db.executeRequest("", r.tableCreate(Tables.CRAWLED_CONTENT.name).optArg("primary_key", "digest"));

        db.executeRequest("", r.tableCreate(Tables.EXTRACTED_TEXT.name).optArg("primary_key", "warcId"));

        db.executeRequest("", r.tableCreate("config_browser_scripts"));

        db.executeRequest("", r.tableCreate(Tables.URI_QUEUE.name));
        db.executeRequest("", r.table(Tables.URI_QUEUE.name).indexCreate("surt"));
        db.executeRequest("", r.table(Tables.URI_QUEUE.name).indexCreate("executeRequestutionId"));
        db.executeRequest("", r.table(Tables.URI_QUEUE.name).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessId"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        db.executeRequest("", r.tableCreate(Tables.EXECUTIONS.name));
        db.executeRequest("", r.table(Tables.EXECUTIONS.name).indexCreate("startTime"));

        db.executeRequest("", r.tableCreate(Tables.SCREENSHOT.name));
        db.executeRequest("", r.table(Tables.SCREENSHOT.name).indexCreate("executeRequestutionId"));

        db.executeRequest("", r.tableCreate(Tables.CRAWL_ENTITIES.name));

        db.executeRequest("", r.tableCreate(Tables.SEEDS.name));
        db.executeRequest("", r.table(Tables.SEEDS.name).indexCreate("jobId").optArg("multi", true));
        db.executeRequest("", r.table(Tables.SEEDS.name).indexCreate("entityId"));

        db.executeRequest("", r.tableCreate("config_crawl_jobs"));

        db.executeRequest("", r.tableCreate("config_crawl_configs"));

        db.executeRequest("", r.tableCreate("config_crawl_schedule_configs"));

        db.executeRequest("", r.tableCreate("config_browser_configs"));

        db.executeRequest("", r.tableCreate("config_politeness_configs"));

        db.executeRequest("", r.tableCreate("config_crawl_host_group_configs"));

        db.executeRequest("", r.tableCreate(Tables.CRAWL_HOST_GROUP.name));
        db.executeRequest("", r.table(Tables.CRAWL_HOST_GROUP.name).indexCreate("nextFetchTime"));

        db.executeRequest("", r.tableCreate(Tables.ALREADY_CRAWLED_CACHE.name)
                .optArg("durability", "soft")
                .optArg("shards", 3)
                .optArg("replicas", 1));

        db.executeRequest("", r.tableCreate("config_role_mappings"));

        createMetaIndexes(
                "config_browser_scripts",
                Tables.CRAWL_ENTITIES.name,
                Tables.SEEDS.name,
                "config_crawl_jobs",
                "config_crawl_configs",
                "config_crawl_schedule_configs",
                "config_browser_configs",
                "config_politeness_configs",
                "config_crawl_host_group_configs"
        );

        db.executeRequest("", r.table(Tables.URI_QUEUE.name)
                .indexWait("surt", "executeRequestutionId", "crawlHostGroupKey_sequence_earliestFetch"));
        db.executeRequest("", r.table(Tables.CRAWL_LOG.name).indexWait("surt_time", "executeRequestutionId"));
        db.executeRequest("", r.table(Tables.PAGE_LOG.name).indexWait("executeRequestutionId"));
        db.executeRequest("", r.table(Tables.SCREENSHOT.name).indexWait("executeRequestutionId"));
        db.executeRequest("", r.table(Tables.SEEDS.name).indexWait("jobId", "entityId"));
        db.executeRequest("", r.table(Tables.CRAWL_HOST_GROUP.name).indexWait("nextFetchTime"));
        db.executeRequest("", r.table(Tables.EXECUTIONS.name).indexWait("startTime"));
    }

    private final void createMetaIndexes(String... tables) throws DbException {
        for (String table : tables) {
            db.executeRequest("", r.table(table).indexCreate("name", row -> row.g("meta").g("name").downcase()));
            db.executeRequest("", r.table(table)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true));
            db.executeRequest("", r.table(table)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true));
        }
        for (String table : tables) {
            db.executeRequest("", r.table(table).indexWait("name", "label", "label_value"));
        }
    }
}
