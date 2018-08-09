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
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewDb implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateNewDb.class);

    public static final String DB_VERSION = "0.2";

    static final RethinkDB r = RethinkDB.r;

    final RethinkDbConnection conn;

    final String dbName;

    public CreateNewDb(String dbName, RethinkDbConnection conn) {
        this.dbName = dbName;
        this.conn = conn;
    }

    @Override
    public void run() {
        try {
            createDb();
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }

    private final void createDb() throws DbQueryException, DbConnectionException {
        conn.exec("create-db", r.dbCreate(dbName));

        conn.exec(r.tableCreate(TABLES.SYSTEM.name));
        conn.exec(r.table(TABLES.SYSTEM.name).insert(r.hashMap("id", "db_version").with("db_version", DB_VERSION)));
        conn.exec(r.table(TABLES.SYSTEM.name).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));

        conn.exec(r.tableCreate(TABLES.CRAWL_LOG.name).optArg("primary_key", "warcId"));
        conn.exec(r.table(TABLES.CRAWL_LOG.name)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))));
        conn.exec(r.table(TABLES.CRAWL_LOG.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(TABLES.PAGE_LOG.name).optArg("primary_key", "warcId"));
        conn.exec(r.table(TABLES.PAGE_LOG.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(TABLES.CRAWLED_CONTENT.name).optArg("primary_key", "digest"));

        conn.exec(r.tableCreate(TABLES.EXTRACTED_TEXT.name).optArg("primary_key", "warcId"));

        conn.exec(r.tableCreate(TABLES.BROWSER_SCRIPTS.name));

        conn.exec(r.tableCreate(TABLES.URI_QUEUE.name));
        conn.exec(r.table(TABLES.URI_QUEUE.name).indexCreate("surt"));
        conn.exec(r.table(TABLES.URI_QUEUE.name).indexCreate("executionId"));
        conn.exec(r.table(TABLES.URI_QUEUE.name).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessId"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        conn.exec(r.tableCreate(TABLES.EXECUTIONS.name));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexCreate("startTime"));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexCreate("jobId"));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexCreate("state"));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexCreate("seedId"));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexCreate("jobExecutionId"));

        conn.exec(r.tableCreate(TABLES.JOB_EXECUTIONS.name));
        conn.exec(r.table(TABLES.JOB_EXECUTIONS.name).indexCreate("startTime"));
        conn.exec(r.table(TABLES.JOB_EXECUTIONS.name).indexCreate("jobId"));
        conn.exec(r.table(TABLES.JOB_EXECUTIONS.name).indexCreate("state"));

        conn.exec(r.tableCreate(TABLES.SCREENSHOT.name));
        conn.exec(r.table(TABLES.SCREENSHOT.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(TABLES.CRAWL_ENTITIES.name));

        conn.exec(r.tableCreate(TABLES.SEEDS.name));
        conn.exec(r.table(TABLES.SEEDS.name).indexCreate("jobId").optArg("multi", true));
        conn.exec(r.table(TABLES.SEEDS.name).indexCreate("entityId"));

        conn.exec(r.tableCreate(TABLES.CRAWL_JOBS.name));

        conn.exec(r.tableCreate(TABLES.CRAWL_CONFIGS.name));

        conn.exec(r.tableCreate(TABLES.CRAWL_SCHEDULE_CONFIGS.name));

        conn.exec(r.tableCreate(TABLES.BROWSER_CONFIGS.name));

        conn.exec(r.tableCreate(TABLES.POLITENESS_CONFIGS.name));

        conn.exec(r.tableCreate(TABLES.CRAWL_HOST_GROUP_CONFIGS.name));

        conn.exec(r.tableCreate(TABLES.CRAWL_HOST_GROUP.name));
        conn.exec(r.table(TABLES.CRAWL_HOST_GROUP.name).indexCreate("nextFetchTime"));

        conn.exec(r.tableCreate(TABLES.ALREADY_CRAWLED_CACHE.name)
                .optArg("durability", "soft")
                .optArg("shards", 3)
                .optArg("replicas", 1));

        conn.exec(r.tableCreate(TABLES.ROLE_MAPPINGS.name));

        createMetaIndexes(
                TABLES.BROWSER_SCRIPTS,
                TABLES.CRAWL_ENTITIES,
                TABLES.SEEDS,
                TABLES.CRAWL_JOBS,
                TABLES.CRAWL_CONFIGS,
                TABLES.CRAWL_SCHEDULE_CONFIGS,
                TABLES.BROWSER_CONFIGS,
                TABLES.POLITENESS_CONFIGS,
                TABLES.CRAWL_HOST_GROUP_CONFIGS
        );

        conn.exec(r.table(TABLES.URI_QUEUE.name)
                .indexWait("surt", "executionId", "crawlHostGroupKey_sequence_earliestFetch"));
        conn.exec(r.table(TABLES.CRAWL_LOG.name).indexWait("surt_time", "executionId"));
        conn.exec(r.table(TABLES.PAGE_LOG.name).indexWait("executionId"));
        conn.exec(r.table(TABLES.SCREENSHOT.name).indexWait("executionId"));
        conn.exec(r.table(TABLES.SEEDS.name).indexWait("jobId", "entityId"));
        conn.exec(r.table(TABLES.CRAWL_HOST_GROUP.name).indexWait("nextFetchTime"));
        conn.exec(r.table(TABLES.EXECUTIONS.name).indexWait("startTime", "jobId", "state", "seedId", "jobExecutionId"));
        conn.exec(r.table(TABLES.JOB_EXECUTIONS.name).indexWait("startTime", "jobId", "state"));
    }

    private final void createMetaIndexes(TABLES... tables) throws DbQueryException, DbConnectionException {
        for (TABLES table : tables) {
            conn.exec(r.table(table.name).indexCreate("name", row -> row.g("meta").g("name").downcase()));
            conn.exec(r.table(table.name)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table.name)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true));
        }
        for (TABLES table : tables) {
            conn.exec(r.table(table.name).indexWait("name", "label", "label_value"));
        }
    }
}
