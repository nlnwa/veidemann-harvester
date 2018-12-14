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
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewDb implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateNewDb.class);

    public static final String DB_VERSION = "1.0";

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

        conn.exec(r.tableCreate(Tables.SYSTEM.name));
        conn.exec(r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "db_version").with("db_version", DB_VERSION)));
        conn.exec(r.table(Tables.SYSTEM.name).insert(r.hashMap("id", "log_levels")
                .with("logLevel",
                        r.array(r.hashMap("logger", "no.nb.nna.veidemann").with("level", "INFO"))
                )));

        conn.exec(r.tableCreate(Tables.CONFIG.name));

        conn.exec(r.tableCreate(Tables.LOCKS.name));

        conn.exec(r.tableCreate(Tables.CRAWL_LOG.name).optArg("primary_key", "warcId"));
        conn.exec(r.table(Tables.CRAWL_LOG.name)
                .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))));
        conn.exec(r.table(Tables.CRAWL_LOG.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(Tables.PAGE_LOG.name).optArg("primary_key", "warcId"));
        conn.exec(r.table(Tables.PAGE_LOG.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(Tables.CRAWLED_CONTENT.name).optArg("primary_key", "digest"));

        conn.exec(r.tableCreate(Tables.EXTRACTED_TEXT.name).optArg("primary_key", "warcId"));

        conn.exec(r.tableCreate(Tables.URI_QUEUE.name));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("surt"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("executionId"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessId"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        conn.exec(r.tableCreate(Tables.EXECUTIONS.name));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("createdTime"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("jobId"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("state"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("seedId"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexCreate("jobExecutionId"));

        conn.exec(r.tableCreate(Tables.JOB_EXECUTIONS.name));
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).indexCreate("startTime"));
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).indexCreate("jobId"));
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).indexCreate("state"));

        conn.exec(r.tableCreate(Tables.SCREENSHOT.name));
        conn.exec(r.table(Tables.SCREENSHOT.name).indexCreate("executionId"));

        conn.exec(r.tableCreate(Tables.CRAWL_ENTITIES.name));

        conn.exec(r.tableCreate(Tables.SEEDS.name));
        conn.exec(r.table(Tables.SEEDS.name)
                .indexCreate("jobId", row -> row.g("seed").g("jobId"))
                .optArg("multi", true));
        conn.exec(r.table(Tables.SEEDS.name)
                .indexCreate("entityId", row -> row.g("seed").g("entityId")));

        conn.exec(r.tableCreate(Tables.CRAWL_HOST_GROUP.name));
        conn.exec(r.table(Tables.CRAWL_HOST_GROUP.name).indexCreate("nextFetchTime"));

        conn.exec(r.tableCreate(Tables.ALREADY_CRAWLED_CACHE.name)
                .optArg("durability", "soft")
                .optArg("shards", 3)
                .optArg("replicas", 1));

        createMetaIndexes(
                Tables.CONFIG,
                Tables.CRAWL_ENTITIES,
                Tables.SEEDS
        );

        conn.exec(r.table(Tables.URI_QUEUE.name)
                .indexWait("surt", "executionId", "crawlHostGroupKey_sequence_earliestFetch"));
        conn.exec(r.table(Tables.CRAWL_LOG.name).indexWait("surt_time", "executionId"));
        conn.exec(r.table(Tables.PAGE_LOG.name).indexWait("executionId"));
        conn.exec(r.table(Tables.SCREENSHOT.name).indexWait("executionId"));
        conn.exec(r.table(Tables.SEEDS.name).indexWait("jobId", "entityId"));
        conn.exec(r.table(Tables.CRAWL_HOST_GROUP.name).indexWait("nextFetchTime"));
        conn.exec(r.table(Tables.EXECUTIONS.name).indexWait("createdTime", "jobId", "state", "seedId", "jobExecutionId"));
        conn.exec(r.table(Tables.JOB_EXECUTIONS.name).indexWait("startTime", "jobId", "state"));
    }

    private final void createMetaIndexes(Tables... tables) throws DbQueryException, DbConnectionException {
        for (Tables table : tables) {
            conn.exec(r.table(table.name).indexCreate("name", row -> row.g("meta").g("name").downcase()));
            conn.exec(r.table(table.name)
                    .indexCreate("label",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table.name)
                    .indexCreate("kind_label_key",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(row.g("kind"), label.g("key").downcase())))
                    .optArg("multi", true));
            conn.exec(r.table(table.name)
                    .indexCreate("label_value",
                            row -> row.g("meta").g("label").map(
                                    label -> label.g("value").downcase()))
                    .optArg("multi", true));
            conn.exec(r.table(table.name).indexCreate("lastModified", row -> row.g("meta").g("lastModified")));
            conn.exec(r.table(table.name).indexCreate("lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase()));
        }
        for (Tables table : tables) {
            conn.exec(r.table(table.name).indexWait("name", "label", "kind_label_key", "label_value", "lastModified", "lastModifiedBy"));
        }
    }
}
