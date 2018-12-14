/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db.initializer;

import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;

public class Upgrade0_4To1_0 extends UpgradeDbBase {
    public Upgrade0_4To1_0(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        // Create config table
        conn.exec(r.tableCreate(Tables.CONFIG.name));
        conn.exec(r.table(Tables.CONFIG.name).indexCreate("name", row -> row.g("meta").g("name").downcase()));
        conn.exec(r.table(Tables.CONFIG.name)
                .indexCreate("label",
                        row -> row.g("meta").g("label").map(
                                label -> r.array(label.g("key").downcase(), label.g("value").downcase())))
                .optArg("multi", true));
        conn.exec(r.table(Tables.CONFIG.name)
                .indexCreate("label_value",
                        row -> row.g("meta").g("label").map(
                                label -> label.g("value").downcase()))
                .optArg("multi", true));
        conn.exec(r.table(Tables.CONFIG.name).indexCreate("lastModified", row -> row.g("meta").g("lastModified")));
        conn.exec(r.table(Tables.CONFIG.name).indexCreate("lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase()));


        // Add indexes to seeds and entities
        conn.exec(r.table(Tables.SEEDS.name).indexCreate("lastModified", row -> row.g("meta").g("lastModified")));
        conn.exec(r.table(Tables.SEEDS.name).indexCreate("lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase()));
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).indexCreate("lastModified", row -> row.g("meta").g("lastModified")));
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).indexCreate("lastModifiedBy", row -> row.g("meta").g("lastModifiedBy").downcase()));

        conn.exec(r.table(Tables.CONFIG.name).indexWait("name", "label", "label_value", "lastModified", "lastModifiedBy"));
        conn.exec(r.table(Tables.SEEDS.name).indexWait("lastModified", "lastModifiedBy"));
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).indexWait("lastModified", "lastModifiedBy"));

        // Convert data
        convertTable("config_browser_scripts", "browserScript");
        convertTable("config_crawl_jobs", "crawlJob");
        convertTable("config_crawl_configs", "crawlConfig");
        convertTable("config_crawl_schedule_configs", "crawlScheduleConfig");
        convertTable("config_browser_configs", "browserConfig");
        convertTable("config_politeness_configs", "politenessConfig");
        convertTable("config_crawl_host_group_configs", "crawlHostGroupConfig");
        renameBrowserConfigField();
        convertSeeds();
        convertCrawlEntities();
        convertRoleMappings();

        createMetaIndexes(Tables.CONFIG, Tables.SEEDS, Tables.CRAWL_ENTITIES);

        // Drop old config tables
        conn.exec(r.tableDrop("config_browser_scripts"));
        conn.exec(r.tableDrop("config_crawl_jobs"));
        conn.exec(r.tableDrop("config_crawl_configs"));
        conn.exec(r.tableDrop("config_crawl_schedule_configs"));
        conn.exec(r.tableDrop("config_browser_configs"));
        conn.exec(r.tableDrop("config_politeness_configs"));
        conn.exec(r.tableDrop("config_crawl_host_group_configs"));
        conn.exec(r.tableDrop("config_role_mappings"));
    }

    @Override
    String fromVersion() {
        return "0.4";
    }

    @Override
    String toVersion() {
        return "1.0";
    }

    private void convertTable(String tableName, String kind) throws DbQueryException, DbConnectionException {
        conn.exec(r.table(tableName).forEach(o ->
                r.table(Tables.CONFIG.name).insert(o
                        .merge(r.hashMap(kind, o.without("id", "meta")))
                        .pluck("id", "meta", kind)
                        .merge(r.hashMap("apiVersion", "v1").with("kind", kind))
                )
        ));
    }

    private void renameBrowserConfigField() throws DbQueryException, DbConnectionException {
        String kind = "browserConfig";
        conn.exec(r.table(Tables.CONFIG.name).filter(r.hashMap("kind", kind)).replace(o -> o
                .merge(r.hashMap(kind, r.hashMap("maxInactivityTimeMs", o.g(kind).g("sleepAfterPageloadMs"))))
                .without(r.hashMap(kind, "sleepAfterPageloadMs"))
        ));
    }

    private void convertSeeds() throws DbQueryException, DbConnectionException {
        conn.exec(r.table(Tables.SEEDS.name).indexDrop("jobId"));
        conn.exec(r.table(Tables.SEEDS.name).indexDrop("entityId"));

        conn.exec(r.table(Tables.SEEDS.name).replace(o -> o
                .merge(r.hashMap("seed", o.without("id", "meta")))
                .pluck("id", "meta", "seed")
                .merge(r.hashMap("apiVersion", "v1").with("kind", "seed"))
        ));

        conn.exec(r.table(Tables.SEEDS.name)
                .indexCreate("jobId", row -> row.g("seed").g("jobId"))
                .optArg("multi", true));
        conn.exec(r.table(Tables.SEEDS.name)
                .indexCreate("entityId", row -> row.g("seed").g("entityId")));
    }

    private void convertCrawlEntities() throws DbQueryException, DbConnectionException {
        conn.exec(r.table(Tables.CRAWL_ENTITIES.name).replace(o -> o
                .merge(r.hashMap("crawlEntity", o.without("id", "meta")))
                .pluck("id", "meta", "crawlEntity")
                .merge(r.hashMap("apiVersion", "v1").with("kind", "crawlEntity"))
        ));
    }

    private void convertRoleMappings() throws DbQueryException, DbConnectionException {
        conn.exec(r.table("config_role_mappings").forEach(o ->
                r.table(Tables.CONFIG.name).insert(o
                        .merge(r.hashMap("roleMapping", o.without("id")))
                        .pluck("id", "roleMapping")
                        .merge(r.hashMap("apiVersion", "v1")
                                .with("kind", "roleMapping")
                                .with("meta", r.hashMap("name", "converted")))
                )
        ));
    }

    private final void createMetaIndexes(Tables... tables) throws DbQueryException, DbConnectionException {
        for (Tables table : tables) {
            conn.exec(r.table(table.name)
                    .indexCreate("kind_label_key",
                            row -> row.g("meta").g("label").map(
                                    label -> r.array(row.g("kind"), label.g("key").downcase())))
                    .optArg("multi", true));
        }
        for (Tables table : tables) {
            conn.exec(r.table(table.name).indexWait("kind_label_key"));
        }
    }
}
