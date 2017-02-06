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
package no.nb.nna.broprox.db;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

/**
 * An implementation of DbAdapter for RethinkDb.
 */
public class RethinkDbAdapter implements DbAdapter {

    static final RethinkDB r = RethinkDB.r;

    final String dbHost;

    final int dbPort;

    final String dbName;

    final Connection conn;

    public RethinkDbAdapter(String dbHost, int dbPort, String dbName) {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;

        conn = connect();
    }

    private final Connection connect() {
        Connection c = r.connection().hostname(dbHost).port(dbPort).db(dbName).connect();
        if (!(boolean) r.dbList().contains(dbName).run(c)) {
            r.dbCreate(dbName).run(c);
            r.tableCreate("crawl_log").optArg("primary_key", "warcId").run(c);
            r.table("crawl_log").indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp"))).run(c);
            r.tableCreate("crawled_content").optArg("primary_key", "digest").run(c);
        }
        return c;
    }

    public Optional<CrawledContent> isDuplicateContent(String digest) {
        Map<String, Object> response = r.table("crawled_content").get(digest).run(conn);
        return DbObjectFactory.of(CrawledContent.class, response);
    }

    public void deleteCrawledContent(String digest) {
        r.table("crawled_content").get(digest).delete().run(conn);
    }

    public CrawledContent addCrawledContent(CrawledContent cc) {
        return insert("crawled_content", cc);
    }

    public CrawlLog addCrawlLog(CrawlLog cl) {
        Map<String, Object> data = ((DbObject)cl).getMap();
        if (!data.containsKey("timeStamp")) {
            data.put("timeStamp", r.now());
        }

        return insert("crawl_log", cl);
    }

    public CrawlLog updateCrawlLog(CrawlLog cl) {
        Map<String, Object> data = ((DbObject)cl).getMap();
        if (!data.containsKey("timeStamp")) {
            data.put("timeStamp", r.now());
        }

        return update("crawl_log", cl.getWarcId(), cl);
    }

    private <T extends DbObject> T insert(String table, T data) {
        Map response = r.table(table)
                .insert(data.getMap())
                .optArg("conflict", "error")
                .optArg("return_changes", "always")
                .run(conn);
        data.setMap(((List<Map<String, Map<String, Object>>>)response.get("changes")).get(0).get("new_val"));
        return data;
    }

    private <T extends DbObject> T update(String table, Object key, T data) {
        Map response = r.table(table)
                .get(key)
                .update(data.getMap())
                .optArg("return_changes", "always")
                .run(conn);
        data.setMap(((List<Map<String, Map<String, Object>>>)response.get("changes")).get(0).get("new_val"));
        return data;
    }
}
