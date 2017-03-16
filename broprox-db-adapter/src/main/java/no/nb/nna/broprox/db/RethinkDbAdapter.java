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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import no.nb.nna.broprox.db.model.CrawledContent;
import no.nb.nna.broprox.db.model.CrawlLog;
import no.nb.nna.broprox.db.model.ExtractedText;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.db.model.BrowserScript;
import no.nb.nna.broprox.db.model.CrawlExecutionStatus;
import no.nb.nna.broprox.db.model.QueuedUri;
import no.nb.nna.broprox.db.model.Screenshot;
import org.yaml.snakeyaml.Yaml;

/**
 * An implementation of DbAdapter for RethinkDb.
 */
public class RethinkDbAdapter implements DbAdapter {

    public static final String TABLE_CRAWL_LOG = "crawl_log";

    public static final String TABLE_CRAWLED_CONTENT = "crawled_content";

    public static final String TABLE_EXTRACTED_TEXT = "extracted_text";

    public static final String TABLE_BROWSER_SCRIPTS = "browser_scripts";

    public static final String TABLE_URI_QUEUE = "uri_queue";

    public static final String TABLE_SCREENSHOT = "screenshot";

    public static final String TABLE_EXECUTIONS = "executions";

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
        createDb();
    }

    private final Connection connect() {
        Connection c = r.connection().hostname(dbHost).port(dbPort).db(dbName).connect();
        return c;
    }

    private final void createDb() {
        if (!(boolean) r.dbList().contains(dbName).run(conn)) {
            r.dbCreate(dbName).run(conn);

            r.tableCreate(TABLE_CRAWL_LOG).optArg("primary_key", "warcId").run(conn);
            r.table(TABLE_CRAWL_LOG)
                    .indexCreate("surt_time", row -> r.array(row.g("surt"), row.g("timeStamp")))
                    .run(conn);
            r.table(TABLE_CRAWL_LOG).indexWait("surt_time").run(conn);

            r.tableCreate(TABLE_CRAWLED_CONTENT).optArg("primary_key", "digest").run(conn);

            r.tableCreate(TABLE_EXTRACTED_TEXT).optArg("primary_key", "warcId").run(conn);

            r.tableCreate(TABLE_BROWSER_SCRIPTS).run(conn);

            r.tableCreate(TABLE_URI_QUEUE).run(conn);
            r.table(TABLE_URI_QUEUE).indexCreate("surt").run(conn);
            r.table(TABLE_URI_QUEUE).indexCreate("executionIds").optArg("multi", true).run(conn);
            r.table(TABLE_URI_QUEUE).indexWait("surt", "executionIds").run(conn);

            r.tableCreate(TABLE_EXECUTIONS).run(conn);

            r.tableCreate(TABLE_SCREENSHOT).run(conn);

            populateDb();
        }
    }

    private final void populateDb() {
        Yaml yaml = new Yaml();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("browser-scripts/extract-outlinks.yaml")) {
            Map<String, Object> scriptDef = yaml.loadAs(in, Map.class);
            BrowserScript script = DbObjectFactory.of(BrowserScript.class, scriptDef).get();
            addBrowserScript(script);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Optional<CrawledContent> isDuplicateContent(String digest) {
        return get(TABLE_CRAWLED_CONTENT, digest, CrawledContent.class);
    }

    public void deleteCrawledContent(String digest) {
        delete(TABLE_CRAWLED_CONTENT, digest);
    }

    public CrawledContent addCrawledContent(CrawledContent cc) {
        return insert(TABLE_CRAWLED_CONTENT, cc);
    }

    public ExtractedText addExtractedText(ExtractedText et) {
        return insert(TABLE_EXTRACTED_TEXT, et);
    }

    public CrawlLog addCrawlLog(CrawlLog cl) {
        Map<String, Object> data = ((DbObject) cl).getMap();
        if (!data.containsKey("timeStamp")) {
            data.put("timeStamp", r.now());
        }

        return insert(TABLE_CRAWL_LOG, cl);
    }

    public CrawlLog updateCrawlLog(CrawlLog cl) {
        Map<String, Object> data = ((DbObject) cl).getMap();
        if (!data.containsKey("timeStamp")) {
            data.put("timeStamp", r.now());
        }

        return update(TABLE_CRAWL_LOG, cl.getWarcId(), cl);
    }

    public BrowserScript addBrowserScript(BrowserScript bbs) {
        return insert(TABLE_BROWSER_SCRIPTS, bbs);
    }

    public List<BrowserScript> getBrowserScripts(BrowserScript.Type type) {
        try (Cursor<Map<String, Object>> cursor = executeRequest(r.table(TABLE_BROWSER_SCRIPTS)
                .filter(r.hashMap("type", type.name())));) {

            List<BrowserScript> result = new ArrayList<>();

            for (Map<String, Object> m : cursor) {
                DbObjectFactory.of(BrowserScript.class, m).ifPresent(b -> result.add(b));
            }

            return result;
        }
    }

    @Override
    public CrawlExecutionStatus addExecutionStatus(CrawlExecutionStatus status) {
        return insert(TABLE_EXECUTIONS, status);
    }

    @Override
    public CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status) {
        return update(TABLE_EXECUTIONS, status.getId(), status);
    }

    @Override
    public QueuedUri addQueuedUri(QueuedUri qu) {
        return insert(TABLE_URI_QUEUE, qu);
    }

    @Override
    public QueuedUri updateQueuedUri(QueuedUri qu) {
        return update(TABLE_URI_QUEUE, qu.getId(), qu);
    }

    @Override
    public Screenshot addScreenshot(Screenshot s) {
        return insert(TABLE_SCREENSHOT, s);
    }

    private <T extends DbObject> T insert(String table, T data) {
        Map response = executeRequest(r.table(table)
                .insert(data.getMap())
                .optArg("conflict", "error")
                .optArg("return_changes", "always"));
        data.setMap(((List<Map<String, Map<String, Object>>>) response.get("changes")).get(0).get("new_val"));
        return data;
    }

    private <T extends DbObject> T update(String table, Object key, T data) {
        Map response = executeRequest(r.table(table)
                .get(key)
                .update(data.getMap())
                .optArg("return_changes", "always"));
        data.setMap(((List<Map<String, Map<String, Object>>>) response.get("changes")).get(0).get("new_val"));
        return data;
    }

    private <T extends DbObject> Optional<T> get(String table, Object key, Class<T> type) {
        Map<String, Object> response = executeRequest(r.table(table).get(key));
        return DbObjectFactory.of(type, response);
    }

    private void delete(String table, Object key) {
        executeRequest(r.table(table).get(key).delete());
    }

    public <T> T executeRequest(ReqlExpr qry) {
        synchronized (this) {
            if (!conn.isOpen()) {
                try {
                    conn.connect();
                    createDb();
                } catch (TimeoutException ex) {
                    throw new RuntimeException("Timed out waiting for connection");
                }
            }
        }

        return qry.run(conn);
    }

    @Override
    public void close() {
        conn.close();
    }

}
