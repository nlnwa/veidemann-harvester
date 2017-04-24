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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.OpenTracingContextKey;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListRequest;
import no.nb.nna.broprox.model.MessagesProto.BrowserScript;
import no.nb.nna.broprox.model.MessagesProto.CrawlEntity;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;
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

    public static final String TABLE_CRAWL_ENTITIES = "crawl_entities";

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
            r.table(TABLE_URI_QUEUE).indexCreate("executionIds", uri -> uri.g("executionIds")
                    .map(eid -> r.array(eid.g("id"), eid.g("seq")))
            ).optArg("multi", true).run(conn);

            r.table(TABLE_URI_QUEUE).indexWait("surt", "executionIds").run(conn);

            r.tableCreate(TABLE_EXECUTIONS).run(conn);

            r.tableCreate(TABLE_SCREENSHOT).run(conn);

            r.tableCreate(TABLE_CRAWL_ENTITIES).run(conn);

            populateDb();
        }
    }

    private final void populateDb() {
        Yaml yaml = new Yaml();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("browser-scripts/extract-outlinks.yaml")) {
            Map<String, Object> scriptDef = yaml.loadAs(in, Map.class);
            BrowserScript script = ProtoUtils.rethinkToProto(scriptDef, BrowserScript.class);
            saveBrowserScript(script);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Optional<CrawledContent> isDuplicateContent(String digest) {
        Span span = createSpan("db-isDuplicateContent");
        Map<String, Object> response = executeRequest(r.table(TABLE_CRAWLED_CONTENT).get(digest));
        span.finish();
        if (response == null) {
            return Optional.empty();
        } else {
            return Optional.of(ProtoUtils.rethinkToProto(response, CrawledContent.class));
        }
    }

    public void deleteCrawledContent(String digest) {
        delete(TABLE_CRAWLED_CONTENT, digest);
    }

    @Override
    public CrawledContent addCrawledContent(CrawledContent cc) {
        Span span = createSpan("db-addCrawledContent");

        Map rMap = ProtoUtils.protoToRethink(cc);

        Map<String, Object> response = executeRequest(r.table(TABLE_CRAWLED_CONTENT)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return cc.toBuilder().setDigest(key).build();
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText et) {
        Span span = createSpan("db-addExtractedText");

        Map rMap = ProtoUtils.protoToRethink(et);

        Map<String, Object> response = executeRequest(r.table(TABLE_EXTRACTED_TEXT)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return et.toBuilder().setWarcId(key).build();
    }

    @Override
    public CrawlLog addCrawlLog(CrawlLog cl) {
        Span span = createSpan("db-addCrawlLog");

        Map rMap = ProtoUtils.protoToRethink(cl);
        if (!rMap.containsKey("timeStamp")) {
            rMap.put("timeStamp", r.now());
        }

        Map<String, Object> response = executeRequest(r.table(TABLE_CRAWL_LOG)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        cl = cl.toBuilder().setWarcId(key).build();

        span.finish();
        return cl;
    }

    @Override
    public CrawlLog updateCrawlLog(CrawlLog cl) {
        Span span = createSpan("db-updateCrawlLog");

        Map rMap = ProtoUtils.protoToRethink(cl);
        if (!rMap.containsKey("timeStamp")) {
            rMap.put("timeStamp", r.now());
        }

        Map<String, Object> response = executeRequest(r.table(TABLE_CRAWL_LOG)
                .get(cl.getWarcId())
                .update(rMap)
                .optArg("return_changes", "always"));
        cl = ProtoUtils.rethinkToProto(
                ((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val"), CrawlLog.class);

        span.finish();
        return cl;
    }

    @Override
    public BrowserScript saveBrowserScript(BrowserScript script) {
        Span span = createSpan("db-addBrowserScript");

        Map rMap = ProtoUtils.protoToRethink(script);

        Map<String, Object> response = executeRequest(r.table(TABLE_BROWSER_SCRIPTS)
                .insert(rMap)
                .optArg("conflict", "replace"));

        String key = ((List<String>) response.get("generated_keys")).get(0);
        script = script.toBuilder().setId(key).build();

        span.finish();
        return script;
    }

    @Override
    public List<BrowserScript> getBrowserScripts(BrowserScript.Type type) {
        Span span = createSpan("db-getBrowserScripts");
        span.setTag(Tags.DB_STATEMENT.getKey(), "type=" + type);

        try (Cursor<Map<String, Object>> cursor = executeRequest(r.table(TABLE_BROWSER_SCRIPTS)
                .filter(r.hashMap("type", type.name())));) {

            List<BrowserScript> result = new ArrayList<>();

            for (Map<String, Object> m : cursor) {
                result.add(ProtoUtils.rethinkToProto(m, BrowserScript.class));
            }

            span.finish();
            return result;
        }
    }

    @Override
    public CrawlExecutionStatus addExecutionStatus(CrawlExecutionStatus status) {
        Span span = createSpan("db-addExecutionStatus");

        Map rMap = ProtoUtils.protoToRethink(status);

        Map<String, Object> response = executeRequest(r.table(TABLE_EXECUTIONS)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return status.toBuilder().setId(key).build();
    }

    @Override
    public CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status) {
        Span span = createSpan("db-updateQueuedUri");

        Map rMap = ProtoUtils.protoToRethink(status);

        Map<String, Object> response = executeRequest(r.table(TABLE_EXECUTIONS)
                .get(status.getId())
                .update(rMap));

        span.finish();
        return status;
    }

    @Override
    public QueuedUri addQueuedUri(QueuedUri qu) {
        Span span = createSpan("db-addQueuedUri");

        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = executeRequest(r.table(TABLE_URI_QUEUE)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return qu.toBuilder().setId(key).build();
    }

    @Override
    public QueuedUri updateQueuedUri(QueuedUri qu) {
        Span span = createSpan("db-updateQueuedUri");

        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = executeRequest(r.table(TABLE_URI_QUEUE)
                .get(qu.getId())
                .update(rMap));

        span.finish();
        return qu;
    }

    @Override
    public Screenshot addScreenshot(Screenshot s) {
        Span span = createSpan("db-addQueuedUri");

        Map rMap = ProtoUtils.protoToRethink(s);

        Map<String, Object> response = executeRequest(r.table(TABLE_SCREENSHOT)
                .insert(rMap)
                .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return s.toBuilder().setId(key).build();
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) {
        Span span = createSpan("db-saveCrawlEntity");

        Map rMap = ProtoUtils.protoToRethink(entity);

        Map<String, Object> response = executeRequest(r.table(TABLE_CRAWL_ENTITIES)
                .insert(rMap)
                .optArg("conflict", "replace"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        span.finish();
        return entity.toBuilder().setId(key).build();
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(CrawlEntityListRequest request) {
        Span span = createSpan("db-listCrawlEntities");

        ReqlExpr qry = r.table(TABLE_CRAWL_ENTITIES);
        if (request != null) {
            if (!request.getId().isEmpty()) {
                qry = ((Table) qry).get(request.getId());
            }
        }
        Object res = executeRequest(qry);

        CrawlEntityListReply.Builder reply = CrawlEntityListReply.newBuilder();
        if (res instanceof Cursor) {
            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                reply.addEntity(ProtoUtils.rethinkToProto(entity, CrawlEntity.class));
            }
        } else {
            reply.addEntity(ProtoUtils.rethinkToProto((Map<String, Object>) res, CrawlEntity.class));
        }

        span.finish();
        return reply.build();
    }

    /**
     * Create a new OpenTracing span.
     *
     * @param operationName
     * @return the created span
     */
    private Span createSpan(String operationName) {
        Span parentSpan = OpenTracingContextKey.activeSpan();
        Tracer.SpanBuilder spanBuilder = GlobalTracer.get()
                .buildSpan(operationName)
                .withTag(Tags.DB_TYPE.getKey(), "rethinkdb")
                .withTag(Tags.COMPONENT.getKey(), "dbAdapter")
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);
        if (parentSpan != null) {
            spanBuilder.asChildOf(parentSpan);
        }
        return spanBuilder.start();
    }

//    private <T extends DbObject> T insert(String table, T data) {
//        Span span = createSpan("db-insert-" + table);
//        Map response = executeRequest(r.table(table)
//                .insert(data.getMap())
//                .optArg("conflict", "error")
//                .optArg("return_changes", "always"));
//        data.setMap(((List<Map<String, Map<String, Object>>>) response.get("changes")).get(0).get("new_val"));
//        span.finish();
//        return data;
//    }
//
//    private <T extends DbObject> T update(String table, Object key, T data) {
//        Span span = createSpan("db-update-" + table);
//        Map response = executeRequest(r.table(table)
//                .get(key)
//                .update(data.getMap())
//                .optArg("return_changes", "always"));
//        data.setMap(((List<Map<String, Map<String, Object>>>) response.get("changes")).get(0).get("new_val"));
//        span.finish();
//        return data;
//    }
//
//    private <T extends DbObject> Optional<T> get(String table, Object key, Class<T> type) {
//        Span span = createSpan("db-get-" + table);
//        Map<String, Object> response = executeRequest(r.table(table).get(key));
//        span.finish();
//        return DbObjectFactory.of(type, response);
//    }

    private void delete(String table, Object key) {
        Span span = createSpan("db-delete-" + table);
        executeRequest(r.table(table).get(key).delete());
        span.finish();
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
