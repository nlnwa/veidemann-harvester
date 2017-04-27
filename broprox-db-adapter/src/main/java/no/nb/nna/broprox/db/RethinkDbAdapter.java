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
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListRequest;
import no.nb.nna.broprox.commons.OpenTracingWrapper;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;

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

    final Connection conn;

    final OpenTracingWrapper otw = new OpenTracingWrapper("dbAdapter", Tags.SPAN_KIND_CLIENT)
            .addTag(Tags.DB_TYPE.getKey(), "rethinkdb")
            .setExtractParentSpanFromGrpcContext(false)
            .setCreateNewGrpcContextForSpan(false)
            .setTraceEnabled(false);

    public RethinkDbAdapter(String dbHost, int dbPort, String dbName) {
        this(r.connection().hostname(dbHost).port(dbPort).db(dbName).connect());
    }

    public RethinkDbAdapter(Connection conn) {
        this.conn = conn;
    }

    @Override
    public Optional<CrawledContent> isDuplicateContent(String digest) {
        Map<String, Object> response = otw.map("db-isDuplicateContent",
                this::executeRequest, r.table(TABLE_CRAWLED_CONTENT).get(digest));

        if (response == null) {
            return Optional.empty();
        } else {
            return Optional.of(ProtoUtils.rethinkToProto(response, CrawledContent.class));
        }
    }

    public void deleteCrawledContent(String digest) {
        otw.map("db-addExtractedText",
                this::executeRequest, r.table(TABLE_CRAWLED_CONTENT).get(digest).delete());
    }

    @Override
    public CrawledContent addCrawledContent(CrawledContent cc) {
        Map rMap = ProtoUtils.protoToRethink(cc);
        Map<String, Object> response = otw.map("db-addCrawledContent",
                this::executeRequest, r.table(TABLE_CRAWLED_CONTENT)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);
        return cc.toBuilder().setDigest(key).build();
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText et) {
        Map rMap = ProtoUtils.protoToRethink(et);
        Map<String, Object> response = otw.map("db-addExtractedText",
                this::executeRequest, r.table(TABLE_EXTRACTED_TEXT)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);
        return et.toBuilder().setWarcId(key).build();
    }

    @Override
    public CrawlLog addCrawlLog(CrawlLog cl) {
        Map rMap = ProtoUtils.protoToRethink(cl);
        if (!rMap.containsKey("timeStamp")) {
            rMap.put("timeStamp", r.now());
        }

        Map<String, Object> response = otw.map("db-addCrawlLog",
                this::executeRequest, r.table(TABLE_CRAWL_LOG)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        cl = cl.toBuilder().setWarcId(key).build();

        return cl;
    }

    @Override
    public CrawlLog updateCrawlLog(CrawlLog cl) {
        Map rMap = ProtoUtils.protoToRethink(cl);
        if (!rMap.containsKey("timeStamp")) {
            rMap.put("timeStamp", r.now());
        }

        Map<String, Object> response = otw.map("db-updateCrawlLog",
                this::executeRequest, r.table(TABLE_CRAWL_LOG)
                        .get(cl.getWarcId())
                        .update(rMap)
                        .optArg("return_changes", "always"));
        cl = ProtoUtils.rethinkToProto(
                ((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val"), CrawlLog.class);

        return cl;
    }

    @Override
    public BrowserScript saveBrowserScript(BrowserScript script) {
        Map rMap = ProtoUtils.protoToRethink(script);

        Map<String, Object> response = otw.map("db-saveBrowserScript",
                this::executeRequest, r.table(TABLE_BROWSER_SCRIPTS)
                        .insert(rMap)
                        .optArg("conflict", "replace"));

        String key = ((List<String>) response.get("generated_keys")).get(0);
        script = script.toBuilder().setId(key).build();

        return script;
    }

    @Override
    public List<BrowserScript> getBrowserScripts(BrowserScript.Type type) {
        try (Cursor<Map<String, Object>> cursor = otw.map("db-getBrowserScripts",
                this::executeRequest, r.table(TABLE_BROWSER_SCRIPTS)
                        .filter(r.hashMap("type", type.name())));) {

            List<BrowserScript> result = new ArrayList<>();

            for (Map<String, Object> m : cursor) {
                result.add(ProtoUtils.rethinkToProto(m, BrowserScript.class));
            }

            return result;
        }
    }

    @Override
    public CrawlExecutionStatus addExecutionStatus(CrawlExecutionStatus status) {
        Map rMap = ProtoUtils.protoToRethink(status);

        Map<String, Object> response = otw.map("db-addExecutionStatus",
                this::executeRequest, r.table(TABLE_EXECUTIONS)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return status.toBuilder().setId(key).build();
    }

    @Override
    public CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status) {
        Map rMap = ProtoUtils.protoToRethink(status);

        Map<String, Object> response = otw.map("db-updateExecutionStatus",
                this::executeRequest, r.table(TABLE_EXECUTIONS)
                        .get(status.getId())
                        .update(rMap));

        return status;
    }

    @Override
    public QueuedUri addQueuedUri(QueuedUri qu) {
        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = otw.map("db-addQueudUri",
                this::executeRequest, r.table(TABLE_URI_QUEUE)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return qu.toBuilder().setId(key).build();
    }

    @Override
    public QueuedUri updateQueuedUri(QueuedUri qu) {
        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = otw.map("db-updateQueuedUri",
                this::executeRequest, r.table(TABLE_URI_QUEUE)
                        .get(qu.getId())
                        .update(rMap));

        return qu;
    }

    @Override
    public Screenshot addScreenshot(Screenshot s) {
        Map rMap = ProtoUtils.protoToRethink(s);

        Map<String, Object> response = otw.map("db-addScreenshot",
                this::executeRequest, r.table(TABLE_SCREENSHOT)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return s.toBuilder().setId(key).build();
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) {
        Map rMap = ProtoUtils.protoToRethink(entity);

        if (!rMap.containsKey("created")) {
            rMap.put("created", r.now());
        }

        Map<String, Object> response = otw.map("db-saveCrawlEntity",
                this::executeRequest, r.table(TABLE_CRAWL_ENTITIES)
                        .insert(rMap)
                        .optArg("conflict", "replace"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return entity.toBuilder().setId(key).build();
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(CrawlEntityListRequest request) {
        ReqlExpr qry = r.table(TABLE_CRAWL_ENTITIES);
        if (!request.getId().isEmpty()) {
            qry = ((Table) qry).get(request.getId());
        }

        Object res = otw.map("db-listCrawlEntities",
                this::executeRequest, qry);

        CrawlEntityListReply.Builder reply = CrawlEntityListReply.newBuilder();
        if (res instanceof Cursor) {
            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                reply.addEntity(ProtoUtils.rethinkToProto(entity, CrawlEntity.class));
            }
        } else {
            reply.addEntity(ProtoUtils.rethinkToProto((Map<String, Object>) res, CrawlEntity.class));
        }

        return reply.build();
    }

    public <T> T executeRequest(ReqlExpr qry) {
        synchronized (this) {
            if (!conn.isOpen()) {
                try {
                    conn.connect();
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
