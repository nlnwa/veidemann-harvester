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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.SeedListReply;
import no.nb.nna.broprox.commons.OpenTracingWrapper;
import no.nb.nna.broprox.model.ConfigProto.BrowserConfig;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.PolitenessConfig;
import no.nb.nna.broprox.model.ConfigProto.Seed;
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

    public static enum TABLES {
        SYSTEM("system", null),
        CRAWL_LOG("crawl_log", CrawlLog.getDefaultInstance()),
        CRAWLED_CONTENT("crawled_content", CrawledContent.getDefaultInstance()),
        EXTRACTED_TEXT("extracted_text", ExtractedText.getDefaultInstance()),
        BROWSER_SCRIPTS("browser_scripts", BrowserScript.getDefaultInstance()),
        URI_QUEUE("uri_queue", QueuedUri.getDefaultInstance()),
        SCREENSHOT("screenshot", Screenshot.getDefaultInstance()),
        EXECUTIONS("executions", CrawlExecutionStatus.getDefaultInstance()),
        CRAWL_ENTITIES("crawl_entities", CrawlEntity.getDefaultInstance()),
        SEEDS("seeds", Seed.getDefaultInstance()),
        CRAWL_JOBS("crawl_jobs", CrawlJob.getDefaultInstance()),
        CRAWL_CONFIGS("crawl_configs", CrawlConfig.getDefaultInstance()),
        CRAWL_SCHEDULE_CONFIGS("crawl_schedule_configs", CrawlScheduleConfig.getDefaultInstance()),
        BROWSER_CONFIGS("browser_configs", BrowserConfig.getDefaultInstance()),
        POLITENESS_CONFIGS("politeness_configs", PolitenessConfig.getDefaultInstance());

        public final String name;

        public final Message schema;

        private TABLES(String name, Message schema) {
            this.name = name;
            this.schema = schema;
        }

    };

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
                this::executeRequest, r.table(TABLES.CRAWLED_CONTENT.name).get(digest));

        if (response == null) {
            return Optional.empty();
        } else {
            return Optional.of(ProtoUtils.rethinkToProto(response, CrawledContent.class));
        }
    }

    public void deleteCrawledContent(String digest) {
        otw.map("db-addExtractedText",
                this::executeRequest, r.table(TABLES.CRAWLED_CONTENT.name).get(digest).delete());
    }

    @Override
    public CrawledContent addCrawledContent(CrawledContent cc) {
        ensureContainsValue(cc, "digest");
        ensureContainsValue(cc, "warc_id");

        Map rMap = ProtoUtils.protoToRethink(cc);
        Map<String, Object> response = otw.map("db-addCrawledContent",
                this::executeRequest, r.table(TABLES.CRAWLED_CONTENT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        return cc;
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText et) {
        ensureContainsValue(et, "warc_id");
        ensureContainsValue(et, "text");

        Map rMap = ProtoUtils.protoToRethink(et);
        Map<String, Object> response = otw.map("db-addExtractedText",
                this::executeRequest, r.table(TABLES.EXTRACTED_TEXT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        return et;
    }

    @Override
    public CrawlLog addCrawlLog(CrawlLog cl) {
        Map rMap = ProtoUtils.protoToRethink(cl);
        if (!rMap.containsKey("timeStamp")) {
            rMap.put("timeStamp", r.now());
        }

        Map<String, Object> response = otw.map("db-addCrawlLog",
                this::executeRequest, r.table(TABLES.CRAWL_LOG.name)
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
                this::executeRequest, r.table(TABLES.CRAWL_LOG.name)
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
                this::executeRequest, r.table(TABLES.BROWSER_SCRIPTS.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"));

        String key = ((List<String>) response.get("generated_keys")).get(0);
        script = script.toBuilder().setId(key).build();

        return script;
    }

    @Override
    public List<BrowserScript> getBrowserScripts(BrowserScript.Type type) {
        try (Cursor<Map<String, Object>> cursor = otw.map("db-getBrowserScripts",
                this::executeRequest, r.table(TABLES.BROWSER_SCRIPTS.name)
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
                this::executeRequest, r.table(TABLES.EXECUTIONS.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return status.toBuilder().setId(key).build();
    }

    @Override
    public CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status) {
        Map rMap = ProtoUtils.protoToRethink(status);

        Map<String, Object> response = otw.map("db-updateExecutionStatus",
                this::executeRequest, r.table(TABLES.EXECUTIONS.name)
                        .get(status.getId())
                        .update(rMap));

        return status;
    }

    @Override
    public QueuedUri addQueuedUri(QueuedUri qu) {
        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = otw.map("db-addQueudUri",
                this::executeRequest, r.table(TABLES.URI_QUEUE.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return qu.toBuilder().setId(key).build();
    }

    @Override
    public QueuedUri updateQueuedUri(QueuedUri qu) {
        Map rMap = ProtoUtils.protoToRethink(qu);

        Map<String, Object> response = otw.map("db-updateQueuedUri",
                this::executeRequest, r.table(TABLES.URI_QUEUE.name)
                        .get(qu.getId())
                        .update(rMap));

        return qu;
    }

    @Override
    public Screenshot addScreenshot(Screenshot s) {
        Map rMap = ProtoUtils.protoToRethink(s);

        Map<String, Object> response = otw.map("db-addScreenshot",
                this::executeRequest, r.table(TABLES.SCREENSHOT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return s.toBuilder().setId(key).build();
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) {
        return saveConfigMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public Empty deleteCrawlEntity(CrawlEntity entity) {
        return deleteConfigMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(ListRequest request) {
        return listConfigMessages(request, CrawlEntityListReply.newBuilder(), TABLES.CRAWL_ENTITIES);
    }

    @Override
    public SeedListReply listSeeds(ListRequest request) {
        return listConfigMessages(request, SeedListReply.newBuilder(), TABLES.SEEDS);
    }

    @Override
    public Seed saveSeed(Seed seed) {
        return saveConfigMessage(seed, TABLES.SEEDS);
    }

    @Override
    public Empty deleteSeed(Seed seed) {
        return deleteConfigMessage(seed, TABLES.SEEDS);
    }

    @Override
    public CrawlJobListReply listCrawlJobs(ListRequest request) {
        return listConfigMessages(request, CrawlJobListReply.newBuilder(), TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlJob saveCrawlJob(CrawlJob crawlJob) {
        return saveConfigMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public Empty deleteCrawlJob(CrawlJob crawlJob) {
        return deleteConfigMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlConfigListReply listCrawlConfigs(ListRequest request) {
        return listConfigMessages(request, CrawlConfigListReply.newBuilder(), TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) {
        return saveConfigMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public Empty deleteCrawlConfig(CrawlConfig crawlConfig) {
        return deleteConfigMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) {
        return listConfigMessages(request, CrawlScheduleConfigListReply.newBuilder(), TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) {
        return saveConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) {
        return deleteConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    public <T extends Message> T saveConfigMessage(T msg, TABLES table) {
        Map rMap = ProtoUtils.protoToRethink(msg);

        rMap.put("meta", updateMeta((Map) rMap.get("meta"), null));

        return otw.map("db-save" + msg.getClass().getSimpleName(),
                this::executeInsert,
                r.table(table.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                (Class<T>) msg.getClass());
    }

    public <T extends Message> Empty deleteConfigMessage(T entity, TABLES table) {
        Descriptors.FieldDescriptor idDescriptor = entity.getDescriptorForType().findFieldByName("id");

        otw.map("db-delete" + entity.getClass().getSimpleName(),
                this::executeRequest,
                r.table(table.name)
                        .get(entity.getField(idDescriptor))
                        .delete());
        return Empty.getDefaultInstance();
    }

    public <T extends Message> T listConfigMessages(ListRequest request, T.Builder resultBuilder, TABLES table) {
        ReqlExpr qry = r.table(table.name);
        long count = 1;
        switch (request.getQryCase()) {
            case ID:
                qry = ((Table) qry).get(request.getId());
                break;
            case NAME_PREFIX:
                String prefix = request.getNamePrefix().toLowerCase();
                qry = qry.between(prefix, prefix + Character.toString(Character.MAX_VALUE)).optArg("index", "name");
                break;
        }

        if (request.getQryCase() != ListRequest.QryCase.ID) {
            count = otw.map("db-countConfigObjects",
                    this::executeRequest, qry.count());

            qry = qry.orderBy().optArg("index", "name");
            if (request.getPageSize() > 0) {
                qry = qry.skip(request.getPage() * request.getPageSize()).limit(request.getPageSize());
            }
        }

        Descriptors.Descriptor resDescr = resultBuilder.getDescriptorForType();
        Descriptors.FieldDescriptor pageSizeField = resDescr.findFieldByName("page_size");
        Descriptors.FieldDescriptor pageField = resDescr.findFieldByName("page");
        Descriptors.FieldDescriptor countField = resDescr.findFieldByName("count");
        Descriptors.FieldDescriptor valueField = resDescr.findFieldByName("value");

        Object res = otw.map("db-listConfigObjects",
                this::executeRequest, qry);
        resultBuilder
                .setField(pageSizeField, request.getPageSize())
                .setField(pageField, request.getPage())
                .setField(countField, count);
        if (res instanceof Cursor) {
            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                resultBuilder.addRepeatedField(valueField, ProtoUtils.rethinkToProto(entity, CrawlEntity.class));
            }
        } else {
            resultBuilder.addRepeatedField(
                    valueField, ProtoUtils.rethinkToProto((Map<String, Object>) res, CrawlEntity.class));
        }

        return (T) resultBuilder.build();
    }

    private Map updateMeta(Map meta, String user) {
        if (meta == null) {
            meta = r.hashMap();
        }

        if (user == null || user.isEmpty()) {
            user = "anonymous";
        }

        if (!meta.containsKey("created")) {
            meta.put("created", r.now());
            meta.put("createdBy", user);
        }

        meta.put("lastModified", r.now());
        meta.put("lastModifiedBy", user);

        return meta;
    }

    public <T extends Message> T executeInsert(Insert qry, Class<T> type) {
        qry = qry.optArg("return_changes", "always");

        Map<String, Object> response = executeRequest(qry);
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        Map newDoc = changes.get(0).get("new_val");
        return ProtoUtils.rethinkToProto(newDoc, type);
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

        try {
            T result = qry.run(conn);
            if (result instanceof Map
                    && ((Map) result).containsKey("errors")
                    && !((Map) result).get("errors").equals(0L)) {
                throw new DbException((String) ((Map) result).get("first_error"));
            }
            return result;
        } catch (ReqlError e) {
            throw new DbException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        conn.close();
    }

    private void ensureContainsValue(Message msg, String fieldName) {
        if (!msg.getAllFields().keySet().stream().filter(k -> k.getName().equals(fieldName)).findFirst().isPresent()) {
            throw new IllegalArgumentException("The required field '" + fieldName + "' is missing from: '" + msg
                    .getClass().getSimpleName() + "'");
        }
    }

}
