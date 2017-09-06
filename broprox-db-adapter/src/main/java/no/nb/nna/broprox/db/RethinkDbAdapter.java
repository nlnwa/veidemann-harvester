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

import java.time.OffsetDateTime;
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
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.net.Connection;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.model.ConfigProto.BrowserConfig;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.LogLevels;
import no.nb.nna.broprox.model.ConfigProto.PolitenessConfig;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.ConfigProto.Selector;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlHostGroup;
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
        POLITENESS_CONFIGS("politeness_configs", PolitenessConfig.getDefaultInstance()),
        CRAWL_HOST_GROUP("crawl_host_group", CrawlHostGroup.getDefaultInstance()),
        CRAWL_HOST_GROUP_CONFIGS("crawl_host_group_configs", CrawlHostGroupConfig.getDefaultInstance());

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
    public Optional<CrawledContent> hasCrawledContent(CrawledContent cc) {
        ensureContainsValue(cc, "digest");
        ensureContainsValue(cc, "warc_id");

        Map rMap = ProtoUtils.protoToRethink(cc);
        Map<String, Object> response = otw.map("db-hasCrawledContent",
                this::executeRequest, r.table(TABLES.CRAWLED_CONTENT.name)
                        .insert(rMap)
                        .optArg("conflict", (id, old_doc, new_doc) -> old_doc)
                        .optArg("return_changes", "always")
                        .g("changes").nth(0).g("old_val"));

        if (response == null) {
            return Optional.empty();
        } else {
            return Optional.of(ProtoUtils.rethinkToProto(response, CrawledContent.class));
        }
    }

    public void deleteCrawledContent(String digest) {
        otw.map("db-deleteCrawledContent",
                this::executeRequest, r.table(TABLES.CRAWLED_CONTENT.name).get(digest).delete());
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
        return saveConfigMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public Empty deleteBrowserScript(BrowserScript script) {
        checkDependencies(script, TABLES.BROWSER_CONFIGS, BrowserConfig.getDefaultInstance(), "script_id");
        return deleteConfigMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public BrowserScriptListReply listBrowserScripts(BrowserScriptListRequest request) {
        BrowserScriptListRequestQueryBuilder queryBuilder = new BrowserScriptListRequestQueryBuilder(request);
        return queryBuilder.executeList(otw, this).build();
    }

    @Override
    public CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) {
        return saveConfigMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) {
        return deleteConfigMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_HOST_GROUP_CONFIGS);
        return queryBuilder.executeList(otw, this, CrawlHostGroupConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlHostGroup getOrCreateCrawlHostGroup(String crawlHostGroupId, String politenessId) {
        List key = r.array(crawlHostGroupId, politenessId);
        Map<String, Object> response = otw.map("db-getOrCreateCrawlHostGroup",
                this::executeRequest, r.table(TABLES.CRAWL_HOST_GROUP.name)
                        .insert(r.hashMap("id", key).with("nextFetchTime", r.now()).with("busy", false))
                        .optArg("conflict", (id, oldDoc, newDoc) -> oldDoc)
                        .optArg("return_changes", "always"));

        Map resultDoc = ((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val");

        CrawlHostGroup chg = CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .build();

        return chg;
    }

    @Override
    public Optional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() {
        Map<String, Object> response = otw.map("db-borrowFirstReadyCrawlHostGroup",
                this::executeRequest, r.table(TABLES.CRAWL_HOST_GROUP.name)
                        .orderBy().optArg("index", "nextFetchTime")
                        .between(r.minval(), r.now()).optArg("right_bound", "closed")
                        .filter(r.hashMap("busy", false))
                        .limit(1)
                        .update(r.hashMap("busy", true))
                        .optArg("return_changes", "always"));

        long replaced = (long) response.get("replaced");
        long unchanged = (long) response.get("unchanged");

        if (unchanged == 1L) {
            // Another thread picked the same CrawlHostGroup during the query (the query is not atomic)
            // Retry the request.
            return borrowFirstReadyCrawlHostGroup();
        }

        if (replaced == 0L) {
            // No CrawlHostGroup was ready
            return Optional.empty();
        }

        Map resultDoc = ((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val");

        CrawlHostGroup chg = CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .build();

        return Optional.of(chg);
    }

    @Override
    public CrawlHostGroup releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) {
        List key = r.array(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId());
        double nextFetchDelayS = nextFetchDelayMs / 1000.0;

        Map<String, Object> response = otw.map("db-releaseCrawlHostGroup",
                this::executeRequest, r.table(TABLES.CRAWL_HOST_GROUP.name)
                        .get(key)
                        .update(r.hashMap("busy", false).with("nextFetchTime", r.now().add(nextFetchDelayS)))
                        .optArg("return_changes", "always"));

        Map resultDoc = ((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val");

        CrawlHostGroup chg = CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .build();

        return chg;
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
        checkDependencies(entity, TABLES.SEEDS, Seed.getDefaultInstance(), "entity_id");
        return deleteConfigMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_ENTITIES);
        return queryBuilder.executeList(otw, this, CrawlEntityListReply.newBuilder()).build();
    }

    @Override
    public SeedListReply listSeeds(SeedListRequest request) {
        SeedListRequestQueryBuilder queryBuilder = new SeedListRequestQueryBuilder(request);
        return queryBuilder.executeList(otw, this).build();
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
    public CrawlJobListReply listCrawlJobs(CrawlJobListRequest request) {
        CrawlJobListRequestQueryBuilder queryBuilder = new CrawlJobListRequestQueryBuilder(request);
        return queryBuilder.executeList(otw, this).build();
    }

    @Override
    public CrawlJob saveCrawlJob(CrawlJob crawlJob) {
        CrawlJob.Builder builder = crawlJob.toBuilder();

        switch (crawlJob.getSceduleConfigOrIdCase()) {
            case SCHEDULE:
                CrawlScheduleConfig schedule = crawlJob.getSchedule();
                schedule = saveCrawlScheduleConfig(schedule);
                builder.setScheduleId(schedule.getId());
                break;
            case SCHEDULE_SELECTOR:
                Selector selector = crawlJob.getScheduleSelector();
                CrawlScheduleConfigListReply res = listCrawlScheduleConfigs(
                        ListRequest.newBuilder().setSelector(selector).build());
                if (res.getCount() == 1) {
                    builder.setScheduleId(res.getValue(0).getId());
                } else {
                    throw new IllegalArgumentException("Schedule selector should return exactly one match, was: "
                            + res.getCount());
                }
                break;
        }

        switch (crawlJob.getCrawlConfigOrIdCase()) {
            case CRAWL_CONFIG:
                CrawlConfig crawlConfig = crawlJob.getCrawlConfig();
                crawlConfig = saveCrawlConfig(crawlConfig);
                builder.setCrawlConfigId(crawlConfig.getId());
                break;
            case CRAWL_CONFIG_SELECTOR:
                Selector selector = crawlJob.getCrawlConfigSelector();
                CrawlConfigListReply res = listCrawlConfigs(ListRequest.newBuilder().setSelector(selector).build());
                if (res.getCount() == 1) {
                    builder.setCrawlConfigId(res.getValue(0).getId());
                } else {
                    throw new IllegalArgumentException("CrawlConfig selector should return exactly one match, was: "
                            + res.getCount());
                }
                break;
            case CRAWLCONFIGORID_NOT_SET:
                throw new IllegalArgumentException("A crawl config is required for crawl jobs");
        }

        return saveConfigMessage(builder.build(), TABLES.CRAWL_JOBS);
    }

    @Override
    public Empty deleteCrawlJob(CrawlJob crawlJob) {
        checkDependencies(crawlJob, TABLES.SEEDS, Seed.getDefaultInstance(), "job_id");
        return deleteConfigMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlConfigListReply listCrawlConfigs(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_CONFIGS);
        return queryBuilder.executeList(otw, this, CrawlConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) {
        CrawlConfig.Builder builder = crawlConfig.toBuilder();

        switch (crawlConfig.getBrowserConfigOrIdCase()) {
            case BROWSER_CONFIG:
                BrowserConfig browserConfig = crawlConfig.getBrowserConfig();
                browserConfig = saveBrowserConfig(browserConfig);
                builder.setBrowserConfigId(browserConfig.getId());
                break;
            case BROWSER_CONFIG_SELECTOR:
                Selector selector = crawlConfig.getBrowserConfigSelector();
                BrowserConfigListReply res = listBrowserConfigs(ListRequest.newBuilder().setSelector(selector).build());
                if (res.getCount() == 1) {
                    builder.setBrowserConfigId(res.getValue(0).getId());
                } else {
                    throw new IllegalArgumentException("BrowserConfig selector should return exactly one match, was: "
                            + res.getCount());
                }
                break;
        }

        switch (crawlConfig.getPolitenessOrIdCase()) {
            case POLITENESS:
                PolitenessConfig politenessConfig = crawlConfig.getPoliteness();
                politenessConfig = savePolitenessConfig(politenessConfig);
                builder.setPolitenessId(politenessConfig.getId());
                break;
            case POLITENESS_SELECTOR:
                Selector selector = crawlConfig.getPolitenessSelector();
                PolitenessConfigListReply res = listPolitenessConfigs(
                        ListRequest.newBuilder().setSelector(selector).build());
                if (res.getCount() == 1) {
                    builder.setPolitenessId(res.getValue(0).getId());
                } else {
                    throw new IllegalArgumentException("Politeness selector should return exactly one match, was: "
                            + res.getCount());
                }
                break;
        }

        return saveConfigMessage(builder.build(), TABLES.CRAWL_CONFIGS);
    }

    @Override
    public Empty deleteCrawlConfig(CrawlConfig crawlConfig) {
        checkDependencies(crawlConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "crawl_config_id");
        return deleteConfigMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_SCHEDULE_CONFIGS);
        return queryBuilder.executeList(otw, this, CrawlScheduleConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) {
        return saveConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) {
        checkDependencies(crawlScheduleConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "schedule_id");
        return deleteConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public PolitenessConfigListReply listPolitenessConfigs(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.POLITENESS_CONFIGS);
        return queryBuilder.executeList(otw, this, PolitenessConfigListReply.newBuilder()).build();
    }

    @Override
    public PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) {
        return saveConfigMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public Empty deletePolitenessConfig(PolitenessConfig politenessConfig) {
        checkDependencies(politenessConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "politeness_id");
        return deleteConfigMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public BrowserConfigListReply listBrowserConfigs(ListRequest request) {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.BROWSER_CONFIGS);
        return queryBuilder.executeList(otw, this, BrowserConfigListReply.newBuilder()).build();
    }

    @Override
    public BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) {
        return saveConfigMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public Empty deleteBrowserConfig(BrowserConfig browserConfig) {
        checkDependencies(browserConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "browser_config_id");
        return deleteConfigMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public LogLevels getLogConfig() {
        Map<String, Object> response = executeRequest(r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                .get("log_levels")
                .pluck("logLevel"));

        return ProtoUtils.rethinkToProto(response, LogLevels.class);
    }

    @Override
    public LogLevels saveLogConfig(LogLevels logLevels) {
        Map<String, Object> doc = ProtoUtils.protoToRethink(logLevels);
        doc.put("id", "log_levels");
//        Map<String, Object> response = executeRequest(r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
        return executeInsert(r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                .insert(doc)
                .optArg("conflict", "replace"), LogLevels.class);
//                .optArg("returnChanges", "always"));

//        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");
//
//        Map newDoc = changes.get(0).get("new_val").;
//        return ProtoUtils.rethinkToProto(newDoc, type);
    }

    public <T extends Message> T saveConfigMessage(T msg, TABLES table) {
        Map rMap = ProtoUtils.protoToRethink(msg);

        // Check that name is set if this is a new object
        if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
            throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                    + " object, but meta.name is not set.");
        }

        rMap.put("meta", updateMeta((Map) rMap.get("meta"), null));

        return otw.map("db-save" + msg.getClass().getSimpleName(),
                this::executeInsert,
                r.table(table.name)
                        .insert(rMap)
                        // A rethink function which copies created and createby from old doc,
                        // and copies name if not existent in new doc
                        .optArg("conflict", (id, old_doc, new_doc) -> new_doc.merge(
                        r.hashMap("meta", r.hashMap()
                                .with("name", r.branch(new_doc.g("meta").hasFields("name"),
                                        new_doc.g("meta").g("name"), old_doc.g("meta").g("name")))
                                .with("created", old_doc.g("meta").g("created"))
                                .with("createdBy", old_doc.g("meta").g("createdBy"))
                        ))),
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
        if (!meta.containsKey("lastModifiedBy")) {
            meta.put("lastModifiedBy", user);
        }

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

    /**
     * Check references to Config object.
     *
     * @param messageToCheck the config message which other objects might refer.
     * @param dependentTable the table containing objects which might have a dependency to the object to check.
     * @param dependentMessage the object type in the table containing objects which might have a dependency to the
     * object to check.
     * @param dependentFieldName the field name in the dependent message which might contain reference to the id field
     * in the object to check.
     * @throws IllegalStateException if there are dependencies.
     */
    private void checkDependencies(Message messageToCheck, TABLES dependentTable,
            Message dependentMessage, String dependentFieldName) {

        Descriptors.FieldDescriptor messageIdField = messageToCheck.getDescriptorForType().findFieldByName("id");
        Descriptors.FieldDescriptor dependentField = dependentMessage.getDescriptorForType()
                .findFieldByName(dependentFieldName);

        ListRequestQueryBuilder qry = new ListRequestQueryBuilder(ListRequest.getDefaultInstance(), dependentTable);
        if (dependentField.isRepeated()) {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).contains(messageToCheck.getField(messageIdField)));
        } else {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).eq(messageToCheck.getField(messageIdField)));
        }

        long dependencyCount = qry.executeCount(otw, this);
        if (dependencyCount > 0) {
            throw new IllegalStateException("Can't delete " + messageToCheck.getClass().getSimpleName()
                    + ", there are " + dependencyCount + " " + dependentMessage.getClass().getSimpleName()
                    + "(s) referring it");
        }
    }

    private void ensureContainsValue(Message msg, String fieldName) {
        if (!msg.getAllFields().keySet().stream().filter(k -> k.getName().equals(fieldName)).findFirst().isPresent()) {
            throw new IllegalArgumentException("The required field '" + fieldName + "' is missing from: '" + msg
                    .getClass().getSimpleName() + "'");
        }
    }

}
