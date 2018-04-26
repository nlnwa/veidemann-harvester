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
package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Update;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus.Builder;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.api.StatusProto;
import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * An implementation of DbAdapter for RethinkDb.
 */
public class RethinkDbAdapter implements DbAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbAdapter.class);

    public static enum TABLES {
        SYSTEM("system", null),
        CRAWL_LOG("crawl_log", CrawlLog.getDefaultInstance()),
        PAGE_LOG("page_log", PageLog.getDefaultInstance()),
        CRAWLED_CONTENT("crawled_content", CrawledContent.getDefaultInstance()),
        EXTRACTED_TEXT("extracted_text", ExtractedText.getDefaultInstance()),
        URI_QUEUE("uri_queue", QueuedUri.getDefaultInstance()),
        SCREENSHOT("screenshot", Screenshot.getDefaultInstance()),
        EXECUTIONS("executions", CrawlExecutionStatus.getDefaultInstance()),
        JOB_EXECUTIONS("job_executions", JobExecutionStatus.getDefaultInstance()),
        CRAWL_HOST_GROUP("crawl_host_group", CrawlHostGroup.getDefaultInstance()),
        ALREADY_CRAWLED_CACHE("already_crawled_cache", null),
        BROWSER_SCRIPTS("config_browser_scripts", BrowserScript.getDefaultInstance()),
        CRAWL_ENTITIES("config_crawl_entities", CrawlEntity.getDefaultInstance()),
        SEEDS("config_seeds", Seed.getDefaultInstance()),
        CRAWL_JOBS("config_crawl_jobs", CrawlJob.getDefaultInstance()),
        CRAWL_CONFIGS("config_crawl_configs", CrawlConfig.getDefaultInstance()),
        CRAWL_SCHEDULE_CONFIGS("config_crawl_schedule_configs", CrawlScheduleConfig.getDefaultInstance()),
        BROWSER_CONFIGS("config_browser_configs", BrowserConfig.getDefaultInstance()),
        POLITENESS_CONFIGS("config_politeness_configs", PolitenessConfig.getDefaultInstance()),
        CRAWL_HOST_GROUP_CONFIGS("config_crawl_host_group_configs", CrawlHostGroupConfig.getDefaultInstance()),
        ROLE_MAPPINGS("config_role_mappings", RoleMapping.getDefaultInstance());

        public final String name;

        public final Message schema;

        private TABLES(String name, Message schema) {
            this.name = name;
            this.schema = schema;
        }

    }

    static final RethinkDB r = RethinkDB.r;

    public RethinkDbAdapter() {
    }

    @Override
    public Optional<CrawledContent> hasCrawledContent(CrawledContent cc) throws DbException {
        ensureContainsValue(cc, "digest");
        ensureContainsValue(cc, "warc_id");

        Map rMap = ProtoUtils.protoToRethink(cc);
        Map<String, Object> response = executeRequest("db-hasCrawledContent",
                r.table(TABLES.CRAWLED_CONTENT.name)
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

    public void deleteCrawledContent(String digest) throws DbException {
        executeRequest("db-deleteCrawledContent", r.table(TABLES.CRAWLED_CONTENT.name).get(digest).delete());
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText et) throws DbException {
        ensureContainsValue(et, "warc_id");
        ensureContainsValue(et, "text");

        Map rMap = ProtoUtils.protoToRethink(et);
        Map<String, Object> response = executeRequest("db-addExtractedText",
                r.table(TABLES.EXTRACTED_TEXT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        return et;
    }

    @Override
    public CrawlLog saveCrawlLog(CrawlLog cl) throws DbException {
        if (!"text/dns".equals(cl.getContentType())) {
            if (cl.getJobExecutionId().isEmpty()) {
                LOG.error("Missing JobExecutionId in CrawlLog: {}", cl, new IllegalStateException());
            }
            if (cl.getExecutionId().isEmpty()) {
                LOG.error("Missing ExecutionId in CrawlLog: {}", cl, new IllegalStateException());
            }
        }
        if (!cl.hasTimeStamp()) {
            cl = cl.toBuilder().setTimeStamp(ProtoUtils.getNowTs()).build();
        }
        return saveMessage(cl, TABLES.CRAWL_LOG);
    }

    @Override
    public CrawlLogListReply listCrawlLogs(CrawlLogListRequest request) throws DbException {
        CrawlLogListRequestQueryBuilder queryBuilder = new CrawlLogListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public PageLog savePageLog(PageLog pageLog) throws DbException {
        return saveMessage(pageLog, TABLES.PAGE_LOG);
    }

    @Override
    public PageLogListReply listPageLogs(PageLogListRequest request) throws DbException {
        PageLogListRequestQueryBuilder queryBuilder = new PageLogListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public BrowserScript getBrowserScript(GetRequest req) throws DbException {
        return getMessage(req, BrowserScript.class, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public BrowserScript saveBrowserScript(BrowserScript script) throws DbException {
        return saveMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public Empty deleteBrowserScript(BrowserScript script) throws DbException {
        checkDependencies(script, TABLES.BROWSER_CONFIGS, BrowserConfig.getDefaultInstance(), "script_id");
        return deleteConfigMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.BROWSER_SCRIPTS);
        return queryBuilder.executeList(this, BrowserScriptListReply.newBuilder()).build();
    }

    @Override
    public CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlHostGroupConfig.class, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        return saveMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        return deleteConfigMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_HOST_GROUP_CONFIGS);
        return queryBuilder.executeList(this, CrawlHostGroupConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlHostGroup addToCrawlHostGroup(QueuedUri qUri) throws DbException {
        String crawlHostGroupId = qUri.getCrawlHostGroupId();
        String politenessId = qUri.getPolitenessId();
        Objects.requireNonNull(crawlHostGroupId, "CrawlHostGroupId cannot be null");
        Objects.requireNonNull(politenessId, "PolitenessId cannot be null");

        List key = r.array(crawlHostGroupId, politenessId);
        Map<String, Object> response = executeRequest("db-getOrCreateCrawlHostGroup",
                r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                        .get(key)
                        .replace(d -> r.branch(r.not(d),
                                r.hashMap("id", key)
                                        .with("nextFetchTime", r.now())
                                        .with("busy", false)
                                        .with("queuedUriCount", 1L),
                                d.merge(r.hashMap("queuedUriCount", d.g("queuedUriCount").add(1)))))
                        .optArg("return_changes", "always")
                        .optArg("durability", "hard")
        );

        return buildCrawlHostGroup(((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val"));
    }

    @Override
    public FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() throws DbException {
        OffsetDateTime nextReadyTime = null;

        Cursor<Map<String, Object>> response = executeRequest("db-borrowFirstReadyCrawlHostGroup",
                r.table(TABLES.CRAWL_HOST_GROUP.name)
                        .orderBy().optArg("index", "nextFetchTime")
                        .between(r.minval(), r.now()).optArg("right_bound", "closed")
                        .filter(r.hashMap("busy", false))
        );

        for (Map<String, Object> chgDoc : response) {
            Object chgId = chgDoc.get("id");

            Map<String, Object> borrowResponse = executeRequest("db-borrowFirstReadyCrawlHostGroup",
                    r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(chgId)
                            .replace(d ->
                                    r.branch(
                                            // Another service has deleted this CrawlHostGroup, return null (unchanged)
                                            r.not(d), null,
                                            // The uri queue for this CrawlHostGroup is empty, delete it by returning null
                                            d.g("busy").eq(false).and(d.g("queuedUriCount").eq(0L)), null,
                                            // This is the one we want, set busy to false and return it
                                            d.g("busy").eq(false), d.merge(r.hashMap("busy", true)),
                                            // The CrawlHostGroup is busy, return it unchanged
                                            d
                                    ))
                            .optArg("return_changes", true)
                            .optArg("durability", "hard")
            );

            long replaced = (long) borrowResponse.get("replaced");
            if (replaced == 1L) {
                CrawlHostGroup chg = buildCrawlHostGroup(((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val"));
                return FutureOptional.of(chg);
            } else {
                if (nextReadyTime == null) {
                    nextReadyTime = (OffsetDateTime) chgDoc.get("nextFetchTime");
                }
            }
        }

        if (nextReadyTime == null) {
            return FutureOptional.empty();
        } else {
            return FutureOptional.emptyUntil(nextReadyTime);
        }
    }

    @Override
    public CrawlHostGroup releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs, boolean qUriProcessed) throws DbException {
        List key = r.array(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId());
        double nextFetchDelayS = nextFetchDelayMs / 1000.0;

        Map<String, Object> response = null;
        Update qry;
        if (qUriProcessed) {
            qry = r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                    .get(key)
                    .update(chg ->
                            r.hashMap("busy", false)
                                    .with("nextFetchTime", r.now().add(nextFetchDelayS))
                                    .with("queuedUriCount", chg.g("queuedUriCount").sub(1L)))
                    .optArg("return_changes", "always")
                    .optArg("durability", "hard");
        } else {
            qry = r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                    .get(key)
                    .update(chg ->
                            r.hashMap("busy", false)
                                    .with("nextFetchTime", r.now().add(nextFetchDelayS)))
                    .optArg("return_changes", "always")
                    .optArg("durability", "hard");
        }
        response = executeRequest("db-releaseCrawlHostGroup", qry);

        return buildCrawlHostGroup(((List<Map<String, Map>>) response.get("changes")).get(0).get("new_val"));
    }

    private CrawlHostGroup buildCrawlHostGroup(Map<String, Object> resultDoc) {
        return CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .setQueuedUriCount((long) resultDoc.get("queuedUriCount"))
                .build();
    }

    @Override
    public JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(JobExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setStartTime(ProtoUtils.getNowTs())
                .setState(JobExecutionStatus.State.RUNNING));

        return executeInsert("db-saveJobExecutionStatus",
                r.table(TABLES.JOB_EXECUTIONS.name)
                        .insert(rMap)
                        .optArg("conflict", (id, oldDoc, newDoc) -> r.branch(
                                oldDoc.hasFields("endTime"),
                                newDoc.merge(
                                        r.hashMap("state", oldDoc.g("state")).with("endTime", oldDoc.g("endTime"))
                                ),
                                newDoc
                        )),
                JobExecutionStatus.class);
    }

    @Override
    public JobExecutionStatus getJobExecutionStatus(String jobExecutionId) throws DbException {
        JobExecutionStatus jes = ProtoUtils.rethinkToProto(executeRequest("db-getJobExecutionStatus",
                r.table(TABLES.JOB_EXECUTIONS.name)
                        .get(jobExecutionId)
        ), JobExecutionStatus.class);

        if (!jes.hasEndTime()) {
            LOG.debug("JobExecution '{}' is still running. Aggregating stats snapshot", jobExecutionId);
            Map sums = summarizeJobExecutionStats(jes.getId());

            JobExecutionStatus.Builder jesBuilder = jes.toBuilder()
                    .setDocumentsCrawled((long) sums.get("documentsCrawled"))
                    .setDocumentsDenied((long) sums.get("documentsDenied"))
                    .setDocumentsFailed((long) sums.get("documentsFailed"))
                    .setDocumentsOutOfScope((long) sums.get("documentsOutOfScope"))
                    .setDocumentsRetried((long) sums.get("documentsRetried"))
                    .setUrisCrawled((long) sums.get("urisCrawled"))
                    .setBytesCrawled((long) sums.get("bytesCrawled"));

            for (State s : State.values()) {
                jesBuilder.putExecutionsState(s.name(), ((Long) sums.get(s.name())).intValue());
            }

            jes = jesBuilder.build();
        }

        return jes;
    }

    @Override
    public JobExecutionsListReply listJobExecutionStatus(ListJobExecutionsRequest request) throws DbException {
        JobExecutionsListRequestQueryBuilder queryBuilder = new JobExecutionsListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId) throws DbException {
        JobExecutionStatus result = executeUpdate("db-setJobExecutionStateAborted",
                r.table(TABLES.JOB_EXECUTIONS.name)
                        .get(jobExecutionId)
                        .update(
                                doc -> r.branch(
                                        doc.hasFields("endTime"),
                                        r.hashMap(),
                                        r.hashMap("state", State.ABORTED_MANUAL.name()).with("endTime", r.now()))
                        ),
                JobExecutionStatus.class);

        // Set all Crawl Executions which are part of this Job Execution to aborted
        ListReplyWalker<ListExecutionsRequest, CrawlExecutionStatus> walker = new ListReplyWalker<>();
        ListExecutionsRequest.Builder executionsRequest = ListExecutionsRequest.newBuilder().setJobExecutionId(jobExecutionId);

        walker.walk(executionsRequest,
                req -> listExecutionStatus(req),
                exe -> {
                    try {
                        setExecutionStateAborted(exe.getId());
                    } catch (DbException e) {
                        LOG.error("Failed to abort Crawl Execution {}", exe.getId(), e);
                    }
                });

        return result;
    }

    @Override
    public CrawlExecutionStatus saveExecutionStatus(CrawlExecutionStatus status) throws DbException {
        if (status.getJobExecutionId().isEmpty()) {
            LOG.error("Missing JobExecutionId in CrawlExecutionStatus: {}", status, new IllegalStateException());
        }

        Map rMap = ProtoUtils.protoToRethink(status);

        // Update the CrawlExecutionStatus, but keep the endTime if it is set
        Insert qry = r.table(TABLES.EXECUTIONS.name)
                .insert(rMap)
                .optArg("conflict", (id, oldDoc, newDoc) -> r.branch(
                        oldDoc.hasFields("endTime"),
                        newDoc.merge(
                                r.hashMap("state", oldDoc.g("state")).with("endTime", oldDoc.g("endTime"))
                        ),
                        newDoc
                ));

        // Return both the new and the old values
        qry = qry.optArg("return_changes", "always");
        Map<String, Object> response = executeRequest("db-saveExecutionStatus", qry);
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        // Check if this update was setting the end time
        boolean wasNotEnded = changes.get(0).get("old_val") == null || changes.get(0).get("old_val").get("endTime") == null;
        CrawlExecutionStatus newDoc = ProtoUtils.rethinkToProto(changes.get(0).get("new_val"), CrawlExecutionStatus.class);
        if (wasNotEnded && newDoc.hasEndTime()) {
            // Get a count of still running CrawlExecutions for this execution's JobExecution
            Long notEndedCount = executeRequest("db-updateJobExecution",
                    r.table(TABLES.EXECUTIONS.name)
                            .getAll(newDoc.getJobExecutionId()).optArg("index", "jobExecutionId")
                            .filter(row -> row.g("state").match("UNDEFINED|CREATED|FETCHING|SLEEPING"))
                            .group("state").count()
                            .ungroup().sum("reduction")
            );

            // If all CrawlExecutions are done for this JobExectuion, update the JobExecution with end statistics
            if (notEndedCount == 0) {
                LOG.debug("JobExecution '{}' finished, saving stats", newDoc.getJobExecutionId());

                // Fetch the JobExecutionStatus object this CrawlExecution is part of
                JobExecutionStatus jes = ProtoUtils.rethinkToProto(executeRequest("db-getJobExecutionStatus",
                        r.table(TABLES.JOB_EXECUTIONS.name)
                                .get(newDoc.getJobExecutionId())
                ), JobExecutionStatus.class);

                // Set JobExecution's status to FINISHED if it wasn't already aborted
                JobExecutionStatus.State state = jes.getState() == JobExecutionStatus.State.ABORTED_MANUAL ? jes.getState() : JobExecutionStatus.State.FINISHED;

                // Update aggregated statistics
                Map sums = summarizeJobExecutionStats(newDoc.getJobExecutionId());
                Builder jesBuilder = jes.toBuilder()
                        .setState(state)
                        .setEndTime(ProtoUtils.getNowTs())
                        .setDocumentsCrawled((long) sums.get("documentsCrawled"))
                        .setDocumentsDenied((long) sums.get("documentsDenied"))
                        .setDocumentsFailed((long) sums.get("documentsFailed"))
                        .setDocumentsOutOfScope((long) sums.get("documentsOutOfScope"))
                        .setDocumentsRetried((long) sums.get("documentsRetried"))
                        .setUrisCrawled((long) sums.get("urisCrawled"))
                        .setBytesCrawled((long) sums.get("bytesCrawled"));

                for (State s : State.values()) {
                    jesBuilder.putExecutionsState(s.name(), ((Long) sums.get(s.name())).intValue());
                }

                executeRequest("db-saveJobExecutionStatus",
                        r.table(TABLES.JOB_EXECUTIONS.name).get(jesBuilder.getId()).update(ProtoUtils.protoToRethink(jesBuilder)));
            }
        }

        return newDoc;
    }

    @Override
    public CrawlExecutionStatus getExecutionStatus(String executionId) throws DbException {
        Map<String, Object> response = executeRequest("db-getExecutionStatus",
                r.table(TABLES.EXECUTIONS.name)
                        .get(executionId)
        );

        return ProtoUtils.rethinkToProto(response, CrawlExecutionStatus.class);
    }

    @Override
    public ExecutionsListReply listExecutionStatus(ListExecutionsRequest request) throws DbException {
        CrawlExecutionsListRequestQueryBuilder queryBuilder = new CrawlExecutionsListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public CrawlExecutionStatus setExecutionStateAborted(String executionId) throws DbException {
        return executeUpdate("db-setExecutionStateAborted",
                r.table(TABLES.EXECUTIONS.name)
                        .get(executionId)
                        .update(
                                doc -> r.branch(
                                        doc.hasFields("endTime"),
                                        r.hashMap(),
                                        r.hashMap("state", State.ABORTED_MANUAL.name()))
                        ),
                CrawlExecutionStatus.class);
    }

    @Override
    public QueuedUri saveQueuedUri(QueuedUri qu) throws DbException {
        if (!qu.hasEarliestFetchTimeStamp()) {
            qu = qu.toBuilder().setEarliestFetchTimeStamp(ProtoUtils.getNowTs()).build();
        }
        return saveMessage(qu, TABLES.URI_QUEUE);
    }

    @Override
    public void deleteQueuedUri(QueuedUri qu) throws DbException {
        deleteConfigMessage(qu, TABLES.URI_QUEUE);
    }

    @Override
    public long deleteQueuedUrisForExecution(String executionId) throws DbException {
        return executeRequest("db-deleteQueuedUrisForExecution",
                r.table(TABLES.URI_QUEUE.name)
                        .getAll(executionId).optArg("index", "executionId")
                        .delete().g("deleted")
        );
    }

    @Override
    public long queuedUriCount(String executionId) throws DbException {
        return executeRequest("db-queuedUriCount",
                r.table(TABLES.URI_QUEUE.name)
                        .getAll(executionId).optArg("index", "executionId")
                        .count()
        );
    }

    @Override
    public boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException {
        return executeRequest("db-uriNotIncludedInQueue",
                r.table(RethinkDbAdapter.TABLES.CRAWL_LOG.name)
                        .between(
                                r.array(qu.getSurt(), ProtoUtils.tsToOdt(since)),
                                r.array(qu.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(RethinkDbAdapter.TABLES.URI_QUEUE.name).getAll(qu.getSurt())
                                        .optArg("index", "surt")
                                        .limit(1)
                        ).isEmpty());
    }

    @Override
    public FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup) throws DbException {
        List fromKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.minval(),
                r.minval()
        );

        List toKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.maxval(),
                r.maxval()
        );

        try (Cursor<Map<String, Object>> cursor = executeRequest("db-getNextQueuedUriToFetch",
                r.table(TABLES.URI_QUEUE.name)
                        .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                        .between(fromKey, toKey)
                        .limit(1));) {

            if (cursor.hasNext()) {
                QueuedUri qUri = ProtoUtils.rethinkToProto(cursor.next(), QueuedUri.class);
                if (Timestamps.comparator().compare(qUri.getEarliestFetchTimeStamp(), ProtoUtils.getNowTs()) <= 0) {
                    return FutureOptional.of(qUri);
                } else {
                    return FutureOptional.emptyUntil(ProtoUtils.tsToOdt(qUri.getEarliestFetchTimeStamp()));
                }
            }
        }
        return FutureOptional.empty();
    }

    @Override
    public Screenshot saveScreenshot(Screenshot s) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(s);

        Map<String, Object> response = executeRequest("db-addScreenshot",
                r.table(TABLES.SCREENSHOT.name)
                        .insert(rMap)
                        .optArg("conflict", "error"));

        String key = ((List<String>) response.get("generated_keys")).get(0);

        return s.toBuilder().setId(key).build();
    }

    @Override
    public ScreenshotListReply listScreenshots(ScreenshotListRequest request) throws DbException {
        ScreenshotListRequestQueryBuilder queryBuilder = new ScreenshotListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public Empty deleteScreenshot(Screenshot screenshot) throws DbException {
        return deleteConfigMessage(screenshot, TABLES.SCREENSHOT);
    }

    @Override
    public ChangeFeed<RunningExecutionsListReply> getExecutionStatusStream(RunningExecutionsRequest request) throws DbException {
        int limit = request.getPageSize();
        if (limit == 0) {
            limit = 100;
        }
        Cursor<Map> res = executeRequest("db-getExecutionStatusStream",
                r.table(RethinkDbAdapter.TABLES.EXECUTIONS.name)
                        .orderBy().optArg("index", r.desc("startTime"))
                        .limit(limit)
                        .changes().optArg("include_initial", true).optArg("include_offsets", true).optArg("squash", 2)
                        .map(v -> v.g("new_val").merge(r.hashMap("newOffset", v.g("new_offset").default_((String) null))
                                .with("oldOffset", v.g("old_offset").default_((String) null))))
                        .eqJoin("seedId", r.table(TABLES.SEEDS.name))
                        .map(v -> {
                            return v.g("left").merge(r.hashMap("seed", v.g("right").g("meta").g("name"))
                                    .with("queueSize",
                                            r.table("uri_queue")
                                                    .getAll(v.g("left").g("id")).optArg("index", "executionId")
                                                    .count()));
                        })
                        .without("seedId")
        );

        return new ChangeFeedBase<RunningExecutionsListReply>(res) {
            RunningExecutionsListReply.Builder reply = RunningExecutionsListReply.newBuilder();

            @Override
            protected Function<Map<String, Object>, RunningExecutionsListReply> mapper() {
                return t -> {
                    Long newOffset = (Long) t.remove("newOffset");
                    Long oldOffset = (Long) t.remove("oldOffset");
                    StatusProto.StatusDetail resp = ProtoUtils.rethinkToProto(t, StatusProto.StatusDetail.class);
                    if (oldOffset != null) {
                        reply.removeValue(oldOffset.intValue());
                    }
                    if (newOffset != null) {
                        if (newOffset > reply.getValueCount()) {
                            newOffset = (long) reply.getValueCount();
                        }
                        reply.addValue(newOffset.intValue(), resp);
                    }
                    return reply.build();
                };
            }

        };
    }

    @Override
    public CrawlEntity getCrawlEntity(GetRequest req) throws DbException {
        return getMessage(req, CrawlEntity.class, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) throws DbException {
        return saveMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public Empty deleteCrawlEntity(CrawlEntity entity) throws DbException {
        checkDependencies(entity, TABLES.SEEDS, Seed.getDefaultInstance(), "entity_id");
        return deleteConfigMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_ENTITIES);
        return queryBuilder.executeList(this, CrawlEntityListReply.newBuilder()).build();
    }

    @Override
    public Seed getSeed(GetRequest req) throws DbException {
        return getMessage(req, Seed.class, TABLES.SEEDS);
    }

    @Override
    public SeedListReply listSeeds(SeedListRequest request) throws DbException {
        SeedListRequestQueryBuilder queryBuilder = new SeedListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public Seed saveSeed(Seed seed) throws DbException {
        return saveMessage(seed, TABLES.SEEDS);
    }

    @Override
    public Empty deleteSeed(Seed seed) throws DbException {
        return deleteConfigMessage(seed, TABLES.SEEDS);
    }

    @Override
    public CrawlJob getCrawlJob(GetRequest req) throws DbException {
        return getMessage(req, CrawlJob.class, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_JOBS);
        return queryBuilder.executeList(this, CrawlJobListReply.newBuilder()).build();
    }

    @Override
    public CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException {
        if (crawlJob.getCrawlConfigId().isEmpty()) {
            throw new IllegalArgumentException("A crawl config is required for crawl jobs");
        }

        return saveMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException {
        checkDependencies(crawlJob, TABLES.SEEDS, Seed.getDefaultInstance(), "job_id");
        return deleteConfigMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlConfig getCrawlConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlConfig.class, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_CONFIGS);
        return queryBuilder.executeList(this, CrawlConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        return saveMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        checkDependencies(crawlConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "crawl_config_id");
        return deleteConfigMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlScheduleConfig.class, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_SCHEDULE_CONFIGS);
        return queryBuilder.executeList(this, CrawlScheduleConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        return saveMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        checkDependencies(crawlScheduleConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "schedule_id");
        return deleteConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException {
        return getMessage(req, PolitenessConfig.class, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.POLITENESS_CONFIGS);
        return queryBuilder.executeList(this, PolitenessConfigListReply.newBuilder()).build();
    }

    @Override
    public PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        return saveMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        checkDependencies(politenessConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "politeness_id");
        return deleteConfigMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public BrowserConfig getBrowserConfig(GetRequest req) throws DbException {
        return getMessage(req, BrowserConfig.class, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.BROWSER_CONFIGS);
        return queryBuilder.executeList(this, BrowserConfigListReply.newBuilder()).build();
    }

    @Override
    public BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException {
        return saveMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException {
        checkDependencies(browserConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "browser_config_id");
        return deleteConfigMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException {
        RoleMappingsListRequestQueryBuilder queryBuilder = new RoleMappingsListRequestQueryBuilder(request);
        return queryBuilder.executeList(this).build();
    }

    @Override
    public RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException {
        Map<String, Object> doc = ProtoUtils.protoToRethink(roleMapping);
        return executeInsert("save-rolemapping",
                r.table(TABLES.ROLE_MAPPINGS.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                RoleMapping.class
        );
    }

    @Override
    public Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException {
        return deleteConfigMessage(roleMapping, TABLES.ROLE_MAPPINGS);
    }

    @Override
    public LogLevels getLogConfig() throws DbException {
        Map<String, Object> response = executeRequest("get-logconfig",
                r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                        .get("log_levels")
                        .pluck("logLevel")
        );

        return ProtoUtils.rethinkToProto(response, LogLevels.class);
    }

    @Override
    public LogLevels saveLogConfig(LogLevels logLevels) throws DbException {
        Map<String, Object> doc = ProtoUtils.protoToRethink(logLevels);
        doc.put("id", "log_levels");
        return executeInsert("save-logconfig",
                r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                LogLevels.class
        );
    }

    public <T extends Message> T getMessage(GetRequest req, Class<T> type, TABLES table) throws DbException {
        Map<String, Object> response = executeRequest("db-get" + type.getSimpleName(),
                r.table(table.name)
                        .get(req.getId())
        );

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, type);
    }

    public <T extends Message> T saveMessage(T msg, TABLES table) throws DbException {
        FieldDescriptor metaField = msg.getDescriptorForType().findFieldByName("meta");
        Map rMap = ProtoUtils.protoToRethink(msg);

        if (metaField == null) {
            return executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table.name)
                            .insert(rMap)
                            .optArg("conflict", "replace"),
                    (Class<T>) msg.getClass()
            );
        } else {
            // Check that name is set if this is a new object
            if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
                throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                        + " object, but meta.name is not set.");
            }

            rMap.put("meta", updateMeta((Map) rMap.get("meta")));

            return executeInsert("db-save" + msg.getClass().getSimpleName(),
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
                    (Class<T>) msg.getClass()
            );
        }
    }

    public <T extends Message> Empty deleteConfigMessage(T entity, TABLES table) throws DbException {
        Descriptors.FieldDescriptor idDescriptor = entity.getDescriptorForType().findFieldByName("id");

        executeRequest("db-delete" + entity.getClass().getSimpleName(),
                r.table(table.name)
                        .get(entity.getField(idDescriptor))
                        .delete()
        );

        return Empty.getDefaultInstance();
    }

    private Map updateMeta(Map meta) {
        if (meta == null) {
            meta = r.hashMap();
        }

        String user = EmailContextKey.email();
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

    public <T extends Message> T executeInsert(String operationName, Insert qry, Class<T> type) throws DbException {
        return executeInsertOrUpdate(operationName, qry, type);
    }

    public <T extends Message> T executeUpdate(String operationName, Update qry, Class<T> type) throws DbException {
        return executeInsertOrUpdate(operationName, qry, type);
    }

    private <T extends Message> T executeInsertOrUpdate(String operationName, ReqlExpr qry, Class<T> type) throws DbException {
        if (qry instanceof Insert) {
            qry = ((Insert) qry).optArg("return_changes", "always");
        } else if (qry instanceof Update) {
            qry = ((Update) qry).optArg("return_changes", "always");
        }

        Map<String, Object> response = executeRequest(operationName, qry);
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        Map newDoc = changes.get(0).get("new_val");
        return ProtoUtils.rethinkToProto(newDoc, type);
    }

    public <T> T executeRequest(String operationName, ReqlExpr qry) throws DbException {
        return RethinkDbConnection.getInstance().exec(operationName, qry);
    }

    @Override
    public void close() {
        RethinkDbConnection.getInstance().close();
    }

    /**
     * Check references to Config object.
     *
     * @param messageToCheck     the config message which other objects might refer.
     * @param dependentTable     the table containing objects which might have a dependency to the object to check.
     * @param dependentMessage   the object type in the table containing objects which might have a dependency to the
     *                           object to check.
     * @param dependentFieldName the field name in the dependent message which might contain reference to the id field
     *                           in the object to check.
     * @throws IllegalStateException if there are dependencies.
     */
    private void checkDependencies(Message messageToCheck, TABLES dependentTable,
                                   Message dependentMessage, String dependentFieldName) throws DbException {

        Descriptors.FieldDescriptor messageIdField = messageToCheck.getDescriptorForType().findFieldByName("id");
        Descriptors.FieldDescriptor dependentField = dependentMessage.getDescriptorForType()
                .findFieldByName(dependentFieldName);

        ListRequestQueryBuilder qry = new ListRequestQueryBuilder(ListRequest.getDefaultInstance(), dependentTable);
        if (dependentField.isRepeated()) {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).contains(messageToCheck.getField(messageIdField)));
        } else {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).eq(messageToCheck.getField(messageIdField)));
        }

        long dependencyCount = qry.executeCount(this);
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

    private Map summarizeJobExecutionStats(String jobExecutionId) throws DbException {
        String[] EXECUTIONS_STAT_FIELDS = new String[]{"documentsCrawled", "documentsDenied",
                "documentsFailed", "documentsOutOfScope", "documentsRetried", "urisCrawled", "bytesCrawled"};

        return executeRequest("db-summarizeJobExecutionStats",
                r.table(TABLES.EXECUTIONS.name)
                        .getAll(jobExecutionId).optArg("index", "jobExecutionId")
                        .map(doc -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, doc.getField(f).default_(0));
                                    }
                                    for (State s : State.values()) {
                                        m.with(s.name(), r.branch(doc.getField("state").eq(s.name()), 1, 0));
                                    }
                                    return m;
                                }
                        )
                        .reduce((left, right) -> {
                                    MapObject m = r.hashMap();
                                    for (String f : EXECUTIONS_STAT_FIELDS) {
                                        m.with(f, left.getField(f).add(right.getField(f)));
                                    }
                                    for (State s : State.values()) {
                                        m.with(s.name(), left.getField(s.name()).add(right.getField(s.name())));
                                    }
                                    return m;
                                }
                        ).default_((doc) -> {
                            MapObject m = r.hashMap();
                            for (String f : EXECUTIONS_STAT_FIELDS) {
                                m.with(f, 0);
                            }
                            for (State s : State.values()) {
                                m.with(s.name(), 0);
                            }
                            return m;
                        }
                )
        );
    }

}
