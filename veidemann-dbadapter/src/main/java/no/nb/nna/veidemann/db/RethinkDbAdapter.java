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

import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
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
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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

    private final RethinkDbConnection conn;

    public RethinkDbAdapter(RethinkDbConnection conn) {
        this.conn = conn;
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
            CrawledContent result = ProtoUtils.rethinkToProto(response, CrawledContent.class);

            // Check existence of original in crawl log.
            // This prevents false positives in the case where writing of original record was cancelled after
            // crawled_content table was updated.
            CrawlLogListReply cl = listCrawlLogs(CrawlLogListRequest.newBuilder()
                    .setPageSize(1)
                    .addWarcId(result.getWarcId())
                    .build()
            );
            if (cl.getValueList().isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(result);
            }
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

        Map rMap = ProtoUtils.protoToRethink(cl);
        return conn.executeInsert("db-saveCrawlLog",
                r.table(TABLES.CRAWL_LOG.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                CrawlLog.class
        );
    }

    @Override
    public CrawlLogListReply listCrawlLogs(CrawlLogListRequest request) throws DbException {
        CrawlLogListRequestQueryBuilder queryBuilder = new CrawlLogListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public PageLog savePageLog(PageLog pageLog) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(pageLog);
        return conn.executeInsert("db-savePageLog",
                r.table(TABLES.PAGE_LOG.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                PageLog.class
        );
    }

    @Override
    public PageLogListReply listPageLogs(PageLogListRequest request) throws DbException {
        PageLogListRequestQueryBuilder queryBuilder = new PageLogListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(JobExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setStartTime(ProtoUtils.getNowTs())
                .setState(JobExecutionStatus.State.RUNNING));

        return conn.executeInsert("db-saveJobExecutionStatus",
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
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId) throws DbException {
        JobExecutionStatus result = conn.executeUpdate("db-setJobExecutionStateAborted",
                r.table(TABLES.JOB_EXECUTIONS.name)
                        .get(jobExecutionId)
                        .update(
                                doc -> r.branch(
                                        doc.hasFields("endTime"),
                                        r.hashMap(),
                                        r.hashMap("state", State.ABORTED_MANUAL.name()))
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
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public CrawlExecutionStatus setExecutionStateAborted(String executionId) throws DbException {
        return conn.executeUpdate("db-setExecutionStateAborted",
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
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public Empty deleteScreenshot(Screenshot screenshot) throws DbException {
        executeRequest("db-deleteScreenshot",
                r.table(TABLES.SCREENSHOT.name).get(screenshot.getId()).delete());
        return Empty.getDefaultInstance();
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
    public boolean setDesiredPausedState(boolean value) throws DbException {
        String id = "state";
        String key = "shouldPause";
        Map<String, List<Map<String, Map>>> state = executeRequest("set-paused",
                r.table(TABLES.SYSTEM.name)
                        .insert(r.hashMap("id", id).with(key, value))
                        .optArg("conflict", "update")
                        .optArg("return_changes", "always")
        );
        Map oldValue = state.get("changes").get(0).get("old_val");
        if (oldValue == null || (Boolean) oldValue.computeIfAbsent(key, k -> Boolean.FALSE) == false) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean getDesiredPausedState() throws DbException {
        String id = "state";
        String key = "shouldPause";
        Map<String, Object> state = executeRequest("get-paused",
                r.table(TABLES.SYSTEM.name)
                        .get(id)
        );
        if (state == null) {
            return false;
        }
        return (Boolean) state.computeIfAbsent(key, k -> Boolean.FALSE);
    }

    @Override
    public boolean isPaused() throws DbException {
        if (getDesiredPausedState()) {
            long busyCount = executeRequest("is-paused",
                    r.table(TABLES.CRAWL_HOST_GROUP.name)
                            .filter(r.hashMap("busy", true))
                            .count()
            );
            return busyCount == 0L;
        } else {
            return false;
        }
    }

    public <T> T executeRequest(String operationName, ReqlAst qry) throws DbException {
        return conn.exec(operationName, qry);
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
