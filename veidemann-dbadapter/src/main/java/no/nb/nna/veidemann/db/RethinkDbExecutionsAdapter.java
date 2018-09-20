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

package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.gen.ast.Update;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScope;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus.Builder;
import no.nb.nna.veidemann.api.StatusProto;
import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class RethinkDbExecutionsAdapter implements ExecutionsAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbExecutionsAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbExecutionsAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(JobExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setStartTime(ProtoUtils.getNowTs())
                .setState(JobExecutionStatus.State.RUNNING));

        return conn.executeInsert("db-saveJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name)
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
        JobExecutionStatus jes = ProtoUtils.rethinkToProto(conn.exec("db-getJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name)
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
                r.table(Tables.JOB_EXECUTIONS.name)
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
                req -> listCrawlExecutionStatus(req),
                exe -> {
                    try {
                        setCrawlExecutionStateAborted(exe.getId());
                    } catch (DbException e) {
                        LOG.error("Failed to abort Crawl Execution {}", exe.getId(), e);
                    }
                });

        return result;
    }

    @Override
    public CrawlExecutionStatus createCrawlExecutionStatus(String jobId, String jobExecutionId, String seedId, CrawlScope scope) throws DbException {
        Objects.requireNonNull(jobId, "jobId must be set");
        Objects.requireNonNull(jobExecutionId, "jobExecutionId must be set");
        Objects.requireNonNull(seedId, "seedId must be set");
        Objects.requireNonNull(scope, "crawl scope must be set");

        CrawlExecutionStatus status = CrawlExecutionStatus.newBuilder()
                .setJobId(jobId)
                .setJobExecutionId(jobExecutionId)
                .setSeedId(seedId)
                .setScope(scope)
                .setState(State.CREATED)
                .build();

        Map rMap = ProtoUtils.protoToRethink(status);
        rMap.put("lastChangeTime", r.now());
        rMap.put("createdTime", r.now());

        Insert qry = r.table(Tables.EXECUTIONS.name).insert(rMap);
        return conn.executeInsert("db-createExecutionStatus", qry, CrawlExecutionStatus.class);
    }

    @Override
    public CrawlExecutionStatus updateCrawlExecutionStatus(CrawlExecutionStatusChange statusChange) throws DbException {
        DistributedLock lock = conn.createDistributedLock(new Key("crawlexe", statusChange.getId()), 60);
        lock.lock();
        try {
            ReqlFunction1 updateFunc = (doc) -> {
                MapObject rMap = r.hashMap("lastChangeTime", r.now());

                if (statusChange.getState() != State.UNDEFINED) {
                    switch (statusChange.getState()) {
                        case FETCHING:
                        case SLEEPING:
                        case CREATED:
                            throw new IllegalArgumentException("Only the final states are allowed to be updated");
                        default:
                            rMap.with("state", statusChange.getState().name());
                    }
                }
                if (statusChange.getAddBytesCrawled() != 0) {
                    rMap.with("bytesCrawled", doc.g("bytesCrawled").add(statusChange.getAddBytesCrawled()).default_(statusChange.getAddBytesCrawled()));
                }
                if (statusChange.getAddDocumentsCrawled() != 0) {
                    rMap.with("documentsCrawled", doc.g("documentsCrawled").add(statusChange.getAddDocumentsCrawled()).default_(statusChange.getAddDocumentsCrawled()));
                }
                if (statusChange.getAddDocumentsDenied() != 0) {
                    rMap.with("documentsDenied", doc.g("documentsDenied").add(statusChange.getAddDocumentsDenied()).default_(statusChange.getAddDocumentsDenied()));
                }
                if (statusChange.getAddDocumentsFailed() != 0) {
                    rMap.with("documentsFailed", doc.g("documentsFailed").add(statusChange.getAddDocumentsFailed()).default_(statusChange.getAddDocumentsFailed()));
                }
                if (statusChange.getAddDocumentsOutOfScope() != 0) {
                    rMap.with("documentsOutOfScope", doc.g("documentsOutOfScope").add(statusChange.getAddDocumentsOutOfScope()).default_(statusChange.getAddDocumentsOutOfScope()));
                }
                if (statusChange.getAddDocumentsRetried() != 0) {
                    rMap.with("documentsRetried", doc.g("documentsRetried").add(statusChange.getAddDocumentsRetried()).default_(statusChange.getAddDocumentsRetried()));
                }
                if (statusChange.getAddUrisCrawled() != 0) {
                    rMap.with("urisCrawled", doc.g("urisCrawled").add(statusChange.getAddUrisCrawled()).default_(statusChange.getAddUrisCrawled()));
                }
                if (statusChange.hasEndTime()) {
                    rMap.with("endTime", ProtoUtils.tsToOdt(statusChange.getEndTime()));
                }
                if (statusChange.hasError()) {
                    rMap.with("error", ProtoUtils.protoToRethink(statusChange.getError()));
                }
                if (statusChange.hasAddCurrentUri()) {
                    rMap.with("currentUriId", doc.g("currentUriId").default_(r.array()).setUnion(r.array(statusChange.getAddCurrentUri().getId())));
                }
                if (statusChange.hasDeleteCurrentUri()) {
                    rMap.with("currentUriId", doc.g("currentUriId").default_(r.array()).setDifference(r.array(statusChange.getDeleteCurrentUri().getId())));
                }
                return doc.merge(rMap)
                        .merge(d -> r.branch(
                                // If the original document had one of the ended states, then keep the
                                // original endTime if it exists, otherwise use the one from the change request
                                doc.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", doc.g("state")).with("endTime",
                                        r.branch(doc.hasFields("endTime"), doc.g("endTime"), d.g("endTime").default_((Object) null))),

                                // If the change request contained an end state, use it
                                d.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", d.g("state")),

                                // Set the state to fetching if currentUriId contains at least one value, otherwise set state to sleeping.
                                d.g("currentUriId").default_(r.array()).count().gt(0),
                                r.hashMap("state", "FETCHING"),
                                r.hashMap("state", "SLEEPING")))

                        // Set start time if not set and state is fetching
                        .merge(d -> r.branch(doc.hasFields("startTime").not().and(d.g("state").match("FETCHING")),
                                r.hashMap("startTime", r.now()),
                                r.hashMap()));
            };


            // Update the CrawlExecutionStatus
            Update qry = r.table(Tables.EXECUTIONS.name)
                    .get(statusChange.getId())
                    .update(updateFunc);


            // Return both the new and the old values
            qry = qry.optArg("return_changes", "always");
            Map<String, Object> response = conn.exec("db-updateCrawlExecutionStatus", qry);
            List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

            // Check if this update was setting the end time
            boolean wasNotEnded = changes.get(0).get("old_val") == null || changes.get(0).get("old_val").get("endTime") == null;
            CrawlExecutionStatus newDoc = ProtoUtils.rethinkToProto(changes.get(0).get("new_val"), CrawlExecutionStatus.class);
            if (wasNotEnded && newDoc.hasEndTime()) {
                updateJobExecution(newDoc.getJobExecutionId());
            }

            // Remove queued uri from queue if change request asks for deletion
            if (statusChange.hasDeleteCurrentUri()) {
                conn.exec("db-deleteQueuedUri",
                        r.table(Tables.URI_QUEUE.name)
                                .get(statusChange.getDeleteCurrentUri().getId())
                                .delete()
                );
            }


            return newDoc;
        } finally {
            lock.unlock();
        }
    }

    private void updateJobExecution(String jobExecutionId) throws DbException {
        DistributedLock lock = conn.createDistributedLock(new Key("jobexe", jobExecutionId), 60);
        lock.lock();
        try {
            // Get a count of still running CrawlExecutions for this execution's JobExecution
            Long notEndedCount = conn.exec("db-updateJobExecution",
                    r.table(Tables.EXECUTIONS.name)
                            .getAll(jobExecutionId).optArg("index", "jobExecutionId")
                            .filter(row -> row.g("state").match("UNDEFINED|CREATED|FETCHING|SLEEPING"))
                            .count()
            );

            // If all CrawlExecutions are done for this JobExectuion, update the JobExecution with end statistics
            if (notEndedCount == 0) {
                LOG.debug("JobExecution '{}' finished, saving stats", jobExecutionId);

                // Fetch the JobExecutionStatus object this CrawlExecution is part of
                JobExecutionStatus jes = conn.executeGet("db-getJobExecutionStatus",
                        r.table(Tables.JOB_EXECUTIONS.name).get(jobExecutionId),
                        JobExecutionStatus.class);
                if (jes == null) {
                    throw new IllegalStateException("Can't find JobExecution: " + jobExecutionId);
                }

                // Set JobExecution's status to FINISHED if it wasn't already aborted
                JobExecutionStatus.State state;
                switch (jes.getState()) {
                    case DIED:
                    case FAILED:
                    case ABORTED_MANUAL:
                        state = jes.getState();
                        break;
                    default:
                        state = JobExecutionStatus.State.FINISHED;
                        break;
                }

                // Update aggregated statistics
                Map sums = summarizeJobExecutionStats(jobExecutionId);
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

                conn.exec("db-saveJobExecutionStatus",
                        r.table(Tables.JOB_EXECUTIONS.name).get(jesBuilder.getId()).update(ProtoUtils.protoToRethink(jesBuilder)));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CrawlExecutionStatus getCrawlExecutionStatus(String crawlExecutionId) throws DbException {
        Map<String, Object> response = conn.exec("db-getExecutionStatus",
                r.table(Tables.EXECUTIONS.name)
                        .get(crawlExecutionId)
        );

        return ProtoUtils.rethinkToProto(response, CrawlExecutionStatus.class);
    }

    @Override
    public ExecutionsListReply listCrawlExecutionStatus(ListExecutionsRequest listExecutionsRequest) throws DbException {
        CrawlExecutionsListRequestQueryBuilder queryBuilder = new CrawlExecutionsListRequestQueryBuilder(listExecutionsRequest);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public CrawlExecutionStatus setCrawlExecutionStateAborted(String crawlExecutionId) throws DbException {
        DistributedLock lock = conn.createDistributedLock(new Key("crawlexe", crawlExecutionId), 60);
        lock.lock();
        try {
            return conn.executeUpdate("db-setExecutionStateAborted",
                    r.table(Tables.EXECUTIONS.name)
                            .get(crawlExecutionId)
                            .update(
                                    doc -> r.branch(
                                            doc.hasFields("endTime"),
                                            r.hashMap(),
                                            r.hashMap("state", State.ABORTED_MANUAL.name()))
                            ),
                    CrawlExecutionStatus.class);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ChangeFeed<RunningExecutionsListReply> getCrawlExecutionStatusStream(RunningExecutionsRequest request) throws DbException {
        int limit = request.getPageSize();
        if (limit == 0) {
            limit = 100;
        }
        Cursor<Map> res = conn.exec("db-getExecutionStatusStream",
                r.table(Tables.EXECUTIONS.name)
                        .orderBy().optArg("index", r.desc("startTime"))
                        .limit(limit)
                        .changes().optArg("include_initial", true).optArg("include_offsets", true).optArg("squash", 2)
                        .map(v -> v.g("new_val").merge(r.hashMap("newOffset", v.g("new_offset").default_((String) null))
                                .with("oldOffset", v.g("old_offset").default_((String) null))))
                        .eqJoin("seedId", r.table(Tables.SEEDS.name))
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

    private Map summarizeJobExecutionStats(String jobExecutionId) throws DbException {
        String[] EXECUTIONS_STAT_FIELDS = new String[]{"documentsCrawled", "documentsDenied",
                "documentsFailed", "documentsOutOfScope", "documentsRetried", "urisCrawled", "bytesCrawled"};

        return conn.exec("db-summarizeJobExecutionStats",
                r.table(Tables.EXECUTIONS.name)
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
