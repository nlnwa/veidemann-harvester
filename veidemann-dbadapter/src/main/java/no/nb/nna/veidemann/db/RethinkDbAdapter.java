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
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.PageLog;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An implementation of DbAdapter for RethinkDb.
 */
public class RethinkDbAdapter implements DbAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public Optional<CrawledContent> hasCrawledContent(CrawledContent cc) throws DbException {
        ensureContainsValue(cc, "digest");
        ensureContainsValue(cc, "warc_id");
        ensureContainsValue(cc, "target_uri");
        ensureContainsValue(cc, "date");

        Map rMap = ProtoUtils.protoToRethink(cc);
        Map<String, Object> response = executeRequest("db-hasCrawledContent",
                r.table(Tables.CRAWLED_CONTENT.name)
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

    @Override
    public StorageRef saveStorageRef(StorageRef storageRef) throws DbException {
        ensureContainsValue(storageRef, "warc_id");
        ensureContainsValue(storageRef, "storage_ref");

        Map rMap = ProtoUtils.protoToRethink(storageRef);
        return conn.executeInsert("db-saveStorageRef",
                r.table(Tables.STORAGE_REF.name)
                        .insert(rMap)
                        .optArg("conflict", "replace"),
                StorageRef.class
        );
    }

    @Override
    public StorageRef getStorageRef(String warcId) throws DbException {
        return conn.executeGet("get-getStorageRef",
                r.table(Tables.STORAGE_REF.name).get(warcId),
                StorageRef.class
        );
    }

    public void deleteCrawledContent(String digest) throws DbException {
        executeRequest("db-deleteCrawledContent", r.table(Tables.CRAWLED_CONTENT.name).get(digest).delete());
    }

    @Override
    public ExtractedText addExtractedText(ExtractedText et) throws DbException {
        ensureContainsValue(et, "warc_id");
        ensureContainsValue(et, "text");

        Map rMap = ProtoUtils.protoToRethink(et);
        Map<String, Object> response = executeRequest("db-addExtractedText",
                r.table(Tables.EXTRACTED_TEXT.name)
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
        if (cl.getCollectionFinalName().isEmpty()) {
            LOG.error("Missing collectionFinalName: {}", cl, new IllegalStateException());
        }
        if (!cl.hasTimeStamp()) {
            cl = cl.toBuilder().setTimeStamp(ProtoUtils.getNowTs()).build();
        }

        Map rMap = ProtoUtils.protoToRethink(cl);
        return conn.executeInsert("db-saveCrawlLog",
                r.table(Tables.CRAWL_LOG.name)
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
        if (pageLog.getCollectionFinalName().isEmpty()) {
            LOG.error("Missing collectionFinalName: {}", pageLog, new IllegalStateException());
        }
        Map rMap = ProtoUtils.protoToRethink(pageLog);
        return conn.executeInsert("db-savePageLog",
                r.table(Tables.PAGE_LOG.name)
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
    public Screenshot saveScreenshot(Screenshot s) throws DbException {
        Map rMap = ProtoUtils.protoToRethink(s);

        Map<String, Object> response = executeRequest("db-addScreenshot",
                r.table(Tables.SCREENSHOT.name)
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
                r.table(Tables.SCREENSHOT.name).get(screenshot.getId()).delete());
        return Empty.getDefaultInstance();
    }

    @Override
    public boolean setDesiredPausedState(boolean value) throws DbException {
        String id = "state";
        String key = "shouldPause";
        Map<String, List<Map<String, Map>>> state = executeRequest("set-paused",
                r.table(Tables.SYSTEM.name)
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
                r.table(Tables.SYSTEM.name)
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
                    r.table(Tables.CRAWL_HOST_GROUP.name)
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

}
