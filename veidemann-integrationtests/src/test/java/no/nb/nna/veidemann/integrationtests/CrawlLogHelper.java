/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CrawlLogHelper {
    final List<CrawlLog> crawlLogs = new ArrayList<>();
    final Map<String, List<CrawlLog>> crawlLogsByType = new HashMap();
    final Map<String, List<CrawlLog>> crawlLogsByEid = new HashMap();
    final Map<String, List<CrawlLog>> crawlLogsByJobEid = new HashMap();

    public CrawlLogHelper(String collectionName) throws DbException {
        DbAdapter db = DbService.getInstance().getDbAdapter();
        CrawlLogListReply reply = db.listCrawlLogs(CrawlLogListRequest.newBuilder()
                .setPageSize(5000).build());
        reply.getValueList()
                .stream()
                .filter(c -> c.getCollectionFinalName().startsWith(collectionName))
                .forEach(c -> addCrawlLog(c));
    }

    private void addCrawlLog(CrawlLog crawlLog) {
        crawlLogs.add(crawlLog);

        String type;
        if (crawlLog.getRequestedUri().startsWith("dns")) {
            type = "dns";
        } else {
            type = crawlLog.getRecordType();
            assertThat(crawlLog.getExecutionId())
                    .as("Execution id should not be empty for crawl log entry %s", crawlLog)
                    .isNotEmpty();
            assertThat(crawlLog.getJobExecutionId())
                    .as("Job execution id should not be empty for crawl log entry %s", crawlLog)
                    .isNotEmpty();
        }

        List<CrawlLog> typeList = crawlLogsByType.computeIfAbsent(type, k -> new ArrayList<>());
        if (crawlLog.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode()
                && !crawlLog.getRequestedUri().endsWith("robots.txt")) {
            typeList.add(crawlLog);
        }

        List<CrawlLog> eidList = crawlLogsByEid.computeIfAbsent(crawlLog.getExecutionId(), k -> new ArrayList<>());
        eidList.add(crawlLog);

        List<CrawlLog> jobEidList = crawlLogsByJobEid.computeIfAbsent(crawlLog.getJobExecutionId(), k -> new ArrayList<>());
        jobEidList.add(crawlLog);
    }

    public int getTypeCount(String type) {
        return getCrawlLogsByType(type).size();
    }

    public List<CrawlLog> getCrawlLog() {
        return crawlLogs;
    }

    public List<CrawlLog> getCrawlLogsByType(String type) {
        return crawlLogsByType.getOrDefault(type, Collections.emptyList());
    }

    public List<CrawlLog> getCrawlLogsByExecutionId(String eid) {
        return crawlLogsByEid.getOrDefault(eid, Collections.emptyList());
    }

    public List<CrawlLog> getCrawlLogsByJobExecutionId(String jobEid) {
        return crawlLogsByJobEid.getOrDefault(jobEid, Collections.emptyList());
    }

    CrawlLog getCrawlLogEntry(String warcId) {
        for (CrawlLog c : getCrawlLog()) {
            if (c.getWarcId().equals(warcId)) {
                return c;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        crawlLogsByType.forEach((k, v) -> {
            sb.append(k).append("(").append(v.size()).append(") {\n");
            v.forEach(crawlLog -> sb.append("  ").append(crawlLog.getRequestedUri()).append("\n"));
            sb.append("}\n");
        });
        return sb.toString();
    }
}
