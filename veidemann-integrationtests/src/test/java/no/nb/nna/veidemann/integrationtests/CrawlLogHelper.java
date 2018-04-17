package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CrawlLogHelper {
    final CrawlLogListReply crawlLogListReply;
    final Map<String, List<CrawlLog>> crawlLogs = new HashMap();
    long reportedCount;
    long logCount;

    public CrawlLogHelper(DbAdapter db) {
        crawlLogListReply = db.listCrawlLogs(CrawlLogListRequest.newBuilder().setPageSize(500).build());
        reportedCount = crawlLogListReply.getCount();
        crawlLogListReply.getValueList().forEach(c -> addCrawlLog(c));
    }

    private void addCrawlLog(CrawlLog crawlLog) {
        String type;
        if (crawlLog.getRequestedUri().startsWith("dns")) {
            type = "dns";
        } else {
            type = crawlLog.getRecordType();
            assertThat(crawlLog.getExecutionId())
                    .as("Execution id should not be empty for %s", crawlLog)
                    .isNotEmpty();
            assertThat(crawlLog.getJobExecutionId())
                    .as("Job execution id should not be empty for %s", crawlLog)
                    .isNotEmpty();
        }

        List<CrawlLog> l = crawlLogs.get(type);
        if (l == null) {
            l = new ArrayList<>();
            crawlLogs.put(type, l);
        }
        l.add(crawlLog);
        logCount++;
    }

    public int getTypeCount(String type) {
        return getCrawlLog(type).size();
    }

    public void checkCount() {
        assertThat(logCount).isEqualTo(reportedCount);
    }

    public List<CrawlLog> getCrawlLog() {
        return crawlLogListReply.getValueList();
    }

    public List<CrawlLog> getCrawlLog(String type) {
        return crawlLogs.getOrDefault(type, Collections.emptyList());
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
        crawlLogs.forEach((k, v) -> {
            sb.append(k).append("(").append(v.size()).append(") {\n");
            v.forEach(crawlLog -> sb.append("  ").append(crawlLog.getRequestedUri()).append("\n"));
            sb.append("}\n");
        });
        return sb.toString();
    }
}
