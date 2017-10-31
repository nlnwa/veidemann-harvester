package no.nb.nna.broprox.integrationtests;

import no.nb.nna.broprox.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.broprox.api.ReportProto.PageLogListRequest;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.PageLog;
import org.jwat.common.HttpHeader;
import org.jwat.common.PayloadWithHeaderAbstract;
import org.jwat.warc.WarcRecord;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CrawlExecutionValidator {
    final RethinkDbAdapter db;

    List<CrawlLog> crawlLogs;
    List<PageLog> pageLogs;
    Map<String, WarcRecord> warcRecords;

    public CrawlExecutionValidator(RethinkDbAdapter db) {
        this.db = db;
    }

    public void validate() {
        init();

        checkConsistency();
        checkValidWarc();
        checkIp();
   }

    private void checkConsistency() {
        crawlLogs.forEach(cl -> {
            assertThat(warcRecords.keySet())
                    .as("Missing WARC record for crawllog entry %s", cl.getWarcId())
                    .contains(cl.getWarcId());
        });
        pageLogs.forEach(pl -> {
            assertThat(warcRecords.keySet())
                    .as("Missing WARC record for pagelog entry %s", pl.getWarcId())
                    .contains(pl.getWarcId());
            pl.getResourceList().forEach(r -> {
                assertThat(warcRecords.keySet())
                        .as("Missing WARC record for crawllog resource entry %s", r.getWarcId())
                        .contains(r.getWarcId());
            });
        });
        warcRecords.keySet().forEach(wid -> {
            assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                    .as("Missing crawllog entry for WARC record %s", wid)
                    .contains(wid);
        });
    }

    private void checkIp() {
        crawlLogs.forEach(cl -> {
            assertThat(cl.getIpAddress())
                    .as("Ip address for crawllog entry %s was empty", cl.getWarcId())
                    .isNotEmpty();
        });
        warcRecords.values().forEach(w -> {
            assertThat(w.header.warcIpAddress)
                    .as("Ip address for WARC entry %s was empty", w.header.warcRecordIdStr)
                    .isNotEmpty();
        });
    }

    private void checkValidWarc() {
        warcRecords.values().forEach(r -> {
            System.out.println("----\n");
            System.out.println(r.header.warcTypeStr + ":  " + r.header.warcTargetUriStr);
            System.out.println("IS COMPLIANT: " + r.isCompliant());
            HttpHeader p = r.getHttpHeader();
            System.out.println("Header: " + p);
            if (p != null) {
                System.out.println("Valid: " + p.isValid());
                p.getHeaderList().forEach(h -> System.out.println(h.line));
            }
            if (!r.diagnostics.getErrors().isEmpty()) {
                System.out.println("R ERRORS: " + r.diagnostics.getErrors()
                        .stream()
                        .map(d -> "\n   " + d.type.toString() + ":" + d.entity + ":" + Arrays.toString(d.getMessageArgs()))
                        .collect(Collectors.joining()));
            }
            if (!r.diagnostics.getWarnings().isEmpty()) {
                System.out.println("R WARNINGS: " + r.diagnostics.getWarnings()
                        .stream()
                        .map(d -> "\n   " + d.type.toString() + ":" + d.entity + ":" + Arrays.toString(d.getMessageArgs()))
                        .collect(Collectors.joining()));
            }
        });
    }

    private void init() {
        crawlLogs = db.listCrawlLogs(CrawlLogListRequest.getDefaultInstance()).getValueList();
        pageLogs = db.listPageLogs(PageLogListRequest.getDefaultInstance()).getValueList();
        warcRecords = new HashMap<>();

        WarcInspector.getWarcFiles().getContentRecordStream()
                .forEach(r -> warcRecords.put(r.header.warcRecordIdStr.substring(10, r.header.warcRecordIdStr.lastIndexOf(">")), r));
    }
}
