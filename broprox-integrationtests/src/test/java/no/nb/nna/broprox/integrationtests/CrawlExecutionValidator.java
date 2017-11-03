package no.nb.nna.broprox.integrationtests;

import no.nb.nna.broprox.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.broprox.api.ReportProto.PageLogListRequest;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.PageLog;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
        checkChecksum();
        checkIp();
    }

    private void checkConsistency() {
        crawlLogs.forEach(cl -> {
            assertThat(warcRecords.keySet())
                    .as("Missing WARC record for crawllog entry %s", cl.getWarcId())
                    .contains(cl.getWarcId());
            if (!cl.getWarcRefersTo().isEmpty()) {
                assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                        .as("Missing crawllog entry for record %s's warcRefersTo", cl)
                        .contains(cl.getWarcRefersTo());
            }
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
        warcRecords.values().stream()
                .filter(w -> ((!"metadata".equals(w.header.warcTypeStr)) && (!"warcinfo".equals(w.header.warcTypeStr))))
                .forEach(wid -> {
                    assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                            .as("Missing crawllog entry for WARC record %s", wid)
                            .contains(wid.header.warcRecordIdStr.substring(10, wid.header.warcRecordIdStr.lastIndexOf(">")));

                    String refersTo = stripWarcId(wid.header.warcRefersToStr);
                    if (!refersTo.isEmpty()) {
                        assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                                .as("Missing crawllog entry for WARC record %s's warcRefersTo", wid)
                                .contains(refersTo);
                        assertThat(warcRecords.keySet().stream())
                                .as("Missing crawllog entry for WARC record %s", wid)
                                .contains(refersTo);
                    }
                });
    }

    private void checkIp() {
        crawlLogs.forEach(cl -> {
            assertThat(cl.getIpAddress())
                    .as("Ip address for crawllog entry %s was empty", cl.getWarcId())
                    .isNotEmpty();
        });
        warcRecords.values().stream()
                .filter(w -> ((!"metadata".equals(w.header.warcTypeStr)) && (!"warcinfo".equals(w.header.warcTypeStr))))
                .forEach(w -> {
                    assertThat(w.header.warcIpAddress)
                            .as("Ip address for WARC entry %s was empty", w.header.warcRecordIdStr)
                            .isNotEmpty();
                });
    }

    private void checkChecksum() {
        crawlLogs.forEach(cl -> {
            assertThat(cl.getBlockDigest())
                    .as("Block digest for crawllog entry %s was empty", cl.getWarcId())
                    .isNotEmpty();
            assertThat(cl.getPayloadDigest())
                    .as("Payload digest for crawllog entry %s was empty", cl.getWarcId())
                    .isNotEmpty();
        });
        warcRecords.values().stream()
                .filter(w -> ((!"metadata".equals(w.header.warcTypeStr)) && (!"warcinfo".equals(w.header.warcTypeStr))))
                .forEach(w -> {
                    assertThat(w.header.warcBlockDigestStr)
                            .as("Block digest for WARC entry %s was empty", w.header.warcRecordIdStr)
                            .isNotEmpty();
                    assertThat(w.header.warcPayloadDigestStr)
                            .as("Payload digest for WARC entry %s was empty", w.header.warcRecordIdStr)
                            .isNotEmpty();
                });
    }

    private void checkValidWarc() {
        warcRecords.values().forEach(r -> {
            assertThat(r.isCompliant())
                    .as("Record is not compliant. Uri: %s, type: %s", r.header.warcTargetUriStr, r.header.warcTypeStr)
                    .isTrue();
            if (!r.isCompliant()) {
                HttpHeader p = r.getHttpHeader();
                System.out.println("Http Header: " + p);
                if (p != null && !p.isValid()) {
                    System.out.println("Valid: " + p.isValid());
                    p.getHeaderList().forEach(h -> System.out.print(" H: " + new String(h.raw)));
                }
            }
            if (!r.diagnostics.getErrors().isEmpty()) {
                System.out.println("ERRORS: " + r.diagnostics.getErrors()
                        .stream()
                        .map(d -> "\n   " + d.type.toString() + ":" + d.entity + ":" + Arrays.toString(d.getMessageArgs()))
                        .collect(Collectors.joining()));
                r.getHeaderList().forEach(h -> System.out.print(" W: " + new String(h.raw)));
            }
            if (!r.diagnostics.getWarnings().isEmpty()) {
                System.out.println("WARNINGS: " + r.diagnostics.getWarnings()
                        .stream()
                        .map(d -> "\n   " + d.type.toString() + ":" + d.entity + ":" + Arrays.toString(d.getMessageArgs()))
                        .collect(Collectors.joining()));
                r.getHeaderList().forEach(h -> System.out.print(" W: " + new String(h.raw)));
            }
        });
    }

    private void init() {
        crawlLogs = db.listCrawlLogs(CrawlLogListRequest.getDefaultInstance()).getValueList();
        pageLogs = db.listPageLogs(PageLogListRequest.getDefaultInstance()).getValueList();
        warcRecords = new HashMap<>();

        WarcInspector.getWarcFiles().getRecordStream()
                .forEach(r -> {
                    try {
                        r.close();
                    } catch (IOException e) {
                        fail("Failed closing record", e);
                    }
                    warcRecords.put(r.header.warcRecordIdStr.substring(10, r.header.warcRecordIdStr.lastIndexOf(">")), r);
                });
    }

    private String stripWarcId(String warcUrn) {
        if (warcUrn == null || warcUrn.isEmpty()) {
            return "";
        }
        return warcUrn.substring(10, warcUrn.lastIndexOf(">"));
    }
}
