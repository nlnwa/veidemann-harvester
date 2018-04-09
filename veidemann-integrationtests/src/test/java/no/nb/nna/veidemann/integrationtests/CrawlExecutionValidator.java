package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
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
                    .as("Missing WARC record for crawllog entry %s with uri: %s",
                            cl.getWarcId(), cl.getRequestedUri())
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
            pl.getResourceList().stream().filter(r -> !r.getFromCache()).forEach(r -> {
                assertThat(warcRecords.keySet())
                        .as("Missing WARC record for crawllog resource entry %s with uri: %s",
                                r.getWarcId(), r.getUri())
                        .contains(r.getWarcId());
            });
        });
        warcRecords.values().stream()
                .filter(w -> (("response".equals(w.header.warcTypeStr)) || ("revisit".equals(w.header.warcTypeStr)) || ("resource".equals(w.header.warcTypeStr))))
                .forEach(wid -> {
                    String warcId = stripWarcId(wid.header.warcRecordIdStr);
                    String refersTo = stripWarcId(wid.header.warcRefersToStr);
                    List<String> concurrentTo = wid.header.warcConcurrentToList.stream()
                            .map(c -> stripWarcId(c.warcConcurrentToStr))
                            .collect(Collectors.toList());

                    assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                            .as("Missing crawllog entry for WARC record %s, record type %s, target %s",
                                    wid.header.warcRecordIdStr, wid.header.warcTypeStr, wid.header.warcTargetUriStr)
                            .contains(warcId);
                    if (!refersTo.isEmpty()) {
                        assertThat(crawlLogs.stream().map(c -> c.getWarcId()))
                                .as("Missing crawllog entry for WARC record %s's warcRefersTo %s", wid, refersTo)
                                .contains(refersTo);
                        assertThat(warcRecords.keySet().stream())
                                .as("Missing referred WARC record for WARC record %s's warcRefersTo %s", wid, refersTo)
                                .contains(refersTo);
                        assertThat(refersTo)
                                .as("Warc record '%s' refers to itself", warcId)
                                .isNotEqualTo(warcId);
                    }
                    if (!concurrentTo.isEmpty()) {
                        assertThat(warcRecords.keySet().stream())
                                .as("Missing referred WARC record for WARC record %s's concurrentTo %s", wid, concurrentTo)
                                .containsAll(concurrentTo);
                        assertThat(concurrentTo)
                                .as("Warc record '%s' is concurrent to itself", warcId)
                                .doesNotContain(warcId);
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
                    .as("Block digest for crawllog entry %s (uri:%s) was empty",
                            cl.getWarcId(), cl.getRequestedUri())
                    .isNotEmpty();
            assertThat(cl.getPayloadDigest())
                    .as("Payload digest for crawllog entry %s (uri:%s) was empty",
                            cl.getWarcId(), cl.getRequestedUri())
                    .isNotEmpty();
        });
        warcRecords.values().stream()
                .filter(w -> ((!"metadata".equals(w.header.warcTypeStr)) && (!"warcinfo".equals(w.header.warcTypeStr))))
                .forEach(w -> {
                    assertThat(w.header.warcBlockDigestStr)
                            .as("Block digest for WARC entry %s (uri:%s) was empty",
                                    w.header.warcRecordIdStr, w.header.warcTargetUriStr)
                            .isNotEmpty();
                    assertThat(w.header.warcPayloadDigestStr)
                            .as("Payload digest for WARC entry %s (uri:%s) was empty",
                                    w.header.warcRecordIdStr, w.header.warcTargetUriStr)
                            .isNotEmpty();
                });
    }

    private void checkValidWarc() {
        warcRecords.values().forEach(r -> {
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
            assertThat(r.isCompliant())
                    .as("Record is not compliant. Uri: %s, type: %s", r.header.warcTargetUriStr, r.header.warcTypeStr)
                    .isTrue();
        });
    }

    private void init() {
        crawlLogs = db.listCrawlLogs(CrawlLogListRequest.newBuilder().setPageSize(500).build()).getValueList();
        pageLogs = db.listPageLogs(PageLogListRequest.newBuilder().setPageSize(500).build()).getValueList();
        warcRecords = new HashMap<>();

        WarcInspector.getWarcFiles().getRecordStream()
                .forEach(r -> {
                    try {
                        r.close();
                    } catch (IOException e) {
                        fail("Failed closing record", e);
                    }
                    WarcRecord existing = warcRecords.put(r.header.warcRecordIdStr.substring(10, r.header.warcRecordIdStr.lastIndexOf(">")), r);
                    assertThat(existing)
                            .as("Duplicate WARC record id %s", r.header.warcRecordIdStr)
                            .isNull();
                });
    }

    private String stripWarcId(String warcUrn) {
        if (warcUrn == null || warcUrn.isEmpty()) {
            return "";
        }
        return warcUrn.substring(10, warcUrn.lastIndexOf(">"));
    }
}
