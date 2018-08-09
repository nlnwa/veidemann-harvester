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

import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.ReportProto.ExecuteDbQueryRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
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

    CrawlLogHelper crawlLogs;
    CrawlExecutionsHelper crawlExecutions;
    List<PageLog> pageLogs;
    Map<String, WarcRecord> warcRecords;

    public CrawlExecutionValidator(RethinkDbAdapter db) {
        this.db = db;
    }

    public CrawlExecutionValidator validate() throws DbException, InterruptedException {
        init();

        checkDrainedQueue();
        checkConsistency();
        checkValidWarc();
        checkChecksum();
        checkIp();

        return this;
    }

    public CrawlExecutionValidator checkCrawlLogCount(String type, int expectedSize) {
        assertThat(crawlLogs.getTypeCount(type))
                .as("Wrong number of crawl log records of type %s", type)
                .isEqualTo(expectedSize);
        return this;
    }

    public CrawlExecutionValidator checkPageLogCount(int expectedSize) {
        assertThat(pageLogs.size())
                .as("Wrong number of page log records")
                .isEqualTo(expectedSize);
        return this;
    }

    private void checkDrainedQueue() throws InterruptedException {
        // Check that there is no uri's in queue
        QueryObserver observer = new QueryObserver();
        CrawlTestBase.reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('uri_queue').count()").build(), observer);
        observer.await();
        assertThat(observer.getResults()).hasSize(1);
        assertThat(observer.getResults().get(0))
                .as("There should be no uri's in queue after crawl has ended")
                .isEqualTo("0");
    }

    private void checkConsistency() {
        crawlLogs.getCrawlLog().stream()
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode())
                .forEach(cl -> {
                    assertThat(warcRecords.keySet())
                            .as("Missing WARC record for crawllog entry %s with uri: %s",
                                    cl.getWarcId(), cl.getRequestedUri())
                            .contains(cl.getWarcId());
                    if (!cl.getWarcRefersTo().isEmpty()) {
                        assertThat(crawlLogs.getCrawlLogEntry(cl.getWarcRefersTo()))
                                .as("Missing crawllog entry for record %s's warcRefersTo", cl)
                                .isNotNull();
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

                    assertThat(crawlLogs.getCrawlLogEntry(warcId))
                            .as("Missing crawllog entry for WARC record %s, record type %s, target %s",
                                    wid.header.warcRecordIdStr, wid.header.warcTypeStr, wid.header.warcTargetUriStr)
                            .isNotNull();
                    if (!refersTo.isEmpty()) {
                        assertThat(crawlLogs.getCrawlLogEntry(refersTo))
                                .as("Missing crawllog entry for WARC record %s's warcRefersTo %s", wid, refersTo)
                                .isNotNull();
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
        crawlExecutions.getCrawlExecutionStatus().forEach(ces -> {
            System.out.println(CrawlExecutionsHelper.formatCrawlExecution(ces));
            List<CrawlLog> logs = crawlLogs.getCrawlLogsByExecutionId(ces.getId());
            logs.forEach(c -> System.out.println("  " + c.getStatusCode() + " - " + c.getRequestedUri()));
            assertThat(logs.stream()
                    .filter(cl -> (cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode()
                            && !cl.getRequestedUri().endsWith("robots.txt")))
                    .count())
                    .as("Mismatch between CrawlExecutionStatus.getDocumentsCrawled and CrawlLog count")
                    .isEqualTo((int) ces.getUrisCrawled());

            long summarizedSize = logs.stream()
                    .filter(cl -> (cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode()
                            && !cl.getRequestedUri().endsWith("robots.txt")))
                    .collect(Collectors.summingLong(v -> v.getSize()));
            assertThat(summarizedSize)
                    .as("Mismatch between CrawlExecutionStatus.getBytesCrawled and sum CrawlLogs size")
                    .isEqualTo(ces.getBytesCrawled());
        });
    }

    private void checkIp() {
        crawlLogs.getCrawlLog().stream()
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode())
                .forEach(cl -> {
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
        crawlLogs.getCrawlLog().stream()
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode())
                .forEach(cl -> {
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

            String refersTo = stripWarcId(r.header.warcRefersToStr);
            List<String> concurrentTo = r.header.warcConcurrentToList.stream()
                    .map(c -> stripWarcId(c.warcConcurrentToStr))
                    .collect(Collectors.toList());
            switch (r.header.warcTypeStr) {
                case "request":
                    assertThat(refersTo).isEmpty();
                    break;
                case "response":
                    assertThat(refersTo).isEmpty();
                    break;
                case "revisit":
                    assertThat(refersTo).isNotEmpty();
                    break;
                case "resource":
                    assertThat(refersTo).isEmpty();
                    break;
                case "metadata":
                    break;
                case "warcinfo":
                    assertThat(refersTo).isEmpty();
                    assertThat(concurrentTo).isEmpty();
                    break;
                default:
                    fail("No tests for record of type '%s'", r.header.warcTypeStr);
            }
        });
    }

    private void init() throws DbException {
        crawlLogs = new CrawlLogHelper(db);
        pageLogs = db.listPageLogs(PageLogListRequest.newBuilder().setPageSize(500).build()).getValueList();
        crawlExecutions = new CrawlExecutionsHelper(db);
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
