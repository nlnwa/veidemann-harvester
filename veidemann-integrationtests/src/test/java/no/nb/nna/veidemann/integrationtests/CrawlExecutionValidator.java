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

import no.nb.nna.veidemann.api.ReportProto.ExecuteDbQueryRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.PageLog;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CrawlExecutionValidator {
    final RethinkDbAdapter db;
    final ConfigAdapter configAdapter;

    CrawlLogHelper crawlLogs;
    CrawlExecutionsHelper crawlExecutions;
    List<PageLog> pageLogs = new ArrayList<>();
    Map<String, WarcRecord> warcRecords;
    Map<String, WarcRecord> warcScreenshotRecords;

    final JobExecutionStatus jobExecutionStatus;

    public CrawlExecutionValidator(JobExecutionStatus jobExecutionStatus) {
        this.db = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();
        this.configAdapter = DbService.getInstance().getConfigAdapter();
        this.jobExecutionStatus = jobExecutionStatus;
    }

    public CrawlExecutionValidator validate() throws DbException, InterruptedException {
        init();

        System.out.println("\nCrawllogs");
        crawlLogs.getCrawlLog().forEach(l -> System.out.println(l.getJobExecutionId() + " :: " + l.getCollectionFinalName() + " :: " + l.getStatusCode() + " :: " + l.getRecordType() + " :: " + l.getRequestedUri() + " :: " + l.getWarcId()));

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

    public CrawlExecutionValidator checkCrawlLogCount(String type, int minExpectedSize, int maxExpectedSize) {
        assertThat(crawlLogs.getTypeCount(type))
                .as("Wrong number of crawl log records of type %s", type)
                .isBetween(minExpectedSize, maxExpectedSize);
        return this;
    }

    public CrawlExecutionValidator checkCrawlLogCount(int expectedSize, String... types) {
        int count = 0;
        for (String t : types) {
            count += crawlLogs.getTypeCount(t);
        }
        assertThat(count)
                .as("Wrong number of crawl log records for types %s", types)
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
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.CONNECT_FAILED.getCode())
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode())
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
        pageLogs.stream()
                .filter(pl -> pl.getResource(0).getStatusCode() != ExtraStatusCodes.CONNECT_FAILED.getCode())
                .forEach(pl -> {
                    assertThat(warcRecords.keySet())
                            .as("Missing WARC record for pagelog entry %s --- %s", pl.getWarcId(), pl)
                            .contains(pl.getWarcId());
                    pl.getResourceList().stream()
                            .filter(r -> !r.getFromCache())
                            .filter(r -> r.getStatusCode() != ExtraStatusCodes.CONNECT_FAILED.getCode())
                            .filter(r -> r.getStatusCode() != ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode())
                            .forEach(r -> {
                                assertThat(warcRecords.keySet())
                                        .as("Missing WARC record for crawllog resource entry %s with uri: %s",
                                                r.getWarcId(), r.getUri())
                                        .contains(r.getWarcId());
                            });
                });
        warcRecords.values().stream()
                .peek(wid -> {
                    try {
                        StorageRef sr = db.getStorageRef(stripWarcId(wid.header.warcRecordIdStr));
                        assertThat(db.getStorageRef(stripWarcId(wid.header.warcRecordIdStr)))
                                .as("StorageRef not found for WARC record %s (uri: %s, type: %s)",
                                        stripWarcId(wid.header.warcRecordIdStr),
                                        wid.header.warcTargetUriStr,
                                        wid.header.warcTypeStr)
                                .isNotNull();
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(wid -> ("response".equals(wid.header.warcTypeStr)
                        || "revisit".equals(wid.header.warcTypeStr)
                ))
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
                        assertThat(wid.header.warcTypeStr)
                                .as("Warc record '%s' refers to another record, record type is not revisit. Actual type is %s",
                                        warcId, wid.header.warcTypeStr)
                                .isEqualTo("revisit");
                        assertThat(crawlLogs.getCrawlLogEntry(refersTo))
                                .as("Missing crawllog entry for WARC record %s's warcRefersTo %s", wid, refersTo)
                                .isNotNull();
                        assertThat(warcRecords.keySet().stream())
                                .as("Missing referred WARC record for WARC record %s's warcRefersTo %s", wid, refersTo)
                                .contains(refersTo);
                        assertThat(refersTo)
                                .as("Warc record '%s' refers to itself", warcId)
                                .isNotEqualTo(warcId);
                        assertThat(wid.header.warcRefersToTargetUriStr)
                                .as("WARC record %s's warcRefersToTargetUri is empty for a revisit", wid)
                                .isNotEmpty();
                        assertThat(wid.header.warcRefersToDateStr)
                                .as("WARC record %s's warcRefersToDate is empty for a revisit", wid)
                                .isNotEmpty();
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
        warcScreenshotRecords.values().stream()
                .peek(wid -> {
                    try {
                        StorageRef sr = db.getStorageRef(stripWarcId(wid.header.warcRecordIdStr));
                        assertThat(db.getStorageRef(stripWarcId(wid.header.warcRecordIdStr)))
                                .as("StorageRef not found for WARC record %s (uri: %s, type: %s)",
                                        stripWarcId(wid.header.warcRecordIdStr),
                                        wid.header.warcTargetUriStr,
                                        wid.header.warcTypeStr)
                                .isNotNull();
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(wid -> {
                    String warcId = stripWarcId(wid.header.warcRecordIdStr);
                    String refersTo = stripWarcId(wid.header.warcRefersToStr);
                    List<String> concurrentTo = wid.header.warcConcurrentToList.stream()
                            .map(c -> stripWarcId(c.warcConcurrentToStr))
                            .collect(Collectors.toList());

                    if (!refersTo.isEmpty()) {
                        assertThat(wid.header.warcTypeStr)
                                .as("Warc screenshot record '%s' refers to another record, record type is not revisit. Actual type is %s",
                                        warcId, wid.header.warcTypeStr)
                                .isEqualTo("revisit");
                        assertThat(warcScreenshotRecords.keySet().stream())
                                .as("Missing referred WARC record for WARC screenshot record %s's warcRefersTo %s", wid, refersTo)
                                .contains(refersTo);
                        assertThat(refersTo)
                                .as("Warc screenshot record '%s' refers to itself", warcId)
                                .isNotEqualTo(warcId);
                        assertThat(wid.header.warcRefersToTargetUriStr)
                                .as("WARC screenshot record %s's warcRefersToTargetUri is empty for a revisit", wid)
                                .isNotEmpty();
                        assertThat(wid.header.warcRefersToDateStr)
                                .as("WARC screenshot record %s's warcRefersToDate is empty for a revisit", wid)
                                .isNotEmpty();
                    }
                    if (!concurrentTo.isEmpty()) {
                        assertThat(Stream.concat(warcScreenshotRecords.keySet().stream(), warcRecords.keySet().stream()))
                                .as("Missing referred WARC record for WARC screenshot record %s's concurrentTo %s", wid, concurrentTo)
                                .containsAll(concurrentTo);
                        assertThat(concurrentTo)
                                .as("Warc scrrenshot record '%s' is concurrent to itself", warcId)
                                .doesNotContain(warcId);
                    }
                });
        crawlExecutions.getCrawlExecutionStatus().forEach(ces -> {
            System.out.println(CrawlExecutionsHelper.formatCrawlExecution(ces));
            List<CrawlLog> logs = crawlLogs.getCrawlLogsByExecutionId(ces.getId());
            assertThat(logs.stream()
                    .filter(cl -> (cl.getStatusCode() != ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode()
                            && !cl.getRequestedUri().endsWith("robots.txt")))
                    .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode())
                    .count())
                    .as("Mismatch between CrawlExecutionStatus.getUrisCrawled and CrawlLog count. CrawlExecutionStatus.getUrisCrawled was %d", ces.getUrisCrawled())
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
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode())
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
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.CONNECT_FAILED.getCode())
                .filter(cl -> cl.getStatusCode() != ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode())
                .forEach(cl -> {
                    assertThat(cl.getBlockDigest())
                            .as("Block digest for crawllog entry %s (uri:%s) was empty",
                                    cl.getWarcId(), cl.getRequestedUri())
                            .isNotEmpty();
                    if (cl.getContentType().equals("text/dns")) {
                        assertThat(cl.getPayloadDigest())
                                .as("Payload digest for crawllog entry %s (uri:%s) should be empty, was: %s",
                                        cl.getWarcId(), cl.getRequestedUri(), cl.getPayloadDigest())
                                .isNullOrEmpty();
                    } else {
                        assertThat(cl.getPayloadDigest())
                                .as("Payload digest for crawllog entry %s (uri:%s) was empty",
                                        cl.getWarcId(), cl.getRequestedUri())
                                .isNotEmpty();
                    }
                });
        warcRecords.values().stream()
                .filter(w -> ((!"metadata".equals(w.header.warcTypeStr)) && (!"warcinfo".equals(w.header.warcTypeStr))))
                .forEach(w -> {
                    assertThat(w.header.warcBlockDigestStr)
                            .as("Block digest for WARC entry %s (uri:%s, content-type: %s) was empty",
                                    w.header.warcRecordIdStr, w.header.warcTargetUriStr)
                            .isNotEmpty();
                    if (w.header.contentTypeStr.startsWith("application/http")) {
                        assertThat(w.header.warcPayloadDigestStr)
                                .as("Payload digest for WARC entry %s (uri:%s, content-type: %s) was empty",
                                        w.header.warcRecordIdStr, w.header.warcTargetUriStr, w.header.contentTypeStr)
                                .isNotEmpty();
                    } else {
                        assertThat(w.header.warcPayloadDigestStr)
                                .as("Payload digest for WARC entry %s (uri:%s, content-type: %s) should be empty, was: %s",
                                        w.header.warcRecordIdStr, w.header.warcTargetUriStr, w.header.contentTypeStr, w.header.warcPayloadDigestStr)
                                .isNullOrEmpty();
                    }
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
        ConfigObject job = configAdapter.getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob)
                .setId(jobExecutionStatus.getJobId()).build());
        ConfigObject crawlConfig = configAdapter.getConfigObject(job.getCrawlJob().getCrawlConfigRef());
        ConfigObject collection = configAdapter.getConfigObject(crawlConfig.getCrawlConfig().getCollectionRef());

        crawlLogs = new CrawlLogHelper(collection.getMeta().getName());
        db.listPageLogs(PageLogListRequest.newBuilder().setPageSize(5000).build()).getValueList()
                .stream()
                .filter(pl -> pl.getCollectionFinalName().startsWith(collection.getMeta().getName()))
                .forEach(pl -> pageLogs.add(pl));
        crawlExecutions = new CrawlExecutionsHelper(jobExecutionStatus.getId());

        warcRecords = new HashMap<>();
        String warcRegex = collection.getMeta().getName() + "((-)|(_dns)).*\\.warc.*";
        WarcInspector.getWarcFiles(warcRegex).listFiles().forEach(f -> System.out.println("Warc file: " + f.getName() + ", size: " + f.getSize()));
        WarcInspector.getWarcFiles(warcRegex).getRecordStream()
                .forEach(w -> {
                    try {
                        w.close();
                    } catch (IOException e) {
                        fail("Failed closing record", e);
                    }

                    String warcId = stripWarcId(w.header.warcRecordIdStr);
                    WarcRecord existing = warcRecords.put(warcId, w);
                    assertThat(existing)
                            .as("Duplicate WARC record id %s", w.header.warcRecordIdStr)
                            .isNull();
                });
        warcScreenshotRecords = new HashMap<>();
        String warcScreenshotRegex = collection.getMeta().getName() + "_screenshot.*\\.warc.*";
        WarcInspector.getWarcFiles(warcScreenshotRegex).listFiles().forEach(f -> System.out.println("Warc file: " + f.getName() + ", size: " + f.getSize()));
        WarcInspector.getWarcFiles(warcScreenshotRegex).getRecordStream()
                .forEach(w -> {
                    try {
                        w.close();
                    } catch (IOException e) {
                        fail("Failed closing record", e);
                    }

                    String warcId = stripWarcId(w.header.warcRecordIdStr);
                    WarcRecord existing = warcScreenshotRecords.put(warcId, w);
                    assertThat(existing)
                            .as("Duplicate WARC record id %s", w.header.warcRecordIdStr)
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
