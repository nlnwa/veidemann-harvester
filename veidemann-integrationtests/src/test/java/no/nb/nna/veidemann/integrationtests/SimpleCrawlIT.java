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
package no.nb.nna.veidemann.integrationtests;

import com.google.protobuf.Empty;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class SimpleCrawlIT implements VeidemannHeaderConstants {

    static ManagedChannel contentWriterChannel;

    static ManagedChannel controllerChannel;

    static ControllerGrpc.ControllerBlockingStub controllerClient;

    static ContentWriterGrpc.ContentWriterBlockingStub contentWriterClient;

    static RethinkDbAdapter db;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() throws InterruptedException {
        String controllerHost = System.getProperty("controller.host");
        int controllerPort = Integer.parseInt(System.getProperty("controller.port"));
        String contentWriterHost = System.getProperty("contentwriter.host");
        int contentWriterPort = Integer.parseInt(System.getProperty("contentwriter.port"));
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        System.out.println("Database address: " + dbHost + ":" + dbPort);

        controllerChannel = ManagedChannelBuilder.forAddress(controllerHost, controllerPort).usePlaintext(true).build();
        controllerClient = ControllerGrpc.newBlockingStub(controllerChannel).withWaitForReady();

        contentWriterChannel = ManagedChannelBuilder.forAddress(contentWriterHost, contentWriterPort).usePlaintext(true)
                .build();
        contentWriterClient = ContentWriterGrpc.newBlockingStub(contentWriterChannel).withWaitForReady();

        db = new RethinkDbAdapter(dbHost, dbPort, "veidemann");
    }

    @AfterClass
    public static void shutdown() {
        if (contentWriterChannel != null) {
            contentWriterChannel.shutdown();
        }
        if (controllerChannel != null) {
            controllerChannel.shutdown();
        }
        if (db != null) {
            db.close();
        }
    }

    @After
    public void cleanup() {
        contentWriterClient.delete(Empty.getDefaultInstance());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.CRAWLED_CONTENT.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.CRAWL_LOG.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.PAGE_LOG.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.EXECUTIONS.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.EXTRACTED_TEXT.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.SCREENSHOT.name).delete());
        db.executeRequest("delete", r.table(RethinkDbAdapter.TABLES.URI_QUEUE.name).delete());
    }

    @Test
    public void testHarvest() throws InterruptedException, ExecutionException {
        Thread.sleep(5000);
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.CrawlJobListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setDepth(10).setMaxDurationS(90).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);
        String jobId = job.getId();

        ConfigProto.CrawlEntity entity = ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                .setName("Test entity 1")).build();
        entity = controllerClient.saveEntity(entity);
        ConfigProto.Seed seed = ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://a1.com"))
//                .setMeta(ConfigProto.Meta.newBuilder().setName("https://www.nb.no"))
                .setEntityId(entity.getId())
                .addJobId(jobId)
                .build();
        seed = controllerClient.saveSeed(seed);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();

        executeJob(request).get();

        // TODO: check these values instead of just printing
        System.out.println("WARC RECORDS");
        WarcInspector.getWarcFiles().getRecordStream().forEach(r -> System.out.println(r.header.warcTypeStr + " -- "
                + r.header.warcTargetUriStr + ", ip: " + r.header.warcIpAddress));

        CrawlLogListReply crawlLog = db.listCrawlLogs(CrawlLogListRequest.newBuilder().setPageSize(100).build());
        PageLogListReply pageLog = db.listPageLogs(PageLogListRequest.getDefaultInstance());

        System.out.println("\nPAGE LOG");
        pageLog.getValueList().forEach(p -> {
            System.out.println(p.getUri());
            p.getResourceList().forEach(r -> System.out.println("  - " + r.getUri() + ", cache: " + r.getFromCache()));
        });

        // The goal is to get as low as 25 when we cache 404, 302, etc
        // assertThat(WarcInspector.getWarcFiles().getRecordCount()).isEqualTo(25L);
        assertThat(WarcInspector.getWarcFiles().getRecordCount()).isEqualTo(43L);

        // TODO: check these values instead of just printing
        System.out.println("\nCRAWL LOG");
        crawlLog.getValueList().forEach(r -> System.out.println(r.getRequestedUri() + " -- " + r.getStatusCode()
                + " -- " + r.getContentType() + " -- " + r.getRecordType() + " -- " + r.getReferrer() + ", ip: " + r.getIpAddress()));

        // The goal is to get as low as 14 when we cache 404, 302, etc
        // assertThat(crawlLog.getCount()).isEqualTo(14L);
        assertThat(crawlLog.getCount()).isEqualTo(22L);
        assertThat(pageLog.getCount()).isEqualTo(6L);

        try {
            new CrawlExecutionValidator(db).validate();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        executeJob(request).get();
        crawlLog = db.listCrawlLogs(CrawlLogListRequest.newBuilder().setPageSize(100).build());

        // TODO: check these values instead of just printing
        System.out.println("---------------");
        crawlLog.getValueList().forEach(r -> System.out.println(r.getRequestedUri() + " -- " + r.getStatusCode()
                + " -- " + r.getContentType() + " -- " + r.getRecordType() + " -- " + r.getReferrer()));

        // The goal is to get as low as 24 when we cache 404, 302, etc
        // assertThat(crawlLog.getCount()).isEqualTo(24);
        assertThat(crawlLog.getCount()).isEqualTo(54);
    }

    JobCompletion executeJob(ControllerProto.RunCrawlRequest crawlRequest) {
        return (JobCompletion) ForkJoinPool.commonPool().submit((ForkJoinTask) new JobCompletion(crawlRequest));
    }

    public class JobCompletion extends ForkJoinTask<Void> implements RunnableFuture<Void> {

        final List<String> eIds;

        final Map<String, CrawlExecutionStatus> executions;

        Void result;

        JobCompletion(ControllerProto.RunCrawlRequest request) {
            ControllerProto.RunCrawlReply crawlReply = controllerClient.runCrawl(request);
            executions = new HashMap<>();
            eIds = new ArrayList<>(crawlReply.getSeedExecutionIdList());
        }

        @Override
        public Void getRawResult() {
            return result;
        }

        @Override
        protected void setRawResult(Void value) {
            result = value;
        }

        @Override
        public void run() {
            invoke();
        }

        @Override
        protected boolean exec() {
            try {
                Cursor<Map<String, Object>> cursor = db.executeRequest("list", r.table(RethinkDbAdapter.TABLES.EXECUTIONS.name)
                        .getAll(eIds.toArray())
                        .changes());

                StreamSupport.stream(cursor.spliterator(), false)
                        .filter(e -> e.containsKey("new_val"))
                        .map(e -> ProtoUtils
                                .rethinkToProto((Map<String, Object>) e.get("new_val"), CrawlExecutionStatus.class))
                        .forEach(e -> {
                            executions.put(e.getId(), e);
                            if (isEnded(e)) {
                                eIds.remove(e.getId());
                                if (eIds.isEmpty()) {
                                    System.out.println("Job completed");
                                    cursor.close();
                                }
                            }
                        });

                result = null;
                return true;
            } catch (Error err) {
                throw err;
            } catch (RuntimeException rex) {
                throw rex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private boolean isEnded(CrawlExecutionStatus execution) {
            switch (execution.getState()) {
                case CREATED:
                case FETCHING:
                case SLEEPING:
                    return false;
                default:
                    System.out.println(execution);
                    return true;
            }
        }

    }

}
