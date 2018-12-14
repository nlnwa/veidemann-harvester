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
package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScope;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbCrawlQueueAdapterIT {
    public static RethinkDbCrawlQueueAdapter queueAdapter;
    public static RethinkDbAdapter dbAdapter;
    static final RethinkDB r = RethinkDB.r;

    private final static int THREAD_COUNT = 8;
    private final static int QURI_COUNT = 500;
    private final int CRAWL_HOST_GROUP_COUNT = 20;

    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    boolean shouldRun = true;

    CountDownLatch finishLatch = new CountDownLatch(QURI_COUNT);

    @Before
    public void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();

        queueAdapter = (RethinkDbCrawlQueueAdapter) DbService.getInstance().getCrawlQueueAdapter();
        dbAdapter = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void testReleaseCrawlHostGroup() throws DbException, InterruptedException {
        RethinkDbConnection conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();

        QueuedUri qUri1 = QueuedUri.newBuilder().setCrawlHostGroupId("crawlHostGroupId1").setPolitenessId("politenessId").setSequence(1).build();
        QueuedUri qUri2 = QueuedUri.newBuilder().setCrawlHostGroupId("crawlHostGroupId2").setPolitenessId("politenessId").setSequence(1).build();
        queueAdapter.addToCrawlHostGroup(qUri1);
        queueAdapter.addToCrawlHostGroup(qUri2);

        FutureOptional<CrawlHostGroup> b1 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b2 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        queueAdapter.releaseCrawlHostGroup(b1.get(), 2000);
        OffsetDateTime now = OffsetDateTime.now();
        queueAdapter.releaseCrawlHostGroup(b2.get(), 0);

        FutureOptional<CrawlHostGroup> b3 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b4 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        Map<String, Object> r1 = conn.exec(r.table(Tables.CRAWL_HOST_GROUP.name).get(r.array("crawlHostGroupId1", "politenessId")));
        assertThat(r1.get("busy")).isEqualTo(false);
        assertThat((OffsetDateTime) r1.get("nextFetchTime")).isAfter(now);

        Map<String, Object> r2 = conn.exec(r.table(Tables.CRAWL_HOST_GROUP.name).get(r.array("crawlHostGroupId2", "politenessId")));
        assertThat(r2.get("busy")).isEqualTo(true);
        assertThat((OffsetDateTime) r2.get("nextFetchTime")).isBeforeOrEqualTo(OffsetDateTime.now(ZoneId.of("UTC")));

        assertThat(b3.isPresent()).isTrue();
        assertThat(b3.get().getId()).isEqualTo("crawlHostGroupId2");
        assertThat(b4.isPresent()).isFalse();
        assertThat(b4.isMaybeInFuture()).isFalse();

        // Sleep enough to ensure next crawl host group is ready for fetch
        Thread.sleep(2000);

        FutureOptional<CrawlHostGroup> b5 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b6 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        assertThat(b5.isPresent()).isTrue();
        assertThat(b5.get().getId()).isEqualTo("crawlHostGroupId1");
        assertThat(b6.isPresent()).isFalse();
        assertThat(b6.isMaybeInFuture()).isFalse();
        assertThat(b6.isEmpty()).isTrue();

        // Set expires to now() to check that stale crawl host groups would be reused
        conn.exec(r.table(Tables.CRAWL_HOST_GROUP.name)
                .get(r.array("crawlHostGroupId2", "politenessId"))
                .update(r.hashMap("expires", r.now())));

        FutureOptional<CrawlHostGroup> b7 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b8 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        assertThat(b7.isPresent()).isTrue();
        assertThat(b7.get().getId()).isEqualTo("crawlHostGroupId2");
        assertThat(b8.isPresent()).isFalse();
        assertThat(b8.isMaybeInFuture()).isFalse();
        assertThat(b8.isEmpty()).isTrue();

    }

    @Test
    public void testGetNextQueuedUriToFetch() throws DbException {
        QueuedUri qUri = QueuedUri.newBuilder()
                .setUri("http://www.foo.bar")
                .setIp("127.0.0.1")
                .setSequence(1L)
                .setCrawlHostGroupId("CHGID")
                .setPolitenessId("PID")
                .setExecutionId("EID")
                .setPriorityWeight(1.0)
                .build();

        queueAdapter.addToCrawlHostGroup(qUri);
        FutureOptional<CrawlHostGroup> b1 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        FutureOptional<QueuedUri> foqu = queueAdapter.getNextQueuedUriToFetch(b1.get());
        assertThat(foqu.isPresent()).isTrue();
    }

//    @Test
//    public void testGetOrCreateCrawlHostGroup() throws DbException {
//        QueuedUri qUri1 = QueuedUri.newBuilder().setCrawlHostGroupId("crawlHostGroupId1").setPolitenessId("politenessId").build();
//        QueuedUri qUri2 = QueuedUri.newBuilder().setCrawlHostGroupId("crawlHostGroupId2").setPolitenessId("politenessId").build();
//        CrawlHostGroup chg1 = db.addToCrawlHostGroup(qUri1);
//        CrawlHostGroup chg2 = db.addToCrawlHostGroup(qUri1);
//        CrawlHostGroup chg3 = db.addToCrawlHostGroup(qUri2);
//
//        assertThat(chg1.getId()).isEqualTo(chg2.getId()).isEqualTo("crawlHostGroupId1");
//        assertThat(chg1.getPolitenessId()).isEqualTo(chg2.getPolitenessId()).isEqualTo("politenessId");
//        assertThat(chg1.getBusy()).isEqualTo(chg2.getBusy()).isFalse();
//        assertThat(chg1.getQueuedUriCount()).isEqualTo(1);
//        assertThat(chg2.getQueuedUriCount()).isEqualTo(2);
//
//        assertThat(chg3.getId()).isEqualTo("crawlHostGroupId2");
//        assertThat(chg3.getPolitenessId()).isEqualTo("politenessId");
//        assertThat(chg3.getBusy()).isFalse();
//        assertThat(chg3.getQueuedUriCount()).isEqualTo(1);
//    }

    @Test
    public void testBorrowFirstReadyCrawlHostGroup() throws DbException {
        QueuedUri qUri1 = QueuedUri.newBuilder()
                .setCrawlHostGroupId("crawlHostGroupId1")
                .setPolitenessId("politenessId")
                .setSequence(1)
                .setPriorityWeight(1.0)
                .build();
        QueuedUri qUri2 = QueuedUri.newBuilder()
                .setCrawlHostGroupId("crawlHostGroupId2")
                .setPolitenessId("politenessId")
                .setSequence(1)
                .setPriorityWeight(1.0)
                .build();
        queueAdapter.addToCrawlHostGroup(qUri1);
        queueAdapter.addToCrawlHostGroup(qUri2);

        FutureOptional<CrawlHostGroup> b1 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b2 = queueAdapter.borrowFirstReadyCrawlHostGroup();
        FutureOptional<CrawlHostGroup> b3 = queueAdapter.borrowFirstReadyCrawlHostGroup();

        assertThat(b1.isPresent()).isTrue();
        assertThat(b2.isPresent()).isTrue();
        assertThat(b3.isPresent()).isFalse();

        assertThat(b1.get().getId()).isEqualTo("crawlHostGroupId1");
        assertThat(b1.get().getBusy()).isTrue();
        assertThat(ProtoUtils.tsToOdt(b1.get().getNextFetchTime())).isBeforeOrEqualTo(OffsetDateTime.now());

        assertThat(b2.get().getId()).isEqualTo("crawlHostGroupId2");
        assertThat(b2.get().getBusy()).isTrue();
        assertThat(ProtoUtils.tsToOdt(b2.get().getNextFetchTime())).isBeforeOrEqualTo(OffsetDateTime.now());
    }

    @Test
    public void testDeleteQueuedUrisForExecution() throws DbException {
        System.setProperty(RethinkDbConnection.RETHINK_ARRAY_LIMIT_KEY, "25");
        for (int i = 0; i < 50; i++) {
            QueuedUri qUri = QueuedUri.newBuilder()
                    .setExecutionId("executionId2")
                    .setCrawlHostGroupId("crawlHostGroupId1")
                    .setPolitenessId("politenessId")
                    .setSequence(1)
                    .build();
            queueAdapter.addToCrawlHostGroup(qUri);
        }
        for (int i = 0; i < 50; i++) {
            QueuedUri qUri = QueuedUri.newBuilder()
                    .setExecutionId("executionId1")
                    .setCrawlHostGroupId("crawlHostGroupId1")
                    .setPolitenessId("politenessId")
                    .setSequence(1)
                    .build();
            queueAdapter.addToCrawlHostGroup(qUri);
        }
        for (int i = 0; i < 50; i++) {
            QueuedUri qUri = QueuedUri.newBuilder()
                    .setExecutionId("executionId1")
                    .setCrawlHostGroupId("crawlHostGroupId2")
                    .setPolitenessId("politenessId")
                    .setSequence(1)
                    .build();
            queueAdapter.addToCrawlHostGroup(qUri);
        }

        assertThat(queueAdapter.deleteQueuedUrisForExecution("executionId1")).isEqualTo(100L);
        System.clearProperty(RethinkDbConnection.RETHINK_ARRAY_LIMIT_KEY);
    }

    @Test
    public void testConsistency() throws DbException, InterruptedException {
        String politenessId = "PolitenessId";

        crawlLoop();
        int qc = 0;
        while (qc < QURI_COUNT) {
            for (int i = 0; i < CRAWL_HOST_GROUP_COUNT && qc++ < QURI_COUNT; i++) {
                String chgId = "chg" + i;
                CrawlExecutionStatus ces = DbService.getInstance().getExecutionsAdapter().createCrawlExecutionStatus(
                        "jobId", "jobExecutionId", "seedId", CrawlScope.getDefaultInstance());
                QueuedUri qUri = QueuedUri.newBuilder()
                        .setCrawlHostGroupId(chgId)
                        .setPolitenessId(politenessId)
                        .setSequence(1)
                        .setExecutionId(ces.getId())
                        .setJobExecutionId(ces.getJobExecutionId())
                        .setPriorityWeight(1.0)
                        .build();
                qUri = queueAdapter.addToCrawlHostGroup(qUri);
            }
        }
        finishLatch.await();
        Thread.sleep(1000L);
        shouldRun = false;
        System.out.println();

        try (Cursor<Map<String, Object>> response = dbAdapter.executeRequest("test",
                r.table(Tables.CRAWL_HOST_GROUP.name));) {
            assertThat(finishLatch.getCount()).isZero();
            assertThat(response.iterator()).isEmpty();
        }

        pool.shutdownNow();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
    }

    private void crawlLoop() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            pool.submit(new QueueWorkerMock());
        }
    }

    private class QueueWorkerMock implements Runnable {
        private static final long RESCHEDULE_DELAY = 1;

        public Exe getNextToFetch() throws InterruptedException {
            long sleep = 0L;

            while (shouldRun) {
                try {
                    FutureOptional<CrawlHostGroup> crawlHostGroup = queueAdapter.borrowFirstReadyCrawlHostGroup();

                    if (crawlHostGroup.isMaybeInFuture()) {
                        // A CrawlHostGroup suitable for execution in the future was found, wait until it is ready.
                        sleep = crawlHostGroup.getDelayMs();
                    } else if (crawlHostGroup.isPresent()) {
                        assertThat(crawlHostGroup.get().getQueuedUriCount()).isGreaterThan(-1L);
                        FutureOptional<MessagesProto.QueuedUri> foqu = queueAdapter.getNextQueuedUriToFetch(crawlHostGroup.get());

                        if (foqu.isPresent()) {
                            // A fetchabel URI was found, return it
                            return new Exe(crawlHostGroup.get(), foqu.get(), foqu.get().getExecutionId());
                        } else if (foqu.isMaybeInFuture()) {
                            // A URI was found, but isn't fetchable yet. Wait for it
                            sleep = (foqu.getDelayMs());
                        } else {
                            // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                            sleep = RESCHEDULE_DELAY;
                        }
                        queueAdapter.releaseCrawlHostGroup(crawlHostGroup.get(), sleep);
                    } else {
                        // No CrawlHostGroup ready. Wait a moment and try again
                        sleep = RESCHEDULE_DELAY;
                    }
                } catch (DbException e) {
                    sleep = RESCHEDULE_DELAY;
                }

                Thread.sleep(sleep);
            }
            throw new InterruptedException();
        }

        @Override
        public void run() {
            try {
                Random rnd = new Random();
                while (true) {
                    Exe exe = getNextToFetch();
                    System.out.print(".");

                    long crawlTime = rnd.nextInt(200) + 20L;
                    Thread.sleep(crawlTime);

                    DbService.getInstance().getExecutionsAdapter().updateCrawlExecutionStatus(
                            CrawlExecutionStatusChange.newBuilder()
                                    .setId(exe.exeId)
                                    .setDeleteCurrentUri(exe.qUri)
                                    .build());
                    queueAdapter.releaseCrawlHostGroup(exe.chg, crawlTime);

                    assertThat(finishLatch.getCount()).isGreaterThan(0);
                    finishLatch.countDown();
                }
            } catch (InterruptedException e) {
                // OK
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private class Exe {
            private final CrawlHostGroup chg;
            private final QueuedUri qUri;
            private final String exeId;

            public Exe(CrawlHostGroup chg, QueuedUri qUri, String exeId) {
                this.chg = chg;
                this.qUri = qUri;
                this.exeId = exeId;
            }
        }
    }
}