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
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.initializer.DbInitializer;
import org.junit.BeforeClass;
import org.junit.Test;

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
//@Ignore
public class CrawlHostGroupTestIT {
    public static RethinkDbAdapter db;
    static final RethinkDB r = RethinkDB.r;

    private final static int THREAD_COUNT = 10;
    private final static int QURI_COUNT = 500;
    private final int CRAWL_HOST_GROUP_COUNT = 20;

    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    boolean shouldRun = true;

    CountDownLatch finishLatch = new CountDownLatch(QURI_COUNT);

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        if (!RethinkDbConnection.isConfigured()) {
            RethinkDbConnection.configure(dbHost, dbPort, "veidemann", "admin", "");
        }
        db = new RethinkDbAdapter();

        RethinkDB r = RethinkDB.r;
        try {
            RethinkDbConnection.getInstance().exec(r.dbDrop("veidemann"));
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        new DbInitializer().initialize();
    }

    @Test
    public void testConsistency() throws DbException, InterruptedException {
        String politenessId = "PolitenessId";

        crawlLoop();
        int qc = 0;
        while (qc < QURI_COUNT) {
            for (int i = 0; i < CRAWL_HOST_GROUP_COUNT && qc++ < QURI_COUNT; i++) {
                String chgId = "chg" + i;
                QueuedUri qUri = db.saveQueuedUri(QueuedUri.newBuilder()
                        .setCrawlHostGroupId(chgId)
                        .setPolitenessId(politenessId)
                        .setSequence(1)
                        .build());
                CrawlHostGroup chg = db.addToCrawlHostGroup(qUri);
            }
        }
        finishLatch.await();
        Thread.sleep(1000L);
        shouldRun = false;

        Cursor<Map<String, Object>> response = db.executeRequest("test",
                r.table(TABLES.CRAWL_HOST_GROUP.name));
        assertThat(finishLatch.getCount()).isZero();
        assertThat(response.iterator()).isEmpty();

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
                    FutureOptional<CrawlHostGroup> crawlHostGroup = db.borrowFirstReadyCrawlHostGroup();

                    if (crawlHostGroup.isMaybeInFuture()) {
                        // A CrawlHostGroup suitable for execution in the future was found, wait until it is ready.
                        sleep = crawlHostGroup.getDelayMs();
                    } else if (crawlHostGroup.isPresent()) {
                        assertThat(crawlHostGroup.get().getQueuedUriCount()).isGreaterThan(-1L);
                        FutureOptional<MessagesProto.QueuedUri> foqu = db.getNextQueuedUriToFetch(crawlHostGroup.get());

                        if (foqu.isPresent()) {
                            // A fetchabel URI was found, return it
                            return new Exe(crawlHostGroup.get(), foqu.get());
                        } else if (foqu.isMaybeInFuture()) {
                            // A URI was found, but isn't fetchable yet. Wait for it
                            sleep = (foqu.getDelayMs());
                        } else {
                            // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                            sleep = RESCHEDULE_DELAY;
                        }
                        db.releaseCrawlHostGroup(crawlHostGroup.get(), sleep, false);
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

                    db.deleteQueuedUri(exe.qUri);

                    long crawlTime = rnd.nextInt(200) + 20L;
                    Thread.sleep(crawlTime);

                    CrawlHostGroup chg = db.releaseCrawlHostGroup(exe.chg, crawlTime, true);
                    assertThat(chg.getQueuedUriCount()).isGreaterThan(-1L);

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

            public Exe(CrawlHostGroup chg, QueuedUri qUri) {
                this.chg = chg;
                this.qUri = qUri;
            }
        }
    }
}