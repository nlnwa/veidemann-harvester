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
package no.nb.nna.broprox.frontier.worker;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import no.nb.nna.broprox.commons.db.FutureOptional;
import no.nb.nna.broprox.model.MessagesProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueueWorker extends ThreadPoolExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(QueueWorker.class);

    private static final long RESCHEDULE_DELAY = 2000;

    private final Frontier frontier;

    private boolean isThreadsExhausted;

    private final ReentrantLock threadsExhaustedLock = new ReentrantLock();

    private final Condition availableThread = threadsExhaustedLock.newCondition();

    private final Thread queueWatcher;

    public QueueWorker(Frontier frontier, int numThreads) {
        super(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        prestartAllCoreThreads();
        this.frontier = frontier;
        this.queueWatcher = new Thread(new QueueWatcher());
        this.queueWatcher.start();
    }

    public void waitForReadyThread() {
        threadsExhaustedLock.lock();
        try {
            while (isThreadsExhausted) {
                availableThread.await();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            threadsExhaustedLock.unlock();
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (getActiveCount() >= getCorePoolSize()) {
            threadsExhaustedLock.lock();
            try {
                isThreadsExhausted = true;
            } finally {
                threadsExhaustedLock.unlock();
            }
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (getActiveCount() < getCorePoolSize()) {
            threadsExhaustedLock.lock();
            try {
                isThreadsExhausted = false;
                availableThread.signalAll();
            } finally {
                threadsExhaustedLock.unlock();
            }
        }
    }

    private class QueueWatcher implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    LOG.trace("Waiting for ready thread");
                    waitForReadyThread();

                    LOG.trace("Waiting for next execution to be ready");
                    CrawlExecution exe = getNextToFetch();

                    processExecution(exe);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                } catch (Throwable t) {
                    LOG.error(t.toString(), t);
                    throw new RuntimeException(t);
                }
            }
        }

        private void processExecution(CrawlExecution exe) {
            try {
                // Execute fetch
                LOG.debug("Running next fetch of exexcution: {}", exe.getId());
                submit(exe);
                LOG.debug("End of Link crawl");
            } catch (Throwable t) {
                LOG.error(t.toString(), t);
                throw new RuntimeException(t);
            }
        }

        /**
         * Get the next Uri to fetch.
         * <p>
         * Waits until there is something to fetch.
         * <p>
         * The returned FutureOptional has three possible states:
         * <ul>
         * <li>{@link FutureOptional#isPresent()} returns true: There is a Uri ready for harvesting
         * <li>{@link FutureOptional#isMaybeInFuture()} returns true: There are one or more Uris in the queue, but it is
         * not ready for harvesting yet
         * <li>{@link FutureOptional#isEmpty()} returns true: No more non-busy CrawlHostGroups are found or there are no
         * more Uris linked to the group.
         *
         * @return
         */
        private CrawlExecution getNextToFetch() throws InterruptedException {
            long sleep = 0L;

            while (true) {
                FutureOptional<MessagesProto.CrawlHostGroup> crawlHostGroup = frontier.getDb()
                        .borrowFirstReadyCrawlHostGroup();
                LOG.trace("Borrow Crawl Host Group: {}", crawlHostGroup);

                if (crawlHostGroup.isMaybeInFuture()) {
                    // A CrawlHostGroup suitable for execution in the future was found, wait until it is ready.
                    LOG.trace("Crawl Host Group not ready yet, delaying: {}", crawlHostGroup.getDelayMs());
                    sleep = crawlHostGroup.getDelayMs();
                } else if (crawlHostGroup.isPresent()) {
                    FutureOptional<MessagesProto.QueuedUri> foqu = frontier.getDb()
                            .getNextQueuedUriToFetch(crawlHostGroup.get());

                    if (foqu.isPresent()) {
                        // A fetchabel URI was found, return it
                        LOG.debug("Found Queued URI: >>{}<<", foqu.get());
                        return new CrawlExecution(foqu.get(), crawlHostGroup.get(), frontier);
                    } else if (foqu.isMaybeInFuture()) {
                        // A URI was found, but isn't fetchable yet. Wait for it
                        LOG.debug("Queued URI might be available at: {}", foqu.getWhen());
                        sleep = (foqu.getDelayMs());
                    } else {
                        // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                        LOG.trace("No Queued URI found waiting {}ms before retry", RESCHEDULE_DELAY);
                        sleep = RESCHEDULE_DELAY;
                    }
                    frontier.getDb().releaseCrawlHostGroup(crawlHostGroup.get(), sleep);
                } else {
                    // No CrawlHostGroup ready. Wait a moment and try again
                    sleep = RESCHEDULE_DELAY;
                }
                Thread.sleep(sleep);
            }
        }

    }
}
