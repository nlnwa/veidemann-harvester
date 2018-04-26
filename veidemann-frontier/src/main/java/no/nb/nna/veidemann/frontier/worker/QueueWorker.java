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
package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueueWorker {

    private static final Logger LOG = LoggerFactory.getLogger(QueueWorker.class);

    private static final long RESCHEDULE_DELAY = 2000;

    private final Frontier frontier;

    public QueueWorker(Frontier frontier) {
        this.frontier = frontier;
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
    public CrawlExecution getNextToFetch() throws InterruptedException {
        long sleep = 0L;

        while (true) {
            try {
                FutureOptional<MessagesProto.CrawlHostGroup> crawlHostGroup = DbUtil.getInstance().getDb()
                        .borrowFirstReadyCrawlHostGroup();
                LOG.trace("Borrow Crawl Host Group: {}", crawlHostGroup);

                if (crawlHostGroup.isMaybeInFuture()) {
                    // A CrawlHostGroup suitable for execution in the future was found, wait until it is ready.
                    LOG.trace("Crawl Host Group not ready yet, delaying: {}", crawlHostGroup.getDelayMs());
                    sleep = crawlHostGroup.getDelayMs();
                } else if (crawlHostGroup.isPresent()) {
                    FutureOptional<MessagesProto.QueuedUri> foqu = DbUtil.getInstance().getDb()
                            .getNextQueuedUriToFetch(crawlHostGroup.get());

                    if (foqu.isPresent()) {
                        // A fetchabel URI was found, return it
                        LOG.debug("Found Queued URI: {}, crawlHostGroup: {}, sequence: {}",
                                foqu.get().getUri(), foqu.get().getCrawlHostGroupId(), foqu.get().getSequence());
                        return new CrawlExecution(foqu.get(), crawlHostGroup.get(), frontier);
                    } else if (foqu.isMaybeInFuture()) {
                        // A URI was found, but isn't fetchable yet. Wait for it
                        LOG.debug("Queued URI might be available at: {}", foqu.getWhen());
                        sleep = foqu.getDelayMs();
                    } else {
                        // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                        LOG.trace("No Queued URI found waiting {}ms before retry", RESCHEDULE_DELAY);
                        sleep = RESCHEDULE_DELAY;
                    }
                    DbUtil.getInstance().getDb().releaseCrawlHostGroup(crawlHostGroup.get(), sleep, false);
                } else {
                    // No CrawlHostGroup ready. Wait a moment and try again
                    sleep = RESCHEDULE_DELAY;
                }
            } catch (DbException e) {
                LOG.error("Caught an DB exception. Might be a temporary condition try again after {}ms",
                        RESCHEDULE_DELAY, e);
                sleep = RESCHEDULE_DELAY;
            }

            Thread.sleep(sleep);
        }
    }
}
