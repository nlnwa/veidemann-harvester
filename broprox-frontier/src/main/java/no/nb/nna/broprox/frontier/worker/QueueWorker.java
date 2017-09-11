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

import java.time.OffsetDateTime;
import java.util.concurrent.RecursiveAction;

import io.opentracing.References;
import no.nb.nna.broprox.commons.FutureOptional;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;

/**
 *
 */
public class QueueWorker extends RecursiveAction {

    private static final long RESCHEDULE_DELAY = 2000;

    private final Frontier frontier;

    public QueueWorker(Frontier frontier) {
        this.frontier = frontier;
    }

    @Override
    protected void compute() {
        while (true) {
            try {
                System.out.println("Waiting for next execution to be ready");
                CrawlExecution exe = getNextToFetch();

                new OpenTracingWrapper("QueueWorker")
                        .setParentSpan(exe.getParentSpan())
                        .setParentReferenceType(References.FOLLOWS_FROM)
                        .run("runNextFetch", this::processExecution, exe);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void processExecution(CrawlExecution exe) {
        System.out.println("Running next fetch of exexcution: " + exe.getId());
        try {
            // Execute fetch
            getPool().managedBlock(exe);
            System.out.println("End of Link crawl");
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
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
     * <li>{@link FutureOptional#isMaybeInFuture()} returns true: There are one or more Uris in the queue, but it is not
     * ready for harvesting yet
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

            if (crawlHostGroup.isMaybeInFuture()) {
                // A CrawlHostGroup suitable for execution in the future was found, wait until it is ready.
                sleep = crawlHostGroup.getDelayMs();
            } else if (crawlHostGroup.isPresent()) {
                FutureOptional<QueuedUri> foqu = frontier.getDb().getNextQueuedUriToFetch(crawlHostGroup.get());

                if (foqu.isPresent()) {
                    // A fetchabel URI was found, return it
                    return new CrawlExecution(foqu.get(), crawlHostGroup.get(), frontier);
                } else if (foqu.isMaybeInFuture()) {
                    // A URI was found, but isn't fetchable yet. Wait for it
                    sleep = (foqu.getWhen().toEpochSecond() - OffsetDateTime.now().toEpochSecond()) * 1000;
                } else {
                    // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
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
