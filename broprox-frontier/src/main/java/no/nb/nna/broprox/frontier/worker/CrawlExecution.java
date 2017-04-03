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

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.rethinkdb.RethinkDB;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.db.model.CrawlConfig;
import no.nb.nna.broprox.db.model.CrawlExecutionStatus;
import no.nb.nna.broprox.db.model.QueuedUri;
import org.netpreserve.commons.uri.UriConfigs;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlExecution implements ForkJoinPool.ManagedBlocker, Delayed {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private final CrawlExecutionStatus status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private QueuedUri currentUri;

    private volatile QueuedUri[] outlinks;

    private long nextExecTimestamp = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(0L);

    private boolean seedResolved = false;

    public CrawlExecution(Frontier frontier, CrawlExecutionStatus status, CrawlConfig config) {
        System.out.println("NEW CRAWL EXECUTION: " + status.getId());
        this.frontier = frontier;
        this.status = status;
        this.config = config;
    }

    public String getId() {
        return status.getId();
    }

    public CrawlConfig getConfig() {
        return config;
    }

    public CrawlExecutionStatus getStatus() {
        return status;
    }

    public QueuedUri getCurrentUri() {
        return currentUri;
    }

    public void setCurrentUri(QueuedUri currentUri) {
        this.currentUri = currentUri;
        this.outlinks = null;
    }

    public void fetch() {
        System.out.println("Fetching " + currentUri.getUri());
        currentUri
                .withCrawlConfig(config)
                .withSurt(UriConfigs.SURT_KEY.buildUri(currentUri.getUri()).toString());

        try {
            try {
                outlinks = frontier.getHarvesterClient().fetchPage(status.getId(), currentUri);
                seedResolved = true;

                status.withState(CrawlExecutionStatus.State.RUNNING)
                        .withDocumentsCrawled(status.getDocumentsCrawled() + 1);
                frontier.getDb().updateExecutionStatus(status);

            } catch (Exception e) {
                System.out.println("Error fetching page (" + currentUri + "): " + e);
                // should do some logging and updating here
            }

            if (outlinks == null || outlinks.length == 0) {
                LOG.debug("No outlinks from {}", currentUri.getSurt());
                outlinks = new QueuedUri[0];
                return;
            }

            queueOutlinks();
        } catch (Exception e) {
            System.out.println("Strange error fetching page (" + currentUri + "): " + e);
            e.printStackTrace();
            // TODO: should do some logging and updating here
        }
        return;
    }

    void queueOutlinks() {
        QueuedUri.IdSeq currentId = new QueuedUri.IdSeq(getId(), 0L);

        long nextSequenceNum = 0;
        if (!config.isDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

        for (QueuedUri outUri : outlinks) {
            try {
                outUri.withSurt(UriConfigs.SURT_KEY.buildUri(outUri.getUri()).toString());
            } catch (Exception e) {
                System.out.println("Strange error with outlink (" + outUri + "): " + e);
                e.printStackTrace();
                return;
            }

            if (shouldInclude(outUri)) {
                LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());
                List<QueuedUri.IdSeq> eIds = outUri.listExecutionIds();

                if (!eIds.contains(currentId)) {
                    if (config.isDepthFirst()) {
                        nextSequenceNum = getNextSequenceNum();
                    }

                    outUri.addExecutionId(new QueuedUri.IdSeq(getId(), nextSequenceNum));
                }
                try {
                    frontier.getDb().addQueuedUri(outUri);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                LOG.debug("Found already included URI: {}, skipping.", outUri.getSurt());
            }
        }
    }

    boolean shouldInclude(QueuedUri outlink) {
        RethinkDB r = RethinkDB.r;
        boolean notSeen = frontier.getDb().executeRequest(
                r.table(RethinkDbAdapter.TABLE_CRAWL_LOG)
                .between(
                        r.array(outlink.getSurt(), status.getStartTime()),
                        r.array(outlink.getSurt(), r.maxval()))
                .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                .union(
                        r.table(RethinkDbAdapter.TABLE_URI_QUEUE).getAll(outlink.getSurt()).optArg("index", "surt")
                        .limit(1)
                ).isEmpty());

        if (notSeen && outlink.getSurt().startsWith(config.getScope())) {
            return true;
        }
        return false;
    }

    public long objectSize() {
        long size = 0L;
        size += GraphLayout.parseInstance(getId()).totalSize();
        size += GraphLayout.parseInstance(config).totalSize();
        size += GraphLayout.parseInstance(currentUri).totalSize();
        return size;
    }

    @Override
    public boolean block() throws InterruptedException {
        if (outlinks == null) {
            fetch();
        }
        return true;
    }

    @Override
    public boolean isReleasable() {
        return outlinks != null;
    }

    public boolean isDone() {
        return outlinks.length == 0;
    }

    public boolean isSeedResolved() {
        return seedResolved;
    }

    public long getNextSequenceNum() {
        return nextSeqNum.getAndIncrement();
    }

    public void calculateDelay() {
        nextExecTimestamp = System.currentTimeMillis() + getConfig().getMinTimeBetweenPageLoadMillis();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long remainingDelayMillis = nextExecTimestamp - System.currentTimeMillis();
        return unit.convert(remainingDelayMillis, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o instanceof CrawlExecution) {
            return (int) (((CrawlExecution) o).nextExecTimestamp - nextExecTimestamp);
        }
        throw new IllegalArgumentException("Can only compare CrawlExecution");
    }

}
