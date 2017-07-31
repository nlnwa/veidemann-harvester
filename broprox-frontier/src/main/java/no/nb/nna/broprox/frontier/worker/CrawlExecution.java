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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.rethinkdb.RethinkDB;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlScope;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.netpreserve.commons.uri.UriConfigs;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlExecution implements ForkJoinPool.ManagedBlocker, Delayed {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private CrawlExecutionStatus status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private final CrawlScope scope;

    private final CrawlLimitsConfig limits;

    private QueuedUri currentUri;

    private volatile List<QueuedUri> outlinks;

    private long nextExecTimestamp = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(1L);

    private boolean seedResolved = false;

    private final Span parentSpan;

    public CrawlExecution(Span span, Frontier frontier, Seed seed, CrawlJob job, CrawlScope scope) {
        this.frontier = frontier;
        this.config = job.getCrawlConfig();
        this.parentSpan = span;
        this.scope = scope;
        this.limits = job.getLimits();

        // Create execution
        CrawlExecutionStatus s = CrawlExecutionStatus.newBuilder()
                .setJobId(job.getId())
                .setSeedId(seed.getId())
                .setState(CrawlExecutionStatus.State.CREATED)
                .build();
        this.status = frontier.getDb().addExecutionStatus(s);
        System.out.println("NEW CRAWL EXECUTION: " + status.getId());
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

    public void setStatus(CrawlExecutionStatus status) {
        this.status = status;
    }

    public QueuedUri getCurrentUri() {
        return currentUri;
    }

    public void setCurrentUri(QueuedUri currentUri) {
        this.currentUri = currentUri;
        this.outlinks = Collections.EMPTY_LIST;
    }

    public void endCrawl(CrawlExecutionStatus.State state) {
        System.out.println("Reached end of crawl");
        CrawlExecutionStatus.State currentState = status.getState();
        if (currentState != CrawlExecutionStatus.State.RUNNING) {
            state = currentState;
        }
        status = status.toBuilder()
                .setState(state)
                .setEndTime(ProtoUtils.getNowTs())
                .build();
        status = frontier.getDb().updateExecutionStatus(status);
        frontier.getHarvesterClient().cleanupExecution(getId());
    }

    public void fetch() {
        if (!status.hasStartTime()) {
            // Start execution
            status = status.toBuilder().setState(CrawlExecutionStatus.State.RUNNING)
                    .setStartTime(ProtoUtils.getNowTs())
                    .build();
            status = frontier.getDb().updateExecutionStatus(status);
        }

        System.out.println("Fetching " + currentUri.getUri());
        currentUri = currentUri.toBuilder()
                .setSurt(UriConfigs.SURT_KEY.buildUri(currentUri.getUri()).toString())
                .setScope(scope)
                .build();

        try {
            try {
                HarvestPageReply harvestReply = frontier.getHarvesterClient().fetchPage(currentUri, config);
                outlinks = harvestReply.getOutlinksList();
                seedResolved = true;

                status = status.toBuilder()
                        .setDocumentsCrawled(status.getDocumentsCrawled() + 1)
                        .setBytesCrawled(status.getBytesCrawled() + harvestReply.getBytesDownloaded())
                        .setUrisCrawled(status.getUrisCrawled() + harvestReply.getUriCount())
                        .build();
                status = frontier.getDb().updateExecutionStatus(status);

            } catch (Exception e) {
                System.out.println("Error fetching page (" + currentUri + "): " + e);
                // should do some logging and updating here
                status = frontier.getDb().updateExecutionStatus(status.toBuilder()
                        .setState(CrawlExecutionStatus.State.FAILED)
                        .build());
                seedResolved = true;
                System.out.println("Nothing more to do since Harvester is dead.");
                return;
            }

            if (outlinks.isEmpty()) {
                LOG.debug("No outlinks from {}", currentUri.getSurt());
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
        long nextSequenceNum = 1L;
        if (!config.getDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

        for (QueuedUri outUri : outlinks) {
            QueuedUri.Builder outUriBuilder = outUri.toBuilder();
            try {
                outUriBuilder.setSurt(UriConfigs.SURT_KEY.buildUri(outUriBuilder.getUri()).toString());
            } catch (Exception e) {
                System.out.println("Strange error with outlink (" + outUriBuilder + "): " + e);
                e.printStackTrace();
                return;
            }

            if (shouldInclude(outUriBuilder)) {
                LOG.debug("Found new URI: {}, queueing.", outUriBuilder.getSurt());

                if (outUriBuilder.getSequence() != 1L) {
                    if (config.getDepthFirst()) {
                        nextSequenceNum = getNextSequenceNum();
                    }

                    outUriBuilder.setSequence(nextSequenceNum);
                }
                try {
                    frontier.getDb().addQueuedUri(outUriBuilder.build());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    boolean shouldInclude(MessagesProto.QueuedUriOrBuilder outlink) {
        if (limits.getDepth() > 0 && limits.getDepth() <= calculateDepth(outlink)) {
            LOG.debug("Maximum configured depth reached for: {}, skipping.", outlink.getSurt());
            return false;
        }

        if (!outlink.getSurt().startsWith(scope.getSurtPrefix())) {
            LOG.debug("URI '{}' is out of scope, skipping.", outlink.getSurt());
            return false;
        }

        RethinkDB r = RethinkDB.r;
        boolean notSeen = frontier.getDb().executeRequest(
                r.table(RethinkDbAdapter.TABLES.CRAWL_LOG.name)
                        .between(
                                r.array(outlink.getSurt(), ProtoUtils.tsToOdt(status.getStartTime())),
                                r.array(outlink.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(RethinkDbAdapter.TABLES.URI_QUEUE.name).getAll(outlink.getSurt())
                                        .optArg("index", "surt")
                                        .limit(1)
                        ).isEmpty());

        if (notSeen) {
            return true;
        }
        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
    }

    public int calculateDepth(MessagesProto.QueuedUriOrBuilder uri) {
        return uri.getDiscoveryPath().length();
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
        if (outlinks.isEmpty()) {
            new OpenTracingWrapper("CrawlExecution")
                    .addTag(Tags.HTTP_URL.getKey(), currentUri.getUri())
                    .run("Fetch", this::fetch);
        }
        return true;
    }

    @Override
    public boolean isReleasable() {
        return !outlinks.isEmpty();
    }

    public boolean isDone() {
        return outlinks.isEmpty();
    }

    public boolean isSeedResolved() {
        return seedResolved;
    }

    public long getNextSequenceNum() {
        return nextSeqNum.getAndIncrement();
    }

    public void calculateDelay() {
        nextExecTimestamp = System.currentTimeMillis() + getConfig().getPoliteness().getMinTimeBetweenPageLoadMs();
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

    public Span getParentSpan() {
        return parentSpan;
    }

}
