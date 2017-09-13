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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.rethinkdb.RethinkDB;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.commons.opentracing.OpenTracingParentContextKey;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlHostGroup;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlExecution implements ForkJoinPool.ManagedBlocker {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private CrawlExecutionStatus status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private final CrawlLimitsConfig limits;

    private final QueuedUri qUri;

    private final CrawlHostGroup crawlHostGroup;

    private volatile List<QueuedUri> outlinks;

    private long delayMs = 0L;

    private long fetchTimeMs = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(1L);

    private final Span parentSpan;

    private boolean done = false;

    public CrawlExecution(QueuedUri qUri, CrawlHostGroup crawlHostGroup, Frontier frontier) {
        this.status = frontier.getDb().getExecutionStatus(qUri.getExecutionId());
        ControllerProto.CrawlJobListRequest jobRequest = ControllerProto.CrawlJobListRequest.newBuilder()
                .setId(status.getJobId())
                .setExpand(true)
                .build();
        CrawlJob job = frontier.getDb().listCrawlJobs(jobRequest).getValueList().get(0);
        this.qUri = qUri;
        this.crawlHostGroup = crawlHostGroup;
        this.frontier = frontier;
        this.config = job.getCrawlConfig();
        this.parentSpan = OpenTracingParentContextKey.parentSpan();
        this.limits = job.getLimits();
    }

    public String getId() {
        return status.getId();
    }

    public CrawlConfig getConfig() {
        return config;
    }

    public QueuedUri getUri() {
        return qUri;
    }

    public CrawlHostGroup getCrawlHostGroup() {
        return crawlHostGroup;
    }

    public void endCrawl(CrawlExecutionStatus.State state) {
        LOG.info("Reached end of crawl '{}' with state: {}", getId(), state);
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
        LOG.info("Fetching " + qUri.getUri());
        frontier.getDb().deleteQueuedUri(qUri);

        // Check robots.txt
        if (!frontier.getRobotsServiceClient().isAllowed(qUri, config)) {
            LOG.info("Precluded by robots.txt");

            // Precluded by robots.txt
            CrawlLog crawlLog = CrawlLog.newBuilder()
                    .setRequestedUri(qUri.getUri())
                    .setSurt(qUri.getSurt())
                    .setRecordType("response")
                    .setStatusCode(-9998)
                    .setFetchTimeStamp(ProtoUtils.getNowTs())
                    .build();
            frontier.getDb().addCrawlLog(crawlLog);
            if (frontier.getDb().queuedUriCount(getId()) == 0) {
                endCrawl(CrawlExecutionStatus.State.FINISHED);
            }
        } else {
            try {
                long fetchStart = System.currentTimeMillis();
                HarvestPageReply harvestReply = frontier.getHarvesterClient().fetchPage(qUri, config);
                long fetchEnd = System.currentTimeMillis();
                fetchTimeMs = fetchEnd - fetchStart;

                outlinks = harvestReply.getOutlinksList();

                status = status.toBuilder()
                        .setDocumentsCrawled(status.getDocumentsCrawled() + 1)
                        .setBytesCrawled(status.getBytesCrawled() + harvestReply.getBytesDownloaded())
                        .setUrisCrawled(status.getUrisCrawled() + harvestReply.getUriCount())
                        .build();
                status = frontier.getDb().updateExecutionStatus(status);

                if (outlinks.isEmpty()) {
                    LOG.debug("No outlinks from {}", qUri.getSurt());
                    if (frontier.getDb().queuedUriCount(getId()) == 0) {
                        endCrawl(CrawlExecutionStatus.State.FINISHED);
                    }
                } else {
                    queueOutlinks();
                }
            } catch (Exception e) {
                e.printStackTrace();
                retryUri(qUri, e);
            }
        }

        calculateDelay();
    }

    void queueOutlinks() {
        long nextSequenceNum = 1L;
        if (!config.getDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

        for (QueuedUri outUri : outlinks) {
            outUri = frontier.enrichUri(outUri, getId(), nextSequenceNum, config);
            QueuedUri.Builder outUriBuilder = outUri.toBuilder();

            if (shouldInclude(outUri)) {
                LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());

                if (outUri.getSequence() != 1L) {
                    if (config.getDepthFirst()) {
                        nextSequenceNum = getNextSequenceNum();
                    }

                    outUriBuilder.setSequence(nextSequenceNum);
                }
                frontier.addUriToQueue(outUriBuilder);
            }
        }
    }

    boolean shouldInclude(QueuedUri outlink) {
        if (limits.getDepth() > 0 && limits.getDepth() <= calculateDepth(outlink)) {
            LOG.debug("Maximum configured depth reached for: {}, skipping.", outlink.getSurt());
            return false;
        }

        if (!outlink.getSurt().startsWith(status.getScope().getSurtPrefix())) {
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

    private int calculateDepth(QueuedUri uri) {
        return uri.getDiscoveryPath().length();
    }

    private void calculateDelay() {
        float delayFactor = getConfig().getPoliteness().getDelayFactor();
        long minTimeBetweenPageLoadMs = getConfig().getPoliteness().getMinTimeBetweenPageLoadMs();
        long maxTimeBetweenPageLoadMs = getConfig().getPoliteness().getMaxTimeBetweenPageLoadMs();
        if (delayFactor == 0f) {
            delayFactor = 1f;
        } else if (delayFactor < 0f) {
            delayFactor = 0f;
        }
        delayMs = (long) (fetchTimeMs * delayFactor);
        if (minTimeBetweenPageLoadMs > 0) {
            delayMs = Math.max(delayMs, minTimeBetweenPageLoadMs);
        }
        if (maxTimeBetweenPageLoadMs > 0) {
            delayMs = Math.min(delayMs, maxTimeBetweenPageLoadMs);
        }
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(delayMs, TimeUnit.MILLISECONDS);
    }

    public long getNextSequenceNum() {
        return nextSeqNum.getAndIncrement();
    }

    private void retryUri(QueuedUri qUri, Exception e) {
        if (qUri.getRetries() < config.getPoliteness().getMaxRetries()) {
            long retryDelaySeconds = getConfig().getPoliteness().getRetryDelaySeconds();
            Timestamp earliestFetch = Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(retryDelaySeconds));
            QueuedUri retryUri = qUri.toBuilder()
                    .setRetries(qUri.getRetries() + 1)
                    .setEarliestFetchTimeStamp(earliestFetch)
                    .setError(e.toString())
                    .build();
            frontier.getDb().addQueuedUri(retryUri);
            LOG.info("Failed fetching ({}) at attempt #{}", qUri, qUri.getRetries());
        }
        LOG.warn("Failed fetching page ({}), reason: e", qUri, e);
        CrawlExecutionStatus.State s = status.getState();
        if (qUri.getDiscoveryPath().isEmpty()) {
            // Seed failed; mark crawl as failed
            s = CrawlExecutionStatus.State.FAILED;
        }
        status = frontier.getDb().updateExecutionStatus(status.toBuilder()
                .setDocumentsFailed(status.getDocumentsFailed() + 1)
                .setState(s)
                .build());
    }

    private boolean shouldFetch() {
        if (limits.getMaxBytes() > 0 && status.getBytesCrawled() > limits.getMaxBytes()) {
            delayMs = 0L;
            frontier.getDb().deleteQueuedUri(qUri);
            switch (status.getState()) {
                case CREATED:
                case RUNNING:
                case UNDEFINED:
                case UNRECOGNIZED:
                    endCrawl(CrawlExecutionStatus.State.ABORTED_SIZE);
            }
            return false;
        }

        if (limits.getMaxDurationS() > 0
                && Timestamps.between(status.getStartTime(), ProtoUtils.getNowTs()).getSeconds()
                > limits.getMaxDurationS()) {
            delayMs = 0L;
            frontier.getDb().deleteQueuedUri(qUri);
            switch (status.getState()) {
                case CREATED:
                case RUNNING:
                case UNDEFINED:
                case UNRECOGNIZED:
                    endCrawl(CrawlExecutionStatus.State.ABORTED_TIMEOUT);
            }
            return false;
        }

        return true;
    }

    @Override
    public boolean block() throws InterruptedException {
        try {
            if (!status.hasStartTime()) {
                // Start execution
                status = status.toBuilder().setState(CrawlExecutionStatus.State.RUNNING)
                        .setStartTime(ProtoUtils.getNowTs())
                        .build();
                status = frontier.getDb().updateExecutionStatus(status);
            }

            if (shouldFetch()) {
                new OpenTracingWrapper("CrawlExecution")
                        .addTag(Tags.HTTP_URL.getKey(), qUri.getUri())
                        .run("Fetch", this::fetch);
            }
            done = true;
        } finally {
            frontier.getDb().releaseCrawlHostGroup(getCrawlHostGroup(), getDelay(TimeUnit.MILLISECONDS));
        }

        return done;
    }

    @Override
    public boolean isReleasable() {
        return done;
    }

    public Span getParentSpan() {
        return parentSpan;
    }

}
