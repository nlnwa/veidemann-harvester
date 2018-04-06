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

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CrawlExecution {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private final StatusWrapper status;

    private final Frontier frontier;

    private final CrawlConfig crawlConfig;

    private final PolitenessConfig politenessConfig;

    private final CrawlLimitsConfig limits;

    private final QueuedUriWrapper qUri;

    private final CrawlHostGroup crawlHostGroup;

    private volatile List<QueuedUri> outlinks;

    private long delayMs = 0L;

    private long fetchTimeMs = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(1L);

    private Span span;

    public CrawlExecution(QueuedUri qUri, CrawlHostGroup crawlHostGroup, Frontier frontier) {
        this.status = StatusWrapper.getStatusWrapper(qUri.getExecutionId());
        this.status.setCurrentUri(qUri.getUri());

        ControllerProto.GetRequest jobRequest = ControllerProto.GetRequest.newBuilder()
                .setId(status.getJobId())
                .build();
        CrawlJob job = DbUtil.getInstance().getDb().getCrawlJob(jobRequest);

        try {
            this.qUri = QueuedUriWrapper.getQueuedUriWrapper(qUri).clearError();
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        this.crawlHostGroup = crawlHostGroup;
        this.frontier = frontier;
        this.crawlConfig = DbUtil.getInstance().getCrawlConfigForJob(job);
        this.politenessConfig = DbUtil.getInstance().getPolitenessConfigForCrawlConfig(crawlConfig);
        this.limits = job.getLimits();
    }

    public String getId() {
        return status.getId();
    }

//    public CrawlConfig getConfig() {
//        return crawlConfig;
//    }

    public CrawlHostGroup getCrawlHostGroup() {
        return crawlHostGroup;
    }

    public QueuedUriWrapper getqUri() {
        return qUri;
    }

    /**
     * Execute crawling of the queued Uri.
     */
    public void execute() {
        if (preFetch()) {
            fetch();
        } else {
            postFetchFinally();
        }
    }

    /**
     * Check if crawl is aborted.
     * </p>
     * @return true if we should do the fetch
     */
    private boolean preFetch() {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        span = GlobalTracer.get()
                .buildSpan("runNextFetch")
                .withTag(Tags.COMPONENT.getKey(), "CrawlExecution")
                .withTag("uri", qUri.getUri())
                .withTag("executionId", getId())
                .ignoreActiveSpan()
                .startManual();

        try {
            status.setState(CrawlExecutionStatus.State.FETCHING)
                    .setStartTimeIfUnset()
                    .saveStatus();
            DbUtil.getInstance().getDb().deleteQueuedUri(qUri.getQueuedUri());

            if (isManualAbort() || LimitsCheck.isLimitReached(frontier, limits, status, qUri)) {
                delayMs = 0L;
                return false;
            } else {
                return true;
            }
        } catch (Throwable t) {
            // Errors should be handled elsewhere. Exception here indicates a bug.
            LOG.error(t.toString(), t);
            return false;
        }
    }

    private void fetch() {
        LOG.info("Fetching " + qUri.getUri());

            final long fetchStart = System.currentTimeMillis();

            frontier.getHarvesterClient().fetchPage(qUri.getQueuedUri(), crawlConfig)
                    .thenAcceptAsync(r -> {
                        postFetchSuccess(r, fetchStart);
                    })
                    .exceptionally(t -> {
                        postFetchFailure(t);
                        return null;
                    })
                    .whenCompleteAsync((r, t) -> {
                        calculateDelay();
                        postFetchFinally();
                    });
    }

    /**
     * Do post processing after a successful fetch.
     */
    private void postFetchSuccess(HarvestPageReply harvestReply, long fetchStart) {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        long fetchEnd = System.currentTimeMillis();
        fetchTimeMs = fetchEnd - fetchStart;

        outlinks = harvestReply.getOutlinksList();

        status.incrementDocumentsCrawled()
                .incrementBytesCrawled(harvestReply.getBytesDownloaded())
                .incrementUrisCrawled(harvestReply.getUriCount());

        if (outlinks.isEmpty()) {
            LOG.debug("No outlinks from {}", qUri.getSurt());
        } else {
            queueOutlinks();
        }
    }

    /**
     * Do post proccessing after a failed fetch.
     * @param t the exception thrown
     */
    private void postFetchFailure(Throwable t) {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        LOG.info("Failed fetch of {}", qUri.getUri(), t);
        retryUri(qUri, t);
    }

    /**
     * Do some housekeeping.
     * </p>
     * This should be run regardless of if we fetched anything or if the fetch failed in any way.
     */
    private void postFetchFinally() {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        try {
            if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                // Seed failed; mark crawl as failed
                endCrawl(CrawlExecutionStatus.State.FAILED);
            } else if (DbUtil.getInstance().getDb().queuedUriCount(getId()) == 0) {
                endCrawl(CrawlExecutionStatus.State.FINISHED);
            } else if (status.getState() == CrawlExecutionStatus.State.FETCHING) {
                status.setState(CrawlExecutionStatus.State.SLEEPING);
            }

            // Recheck if user aborted crawl while fetching last uri.
            if (isManualAbort()) {
                delayMs = 0L;
            }
        } catch (Throwable t) {
            // An error here indicates problems with DB communication. No idea how to handle that yet.
            LOG.error("Error updating status after fetch: {}", t.toString(), t);
        } finally {
            // Save updated status
            status.saveStatus();
        }

        try {
            DbUtil.getInstance().getDb().releaseCrawlHostGroup(getCrawlHostGroup(), getDelay(TimeUnit.MILLISECONDS));
        } catch (Throwable t) {
            // An error here indicates problems with DB communication. No idea how to handle that yet.
            LOG.error("Error releasing CrawlHostGroup: {}", t.toString(), t);
        }

        span.finish();
    }

    void queueOutlinks() {
        long nextSequenceNum = 1L;
        if (!crawlConfig.getDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

        for (QueuedUri outlink : outlinks) {
            try {
                QueuedUriWrapper outUri = QueuedUriWrapper.getQueuedUriWrapper(outlink);

                if (shouldInclude(outUri)) {
                    Preconditions.checkPreconditions(frontier, crawlConfig, status, outUri, nextSequenceNum);
                    LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());
                }
            } catch (URISyntaxException ex) {
                status.incrementDocumentsFailed();
                LOG.info("Illegal URI {}", ex);
            }
        }
    }

    boolean shouldInclude(QueuedUriWrapper outlink) {
        if (!LimitsCheck.isQueueable(limits, status, outlink)) {
            return false;
        }

        if (!ScopeCheck.isInScope(status, outlink)) {
            return false;
        }

        if (DbUtil.getInstance().getDb().uriNotIncludedInQueue(outlink.getQueuedUri(), status.getStartTime())) {
            return true;
        }

        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
    }

    private void calculateDelay() {
        float delayFactor = politenessConfig.getDelayFactor();
        long minTimeBetweenPageLoadMs = politenessConfig.getMinTimeBetweenPageLoadMs();
        long maxTimeBetweenPageLoadMs = politenessConfig.getMaxTimeBetweenPageLoadMs();
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

    private void retryUri(QueuedUriWrapper qUri, Throwable t) {

        LOG.info("Failed fetching ({}) at attempt #{}", qUri, qUri.getRetries());
        qUri.incrementRetries()
                .setEarliestFetchDelaySeconds(politenessConfig.getRetryDelaySeconds())
                .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));

        Preconditions.checkPreconditions(frontier, crawlConfig, status, qUri, qUri.getQueuedUri().getSequence());
    }

    private void endCrawl(CrawlExecutionStatus.State state) {
        if (!status.isEnded()) {
            status.setEndState(state)
                    .clearCurrentUri();
        }
        try {
            frontier.getHarvesterClient().cleanupExecution(getId());
        } catch (Throwable t) {
            LOG.error("Error cleaning up after execution. Harvester is probably dead: {}", t.toString(), t);
        }
    }

    private boolean isManualAbort() {
        if (status.getState() == CrawlExecutionStatus.State.ABORTED_MANUAL) {
            status.clearCurrentUri()
                    .incrementDocumentsDenied(DbUtil.getInstance().getDb().deleteQueuedUrisForExecution(status.getId()));
            frontier.getHarvesterClient().cleanupExecution(status.getId());

            // Re-set end state to ensure end time is updated
            status.setEndState(status.getState());
            return true;
        }
        return false;
    }

}
