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

import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.opentracing.ActiveSpan;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.commons.ExtraStatusCodes;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlHostGroup;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlExecution implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private final StatusWrapper status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private final CrawlLimitsConfig limits;

    private final QueuedUriWrapper qUri;

    private final CrawlHostGroup crawlHostGroup;

    private volatile List<QueuedUri> outlinks;

    private long delayMs = 0L;

    private long fetchTimeMs = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(1L);

    public CrawlExecution(QueuedUri qUri, CrawlHostGroup crawlHostGroup, Frontier frontier) {
        this.status = StatusWrapper.getStatusWrapper(frontier.getDb(), qUri.getExecutionId());
        this.status.setCurrentUri(qUri.getUri());

        ControllerProto.CrawlJobListRequest jobRequest = ControllerProto.CrawlJobListRequest.newBuilder()
                .setId(status.getJobId())
                .setExpand(true)
                .build();
        CrawlJob job = frontier.getDb().listCrawlJobs(jobRequest).getValueList().get(0);

        try {
            this.qUri = QueuedUriWrapper.getQueuedUriWrapper(frontier, qUri).clearError();
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        this.crawlHostGroup = crawlHostGroup;
        this.frontier = frontier;
        this.config = job.getCrawlConfig();
        this.limits = job.getLimits();
    }

    public String getId() {
        return status.getId();
    }

    public CrawlConfig getConfig() {
        return config;
    }

    public CrawlHostGroup getCrawlHostGroup() {
        return crawlHostGroup;
    }

    public QueuedUriWrapper getqUri() {
        return qUri;
    }

    public void fetch() {
        LOG.info("Fetching " + qUri.getUri());

        try {
            long fetchStart = System.currentTimeMillis();
            HarvestPageReply harvestReply = frontier.getHarvesterClient().fetchPage(qUri.getQueuedUri(), config);
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
        } catch (Exception e) {
            LOG.info("Failed fetch of {}", qUri.getUri(), e);
            retryUri(qUri, e);
        }

        calculateDelay();
    }

    void queueOutlinks() {
        long nextSequenceNum = 1L;
        if (!config.getDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

        for (QueuedUri outlink : outlinks) {
            try {
                QueuedUriWrapper outUri = QueuedUriWrapper.getQueuedUriWrapper(frontier, outlink);

                if (shouldInclude(outUri)) {
                    Preconditions.checkPreconditions(frontier, config, status, outUri, nextSequenceNum);
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

        if (frontier.getDb().uriNotIncludedInQueue(outlink.getQueuedUri(), status.getStartTime())) {
            return true;
        }

        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
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

    private void retryUri(QueuedUriWrapper qUri, Exception e) {

        LOG.info("Failed fetching ({}) at attempt #{}", qUri, qUri.getRetries());
        qUri.incrementRetries()
                .setEarliestFetchDelaySeconds(getConfig().getPoliteness().getRetryDelaySeconds())
                .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(e.toString()));

        Preconditions.checkPreconditions(frontier, config, status, qUri, qUri.getQueuedUri().getSequence());
    }

    @Override
    public void run() {
        try (ActiveSpan span = GlobalTracer.get()
                .buildSpan("runNextFetch")
                .withTag(Tags.COMPONENT.getKey(), "CrawlExecution")
                .withTag("uri", qUri.getUri())
                .withTag("executionId", getId())
                .ignoreActiveSpan()
                .startActive()) {

            try {
                status.setState(CrawlExecutionStatus.State.FETCHING)
                        .setStartTimeIfUnset()
                        .saveStatus(frontier.getDb());
                frontier.getDb().deleteQueuedUri(qUri.getQueuedUri());

                if (isManualAbort() || LimitsCheck.isLimitReached(frontier, limits, status, qUri)) {
                    delayMs = 0L;
                } else {
                    fetch();
                }
            } catch (Throwable t) {
                // Errors should be handled elsewhere. Exception here indicates a bug.
                LOG.error(t.toString(), t);
            }

            try {
                if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                    // Seed failed; mark crawl as failed
                    endCrawl(CrawlExecutionStatus.State.FAILED);
                } else if (frontier.getDb().queuedUriCount(getId()) == 0) {
                    endCrawl(CrawlExecutionStatus.State.FINISHED);
                } else if (status.getState() == CrawlExecutionStatus.State.FETCHING) {
                    status.setState(CrawlExecutionStatus.State.SLEEPING);
                }
                status.saveStatus(frontier.getDb());

                // Recheck if user aborted crawl while fetching last uri.
                if (isManualAbort()) {
                    // Save updated status
                    status.saveStatus(frontier.getDb());
                    delayMs = 0L;
                }
            } catch (Throwable t) {
                // An error here indicates problems with DB communication. No idea how to handle that yet.
                LOG.error("Error updating status after fetch: {}", t.toString(), t);
            }

            try {
                frontier.getDb().releaseCrawlHostGroup(getCrawlHostGroup(), getDelay(TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                // An error here indicates problems with DB communication. No idea how to handle that yet.
                LOG.error("Error releasing CrawlHostGroup: {}", t.toString(), t);
            }
        }
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
                    .incrementDocumentsDenied(frontier.getDb().deleteQueuedUrisForExecution(status.getId()));
            frontier.getHarvesterClient().cleanupExecution(status.getId());
            return true;
        }
        return false;
    }

}
