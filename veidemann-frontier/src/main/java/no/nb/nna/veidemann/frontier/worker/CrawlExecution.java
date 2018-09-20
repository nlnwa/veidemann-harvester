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

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvest.Metrics;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvestSpec;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.Error;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.frontier.worker.Preconditions.PreconditionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

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

    private long delayMs = 0L;

    private long fetchStart;

    private long fetchTimeMs = 0L;

    private Span span;

    private DistributedLock lock;

    public CrawlExecution(QueuedUri queuedUri, CrawlHostGroup crawlHostGroup, Frontier frontier) throws DbException {
        this.status = StatusWrapper.getStatusWrapper(queuedUri.getExecutionId());
        try {
            this.qUri = QueuedUriWrapper.getQueuedUriWrapper(queuedUri).clearError();
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        this.status.addCurrentUri(this.qUri).saveStatus();

        ControllerProto.GetRequest jobRequest = ControllerProto.GetRequest.newBuilder()
                .setId(status.getJobId())
                .build();
        CrawlJob job = DbService.getInstance().getConfigAdapter().getCrawlJob(jobRequest);

        this.crawlHostGroup = crawlHostGroup;
        this.frontier = frontier;
        this.crawlConfig = DbHelper.getCrawlConfigForJob(job);
        this.politenessConfig = DbHelper.getPolitenessConfigForCrawlConfig(crawlConfig);
        this.limits = job.getLimits();
    }

    public String getId() {
        return status.getId();
    }

    public CrawlHostGroup getCrawlHostGroup() {
        return crawlHostGroup;
    }

    public QueuedUriWrapper getUri() {
        return qUri;
    }

    /**
     * Check if crawl is aborted.
     * </p>
     *
     * @return true if we should do the fetch
     */
    public PageHarvestSpec preFetch() {
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
            try {
                if (isManualAbort() || LimitsCheck.isLimitReached(frontier, limits, status, qUri)) {
                    delayMs = -1L;
                    return null;
                }

                if (qUri.isUnresolved()) {
                    PreconditionState check = Preconditions.checkPreconditions(frontier, crawlConfig, status, qUri);
                    switch (check) {
                        case DENIED:
                        case FAIL:
                            delayMs = -1L;
                            return null;
                        case RETRY:
                            qUri.addUriToQueue();
                            return null;
                        case OK:
                            // IP resolution done, requeue to account for politeness
                            qUri.addUriToQueue();
                            return null;
                    }
                }

                LOG.info("Fetching " + qUri.getUri());
                fetchStart = System.currentTimeMillis();

                return PageHarvestSpec.newBuilder()
                        .setQueuedUri(qUri.getQueuedUri())
                        .setCrawlConfig(crawlConfig)
                        .build();

            } catch (StatusRuntimeException e) {
                Code code = e.getStatus().getCode();
                if (code.equals(Status.CANCELLED.getCode())
                        || code.equals(Status.DEADLINE_EXCEEDED.getCode())
                        || code.equals(Status.ABORTED.getCode())) {
                    LOG.info("Request was aborted", e);
                    qUri.addUriToQueue();
                } else {
                    LOG.error("Unexpected error", e);
                }
                delayMs = -1L;
                return null;
            }
        } catch (DbException e) {
            LOG.error("Failed communicating with DB: {}", e.toString(), e);
            delayMs = -1L;
            return null;
        } catch (Throwable t) {
            // Errors should be handled elsewhere. Exception here indicates a bug.
            LOG.error("Possible bug: {}", t.toString(), t);
            delayMs = -1L;
            return null;
        }
    }

    /**
     * Do post processing after a successful fetch.
     */
    public void postFetchSuccess(Metrics metrics) {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        status.incrementDocumentsCrawled()
                .incrementBytesCrawled(metrics.getBytesDownloaded())
                .incrementUrisCrawled(metrics.getUriCount());
    }

    /**
     * Do post proccessing after a failed fetch.
     *
     * @param error the Error causing the failure
     */
    public void postFetchFailure(Error error) throws DbException {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        LOG.info("Failed fetch of {}: {}", qUri.getUri(), error);
        retryUri(qUri, error);
    }

    /**
     * Do post proccessing after a failed fetch.
     *
     * @param t the exception thrown
     */
    public void postFetchFailure(Throwable t) throws DbException {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        LOG.info("Failed fetch of {}", qUri.getUri(), t);
        retryUri(qUri, ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
    }

    /**
     * Do some housekeeping.
     * </p>
     * This should be run regardless of if we fetched anything or if the fetch failed in any way.
     */
    public void postFetchFinally() {
        MDC.put("eid", qUri.getExecutionId());
        MDC.put("uri", qUri.getUri());

        try {
            status.removeCurrentUri(qUri).saveStatus();
            long fetchEnd = System.currentTimeMillis();
            fetchTimeMs = fetchEnd - fetchStart;
            calculateDelay();

            if (qUri.hasError() && qUri.getDiscoveryPath().isEmpty()) {
                if (qUri.getError().getCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
                    // Seed precluded by robots.txt; mark crawl as finished
                    endCrawl(State.FINISHED, qUri.getError());
                } else {
                    // Seed failed; mark crawl as failed
                    endCrawl(State.FAILED, qUri.getError());
                }
            } else if (DbService.getInstance().getCrawlQueueAdapter().queuedUriCount(getId()) == 0) {
                endCrawl(State.FINISHED);
            }

            // Save updated status
            status.saveStatus();

            // Recheck if user aborted crawl while fetching last uri.
            if (isManualAbort()) {
                delayMs = 0L;
            }
        } catch (DbException e) {
            // An error here indicates problems with DB communication. No idea how to handle that yet.
            LOG.error("Error updating status after fetch: {}", e.toString(), e);
        } catch (Throwable e) {
            // Catch everything to ensure crawl host group gets released.
            // Discovering this message in logs should be investigated as a possible bug.
            LOG.error("Unknown error in post fetch. Might be a bug", e);
        }

        try {
            DbService.getInstance().getCrawlQueueAdapter().releaseCrawlHostGroup(getCrawlHostGroup(), getDelay(TimeUnit.MILLISECONDS));
        } catch (DbException e) {
            LOG.error("Error releasing CrawlHostGroup: {}", e.toString(), e);
        } catch (Throwable t) {
            // An error here indicates unknown problems with DB communication. No idea how to handle that yet.
            LOG.error("Error releasing CrawlHostGroup: {}", t.toString(), t);
        }

        span.finish();
    }

    public void queueOutlink(QueuedUri outlink) throws DbException {
        try {
            QueuedUriWrapper outUri = QueuedUriWrapper.getQueuedUriWrapper(qUri, outlink);

            DistributedLock lock = DbService.getInstance()
                    .createDistributedLock(new Key("quri", outUri.getQueuedUri().getSurt()), 10);
            lock.lock();
            try {
                if (shouldInclude(outUri)) {
                    outUri.setSequence(outUri.getDiscoveryPath().length());

                    PreconditionState check = Preconditions.checkPreconditions(frontier, crawlConfig, status, outUri);
                    switch (check) {
                        case OK:
                            LOG.debug("Found new URI: {}, queueing.", outUri.getSurt());
                            outUri.addUriToQueue();
                            break;
                        case RETRY:
                            LOG.debug("Failed preconditions for: {}, queueing for retry.", outUri.getSurt());
                            outUri.addUriToQueue();
                            break;
                        case FAIL:
                        case DENIED:
                            break;
                    }
                }
            } finally {
                lock.unlock();
            }
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed();
            LOG.info("Illegal URI {}", ex);
        }
    }

    boolean shouldInclude(QueuedUriWrapper outlink) throws DbException {
        if (!LimitsCheck.isQueueable(limits, status, outlink)) {
            return false;
        }

        if (!ScopeCheck.isInScope(status, outlink)) {
            return false;
        }

        if (DbService.getInstance().getCrawlQueueAdapter().uriNotIncludedInQueue(outlink.getQueuedUri(), status.getStartTime())) {
            return true;
        }

        LOG.debug("Found already included URI: {}, skipping.", outlink.getSurt());
        return false;
    }

    private void calculateDelay() {
        if (delayMs == -1) {
            delayMs = 0L;
            return;
        }

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

    private void retryUri(QueuedUriWrapper qUri, Error error) throws DbException {
        LOG.info("Failed fetching ({}) at attempt #{}", qUri, qUri.getRetries());
        qUri.incrementRetries()
                .setEarliestFetchDelaySeconds(politenessConfig.getRetryDelaySeconds())
                .setError(error);

        if (LimitsCheck.isRetryLimitReached(politenessConfig, qUri)) {
            LOG.info("Failed fetching '{}' due to retry limit", qUri);
            status.incrementDocumentsFailed();
        } else {
            qUri.addUriToQueue();
        }
    }

    private void endCrawl(CrawlExecutionStatus.State state) throws DbException {
        status.setEndState(state)
                .removeCurrentUri(qUri).saveStatus();
    }

    private void endCrawl(CrawlExecutionStatus.State state, Error error) throws DbException {
        status.setEndState(state)
                .setError(error)
                .removeCurrentUri(qUri).saveStatus();
    }

    private boolean isManualAbort() throws DbException {
        if (status.getState() == CrawlExecutionStatus.State.ABORTED_MANUAL) {
            status.removeCurrentUri(qUri)
                    .incrementDocumentsDenied(DbService.getInstance().getCrawlQueueAdapter()
                            .deleteQueuedUrisForExecution(status.getId()));

            // Re-set end state to ensure end time is updated
            status.setEndState(status.getState()).saveStatus();
            return true;
        }
        return false;
    }

}
