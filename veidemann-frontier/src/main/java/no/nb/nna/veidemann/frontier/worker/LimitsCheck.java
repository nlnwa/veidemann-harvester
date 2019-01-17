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

import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LimitsCheck {

    private static final Logger LOG = LoggerFactory.getLogger(LimitsCheck.class);

    private LimitsCheck() {
    }

    /**
     * Checks that a URI which is about to be queued is within the limits required.
     *
     * @param limits the limits configuration
     * @param status the status object which might be updated by this method
     * @param qUri   the URI to check
     * @return true if the submitted URI is within limits for queueing
     */
    public static boolean isQueueable(CrawlLimitsConfig limits, StatusWrapper status,
                                      QueuedUriWrapper qUri) {

        if (limits.getDepth() > 0 && limits.getDepth() <= calculateDepth(qUri)) {
            LOG.debug("Maximum configured depth reached for: {}, skipping.", qUri.getSurt());
            status.incrementDocumentsDenied(1L);
            return false;
        }
        return true;
    }

    /**
     * Checks that should be run after fetching a URI to see if the limits for crawling are reached.
     *
     * @param frontier the frontier
     * @param limits   the limits configuration
     * @param status   the status object which might be updated by this method
     * @param qUri     the URI to check
     * @return true if crawl should be stopped
     */
    public static boolean isLimitReached(Frontier frontier, CrawlLimitsConfig limits, StatusWrapper status,
                                         QueuedUriWrapper qUri) throws DbException {

        if (limits.getMaxBytes() > 0 && status.getBytesCrawled() > limits.getMaxBytes()) {
            switch (status.getState()) {
                case CREATED:
                case FETCHING:
                case SLEEPING:
                case UNDEFINED:
                case UNRECOGNIZED:
                    status.setEndState(CrawlExecutionStatus.State.ABORTED_SIZE)
                            .incrementDocumentsDenied(
                                    DbService.getInstance().getCrawlQueueAdapter()
                                            .deleteQueuedUrisForExecution(status.getId())
                            ).saveStatus();
            }
            return true;
        }

        if (limits.getMaxDurationS() > 0
                && Timestamps.between(status.getCreatedTime(), ProtoUtils.getNowTs()).getSeconds() > limits
                .getMaxDurationS()) {

            switch (status.getState()) {
                case CREATED:
                case FETCHING:
                case SLEEPING:
                case UNDEFINED:
                case UNRECOGNIZED:
                    status.setEndState(CrawlExecutionStatus.State.ABORTED_TIMEOUT);
                    status.incrementDocumentsDenied(
                            DbService.getInstance().getCrawlQueueAdapter()
                                    .deleteQueuedUrisForExecution(status.getId())
                    ).saveStatus();
            }
            return true;
        }

        return false;
    }

    public static boolean isRetryLimitReached(ConfigObject politeness, QueuedUriWrapper qUri) throws DbException {
        if (qUri.getRetries() < politeness.getPolitenessConfig().getMaxRetries()) {
            qUri.clearError();
            return false;
        } else {
            qUri.setError(ExtraStatusCodes.RETRY_LIMIT_REACHED.toFetchError());
            DbUtil.writeLog(qUri, ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode());
            return true;
        }
    }

    private static int calculateDepth(QueuedUriWrapper qUri) {
        // Remove redirects from discovery path before calculating depth
        return qUri.getDiscoveryPath().replace("R", "").length();
    }

}
