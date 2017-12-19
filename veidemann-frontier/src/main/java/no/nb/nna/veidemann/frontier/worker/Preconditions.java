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

import java.net.UnknownHostException;
import java.util.List;

import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Preconditions {

    private static final Logger LOG = LoggerFactory.getLogger(Preconditions.class);

    private static final String DNS_FAILURE = "dns_failure";

    private Preconditions() {
    }

    public static boolean checkPreconditions(Frontier frontier, CrawlConfig config, StatusWrapper status,
            QueuedUriWrapper qUri, long sequence) {

        qUri.setExecutionId(status.getId())
                .setPolitenessId(config.getPoliteness().getId())
                .setSequence(sequence);

        if (!checkRobots(frontier, config, qUri)) {
            status.incrementDocumentsDenied(1L);
            return false;
        }

        resolveDns(frontier, config, qUri);

        if (retryLimitReached(frontier, config, qUri)) {
            LOG.info("Failed fetching '{}' due to retry limit", qUri.getUri());
            status.incrementDocumentsFailed();
            return false;
        }

        if (qUri.getIp().isEmpty()) {
            // If ip is empty (caused by failed dns resolution) the value 'dns_failure' is used as crawl host group
            // To be able so schedule uri for new dns resolution attempt.

            frontier.getDb().getOrCreateCrawlHostGroup(DNS_FAILURE, config.getPoliteness().getId());
            qUri.setCrawlHostGroupId(DNS_FAILURE);
        } else {
            setCrawlHostGroup(frontier, qUri, config);
        }

        if (qUri.hasError()) {
            status.incrementDocumentsRetried();
        }

        qUri.addUriToQueue(frontier.getDb());
        return true;
    }

    private static boolean checkRobots(Frontier frontier, CrawlConfig config, QueuedUriWrapper qUri) {
        LOG.debug("Check robots.txt for URI '{}'", qUri.getUri());
        // Check robots.txt
        if (config.getPoliteness().getRobotsPolicy() != RobotsPolicy.IGNORE_ROBOTS
                && !frontier.getRobotsServiceClient().isAllowed(qUri.getQueuedUri(), config)) {
            LOG.info("URI '{}' precluded by robots.txt", qUri.getUri());
            qUri = qUri.setError(ExtraStatusCodes.PRECLUDED_BY_ROBOTS.toFetchError());
            frontier.writeLog(frontier, qUri);
            return false;
        }
        return true;
    }

    private static void resolveDns(Frontier frontier, CrawlConfig config, QueuedUriWrapper qUri) {
        if (!qUri.getIp().isEmpty()) {
            return;
        }

        LOG.debug("Resolve ip for URI '{}'", qUri.getUri());

        try {
            String ip = frontier.getDnsServiceClient()
                    .resolve(qUri.getHost(), qUri.getPort())
                    .getAddress()
                    .getHostAddress();
            qUri.setIp(ip);
        } catch (UnknownHostException ex) {
            LOG.info("Failed ip resolution for URI '{}' by extracting host '{}' and port '{}'",
                    qUri.getUri(),
                    qUri.getHost(),
                    qUri.getPort());

            qUri.setError(ExtraStatusCodes.FAILED_DNS.toFetchError(ex.toString()))
                .setEarliestFetchDelaySeconds(config.getPoliteness().getRetryDelaySeconds())
                .incrementRetries();
        }
    }

    private static boolean retryLimitReached(Frontier frontier, CrawlConfig config, QueuedUriWrapper qUri) {
        if (qUri.getRetries() < config.getPoliteness().getMaxRetries()) {
            return false;
        } else {
            frontier.writeLog(frontier, qUri, ExtraStatusCodes.RETRY_LIMIT_REACHED.getCode());
            return true;
        }
    }

    private static void setCrawlHostGroup(Frontier frontier, QueuedUriWrapper qUri, CrawlConfig config) {
        List<CrawlHostGroupConfig> groupConfigs = frontier.getDb()
                .listCrawlHostGroupConfigs(ControllerProto.ListRequest.newBuilder()
                        .setSelector(config.getPoliteness().getCrawlHostGroupSelector()).build()).getValueList();

        String crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroup(qUri.getIp(), groupConfigs);
        frontier.getDb().getOrCreateCrawlHostGroup(crawlHostGroupId, config.getPoliteness().getId());

        qUri.setCrawlHostGroupId(crawlHostGroupId);
    }

}
