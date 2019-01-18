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

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

/**
 *
 */
public class Frontier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Frontier.class);

    private final RobotsServiceClient robotsServiceClient;

    private final DnsServiceClient dnsServiceClient;

    private final QueueWorker queueWorker;

    public Frontier(RobotsServiceClient robotsServiceClient, DnsServiceClient dnsServiceClient) {
        this.robotsServiceClient = robotsServiceClient;
        this.dnsServiceClient = dnsServiceClient;
        this.queueWorker = new QueueWorker(this);
    }

    public CrawlExecutionStatus scheduleSeed(CrawlSeedRequest request) throws DbException {
        // Create execution
        StatusWrapper status = StatusWrapper.getStatusWrapper(
                DbService.getInstance().getExecutionsAdapter().createCrawlExecutionStatus(
                        request.getJob().getId(),
                        request.getJobExecutionId(),
                        request.getSeed().getId(),
                        request.getSeed().getSeed().getScope()));

        LOG.debug("New crawl execution: " + status.getId());

        String uri = request.getSeed().getMeta().getName();

        try {
            ConfigObject crawlConfig = DbService.getInstance().getConfigAdapter()
                    .getConfigObject(request.getJob().getCrawlJob().getCrawlConfigRef());
            ConfigObject collectionConfig = DbService.getInstance().getConfigAdapter()
                    .getConfigObject(crawlConfig.getCrawlConfig().getCollectionRef());
            QueuedUriWrapper qUri = QueuedUriWrapper.getQueuedUriWrapper(uri, request.getJobExecutionId(),
                    status.getId(), crawlConfig.getCrawlConfig().getPolitenessRef(), collectionConfig.getMeta().getName());
            qUri.setPriorityWeight(crawlConfig.getCrawlConfig().getPriorityWeight());
            qUri.addUriToQueue();
            LOG.debug("Seed '{}' added to queue", qUri.getUri());
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError(ex.toString()))
                    .saveStatus();
        } catch (Exception ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(ex.toString()))
                    .saveStatus();
        }

        return status.getCrawlExecutionStatus();
    }

    public CrawlExecution getNextPageToFetch() throws InterruptedException {
        return queueWorker.getNextToFetch();
    }

    public RobotsServiceClient getRobotsServiceClient() {
        return robotsServiceClient;
    }

    public DnsServiceClient getDnsServiceClient() {
        return dnsServiceClient;
    }

    @Override
    public void close() {
    }

}
