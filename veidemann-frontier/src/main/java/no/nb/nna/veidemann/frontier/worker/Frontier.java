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

import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.FrontierProto.CrawlSeedRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
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
                        request.getSeed().getScope()));

        LOG.debug("New crawl execution: " + status.getId());

        String uri = request.getSeed().getMeta().getName();

        try {
            CrawlConfig crawlConfig = DbHelper.getCrawlConfigForJob(request.getJob());
            QueuedUriWrapper qUri = QueuedUriWrapper.getQueuedUriWrapper(uri, request.getJobExecutionId(),
                    status.getId(), crawlConfig.getPolitenessId());
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
