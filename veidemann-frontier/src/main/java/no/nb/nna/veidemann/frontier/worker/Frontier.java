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

import no.nb.nna.veidemann.api.FrontierProto.CrawlSeedRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus.State;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
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

    public Frontier(RethinkDbAdapter db, RobotsServiceClient robotsServiceClient, DnsServiceClient dnsServiceClient) {
        DbUtil.getInstance().configure(db);

        this.robotsServiceClient = robotsServiceClient;
        this.dnsServiceClient = dnsServiceClient;
        this.queueWorker = new QueueWorker(this);
    }

    public CrawlExecutionStatus scheduleSeed(CrawlSeedRequest request) {
        // Create execution
        StatusWrapper status = StatusWrapper.getStatusWrapper(CrawlExecutionStatus.newBuilder()
                .setJobId(request.getJob().getId())
                .setJobExecutionId(request.getJobExecutionId())
                .setSeedId(request.getSeed().getId())
                .setState(CrawlExecutionStatus.State.CREATED)
                .setScope(request.getSeed().getScope()));
        status.saveStatus();
        LOG.debug("New crawl execution: " + status.getId());

        String uri = request.getSeed().getMeta().getName();

        try {
            QueuedUriWrapper qUri = QueuedUriWrapper.getQueuedUriWrapper(uri, request.getJobExecutionId(), status.getId());
            boolean isAllowed = Preconditions.checkPreconditions(this,
                    DbUtil.getInstance().getCrawlConfigForJob(request.getJob()), status, qUri, 1L);
            if (isAllowed) {
                LOG.debug("Seed '{}' added to queue", qUri.getUri());
            } else {
                LOG.info("Seed '{}' not queueable. Cause: {}", qUri.getUri(), qUri.getError());
                status.setEndState(State.FINISHED)
                        .setError(qUri.getError());
            }
        } catch (URISyntaxException ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError(ex.toString()));
        } catch (Exception ex) {
            status.incrementDocumentsFailed()
                    .setEndState(CrawlExecutionStatus.State.FAILED)
                    .setError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(ex.toString()));
        }
        status.saveStatus();

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
