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
package no.nb.nna.veidemann.controller.scheduler;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus.State;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCrawlJob.class);

    final CrawlJob job;

    final DbAdapter db;

    final FrontierClient frontierClient;

    public ScheduledCrawlJob(DbAdapter db, FrontierClient frontierClient, CrawlJob job) {
        this.db = db;
        this.job = job;
        this.frontierClient = frontierClient;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        LOG.info("Job '{}' starting", job.getMeta().getName());

        JobExecutionStatus jobExecutionStatus = db.saveJobExecutionStatus(JobExecutionStatus.newBuilder()
                .setJobId(job.getId())
                .setStartTime(ProtoUtils.getNowTs())
                .setState(State.RUNNING)
                .build());

        SeedListRequest seedRequest;
        int page = 0;

        seedRequest = SeedListRequest.newBuilder()
                .setCrawlJobId(job.getId())
                .setPageSize(100)
                .setPage(page)
                .build();

        SeedListReply seedList = db.listSeeds(seedRequest);
        while (seedList.getValueCount() > 0) {
            for (Seed seed : seedList.getValueList()) {
                runSeed(job, seed, jobExecutionStatus);
            }
            seedRequest = seedRequest.toBuilder().setPage(++page).build();
            seedList = db.listSeeds(seedRequest);
        }

        LOG.info("All seeds for job '{}' started", job.getMeta().getName());
    }


    private void runSeed(CrawlJob job, Seed seed, JobExecutionStatus jobExecutionStatus) {
        if (!seed.getDisabled()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Start harvest of: {}", seed.getMeta().getName());
                frontierClient.crawlSeed(job, seed, jobExecutionStatus).getId();
            }
        }
    }
}
