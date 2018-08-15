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
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.crawlSeed;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCrawlJob.class);

    final CrawlJob job;

    public ScheduledCrawlJob(CrawlJob job) {
        this.job = job;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        LOG.debug("Job '{}' starting", job.getMeta().getName());

        ListReplyWalker<SeedListRequest, Seed> walker = new ListReplyWalker<>();
        SeedListRequest.Builder seedRequest = SeedListRequest.newBuilder().setCrawlJobId(job.getId());

        try {
            if (DbService.getInstance().getConfigAdapter().listSeeds(seedRequest.build()).getCount() > 0) {
                JobExecutionStatus jobExecutionStatus = DbService.getInstance().getDbAdapter()
                        .createJobExecutionStatus(job.getId());

                walker.walk(seedRequest,
                        req -> DbService.getInstance().getConfigAdapter().listSeeds(req),
                        seed -> crawlSeed(job, seed, jobExecutionStatus));

                LOG.info("All seeds for job '{}' started", job.getMeta().getName());
            }
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
    }
}
