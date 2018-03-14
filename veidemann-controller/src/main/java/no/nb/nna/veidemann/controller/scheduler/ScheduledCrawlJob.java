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
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.crawlSeed;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCrawlJob.class);

    final CrawlJob job;

    final DbAdapter db;

    public ScheduledCrawlJob(DbAdapter db, CrawlJob job) {
        this.db = db;
        this.job = job;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        LOG.info("Job '{}' starting", job.getMeta().getName());

        JobExecutionStatus jobExecutionStatus = db.saveJobExecutionStatus(JobExecutionStatus.newBuilder()
                .setJobId(job.getId())
                .setStartTime(ProtoUtils.getNowTs())
                .setState(State.RUNNING)
                .build());

        ListReplyWalker<SeedListRequest, Seed> walker = new ListReplyWalker<>();
        SeedListRequest.Builder seedRequest = SeedListRequest.newBuilder().setCrawlJobId(job.getId());

        walker.walk(seedRequest,
                req -> db.listSeeds(req),
                seed -> crawlSeed(job, seed, jobExecutionStatus));

        LOG.info("All seeds for job '{}' started", job.getMeta().getName());
    }
}
