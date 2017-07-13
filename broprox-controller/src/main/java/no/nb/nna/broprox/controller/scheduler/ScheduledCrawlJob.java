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
package no.nb.nna.broprox.controller.scheduler;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.Seed;
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
        SeedListRequest request = SeedListRequest.newBuilder()
                .setCrawlJobId(job.getId())
                .build();

        for (Seed seed : db.listSeeds(request).getValueList()) {
            if (!seed.getDisabled()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Start harvest of: {}", seed.getMeta().getName());
                    frontierClient.crawlSeed(job, seed);
                }
            }
        }
        LOG.info("All seeds for job '{}' started", job.getMeta().getName());
    }

}
