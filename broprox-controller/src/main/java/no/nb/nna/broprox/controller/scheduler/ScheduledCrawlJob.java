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
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.Seed;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    final CrawlJob job;

    final DbAdapter db;

    public ScheduledCrawlJob(DbAdapter db, CrawlJob job) {
        this.db = db;
        this.job = job;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        System.out.println("Job '" + job.getMeta().getName() + "' starting");
        SeedListRequest request = SeedListRequest.newBuilder()
                .setCrawlJobId(job.getId())
                .build();

        for (Seed seed : db.listSeeds(request).getValueList()) {
            System.out.println("Start harvest of: " + seed.getMeta().getName() + ", URI: " + seed.getUri());
        }
        System.out.println("All seeds for job '" + job.getMeta().getName() + "' started");
    }

}
