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

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;

/**
 *
 */
public class CrawlJobCollector implements TaskCollector {

    final DbAdapter db;

    final CrawlJobListRequest listRequest;

    public CrawlJobCollector(DbAdapter db) {
        this.db = db;
        this.listRequest = CrawlJobListRequest.newBuilder().setExpand(true).build();
    }

    @Override
    public TaskTable getTasks() {
        TaskTable tasks = new TaskTable();

        for (CrawlJob job : db.listCrawlJobs(listRequest).getValueList()) {
            System.out.println("Job: " + job.getMeta().getName() + ", Cron: " + job.getSchedule().getCronExpression());
            SchedulingPattern pattern = new SchedulingPattern(job.getSchedule().getCronExpression());
            Task task = new ScheduledCrawlJob(db, job);
            tasks.add(pattern, task);
        }

        return tasks;
    }

}
