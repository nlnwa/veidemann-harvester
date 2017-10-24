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

import java.time.OffsetDateTime;

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlJobCollector implements TaskCollector {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlJobCollector.class);

    final DbAdapter db;

    final CrawlJobListRequest listRequest;

    final FrontierClient frontierClient;

    public CrawlJobCollector(DbAdapter db, FrontierClient frontierClient) {
        this.db = db;
        this.listRequest = CrawlJobListRequest.newBuilder().setExpand(true).build();
        this.frontierClient = frontierClient;
    }

    @Override
    public TaskTable getTasks() {
        TaskTable tasks = new TaskTable();

        for (CrawlJob job : db.listCrawlJobs(listRequest).getValueList()) {
            // Check if job is disabled
            if (!job.getDisabled()) {
                CrawlScheduleConfig schedule = job.getSchedule();

                // Check if job is valid for current date
                OffsetDateTime now = ProtoUtils.getNowOdt();
                if (!schedule.getCronExpression().isEmpty()
                        && (!schedule.hasValidFrom() || ProtoUtils.tsToOdt(schedule.getValidFrom()).isBefore(now))
                        && (!schedule.hasValidTo() || ProtoUtils.tsToOdt(schedule.getValidTo()).isAfter(now))) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Read Job: '{}' with cron expression '{}'",
                                job.getMeta().getName(), schedule.getCronExpression());
                    }

                    SchedulingPattern pattern = new SchedulingPattern(schedule.getCronExpression());
                    Task task = new ScheduledCrawlJob(db, frontierClient, job);
                    tasks.add(pattern, task);
                }
            }
        }

        return tasks;
    }

}
