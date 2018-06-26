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

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class CrawlJobCollector implements TaskCollector {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlJobCollector.class);

    final DbAdapter db;

    final ListRequest listRequest;

    public CrawlJobCollector(DbAdapter db) {
        this.db = db;
        this.listRequest = ListRequest.newBuilder().build();
    }

    @Override
    public TaskTable getTasks() {
        TaskTable tasks = new TaskTable();
        Map<String, CrawlScheduleConfig> schedules = new HashMap<>();

        try {
            // Do not schedule jobs if Veidemann is paused.
            if (db.isPaused()) {
                return tasks;
            }

            for (CrawlJob job : db.listCrawlJobs(listRequest).getValueList()) {
                // Check if job is disabled
                if (!job.getDisabled()) {
                    CrawlScheduleConfig schedule = schedules.computeIfAbsent(job.getScheduleId(),
                            k -> {
                                if (k.isEmpty()) {
                                    return CrawlScheduleConfig.getDefaultInstance();
                                } else {
                                    try {
                                        return db.getCrawlScheduleConfig(GetRequest.newBuilder().setId(k).build());
                                    } catch (DbException e) {
                                        LOG.error("Failed getting tasks for scheduler", e);
                                        throw new RuntimeException(e);
                                    }
                                }
                            });

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
                        Task task = new ScheduledCrawlJob(db, job);
                        tasks.add(pattern, task);
                    }
                }
            }
        } catch (DbException e) {
            LOG.error("Failed getting tasks for scheduler", e);
            throw new RuntimeException(e);
        }

        return tasks;
    }

}
