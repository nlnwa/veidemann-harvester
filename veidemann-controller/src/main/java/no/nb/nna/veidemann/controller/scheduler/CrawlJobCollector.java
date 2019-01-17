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
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
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

    final ListRequest listRequest;

    public CrawlJobCollector() {
        this.listRequest = ListRequest.newBuilder().setKind(Kind.crawlJob).build();
    }

    @Override
    public TaskTable getTasks() {
        TaskTable tasks = new TaskTable();
        Map<ConfigRef, ConfigObject> schedules = new HashMap<>();

        try {
            // Do not schedule jobs if Veidemann is paused.
            if (DbService.getInstance().getDbAdapter().isPaused()) {
                return tasks;
            }

            try (ChangeFeed<ConfigObject> cursor = DbService.getInstance().getConfigAdapter().listConfigObjects(listRequest)) {
                cursor.stream().forEach(job -> {
                    // Check if job is disabled
                    if (!job.getCrawlJob().getDisabled() && job.getCrawlJob().hasScheduleRef()) {
                        ConfigObject schedule = schedules.computeIfAbsent(job.getCrawlJob().getScheduleRef(),
                                k -> {
                                    try {
                                        return DbService.getInstance().getConfigAdapter().getConfigObject(k);
                                    } catch (DbException e) {
                                        LOG.error("Failed getting tasks for scheduler", e);
                                        throw new RuntimeException(e);
                                    }
                                });

                        // Check if job is valid for current date
                        OffsetDateTime now = ProtoUtils.getNowOdt();
                        CrawlScheduleConfig csc = schedule.getCrawlScheduleConfig();
                        if (!csc.getCronExpression().isEmpty()
                                && (!csc.hasValidFrom() || ProtoUtils.tsToOdt(csc.getValidFrom()).isBefore(now))
                                && (!csc.hasValidTo() || ProtoUtils.tsToOdt(csc.getValidTo()).isAfter(now))) {

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Read Job: '{}' with cron expression '{}'",
                                        job.getMeta().getName(), csc.getCronExpression());
                            }

                            SchedulingPattern pattern = new SchedulingPattern(csc.getCronExpression());
                            Task task = new ScheduledCrawlJob(job);
                            tasks.add(pattern, task);
                        }
                    }
                });
            }
        } catch (DbException e) {
            LOG.error("Failed getting tasks for scheduler", e);
            throw new RuntimeException(e);
        }

        return tasks;
    }

}
