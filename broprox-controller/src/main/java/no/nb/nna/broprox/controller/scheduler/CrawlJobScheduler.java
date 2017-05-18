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

import it.sauronsoftware.cron4j.Scheduler;
import no.nb.nna.broprox.db.DbAdapter;

/**
 *
 */
public class CrawlJobScheduler implements AutoCloseable {

    Scheduler scheduler;

    final DbAdapter db;

    final FrontierClient frontierClient;

    public CrawlJobScheduler(DbAdapter db, FrontierClient frontierClient) {
        this.db = db;
        this.frontierClient = frontierClient;
    }

    public CrawlJobScheduler start() {
        scheduler = new Scheduler();
        scheduler.addTaskCollector(new CrawlJobCollector(db, frontierClient));
        scheduler.start();
        return this;
    }

    @Override
    public void close() {
        scheduler.stop();
    }

}
