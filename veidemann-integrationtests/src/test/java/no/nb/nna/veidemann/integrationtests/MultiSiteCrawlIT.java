/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbException;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class MultiSiteCrawlIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testHarvest() throws InterruptedException, ExecutionException, DbException {
        String jobId = createJob("MultiSiteCrawlIT", 10, 300, 0).getId();

        ConfigObject entity1 = createEntity("Test entity 1");
        ConfigObject seed1 = createSeed("https://a1.com", entity1, jobId);

        ConfigObject entity2 = createEntity("Test entity 2");
        createSeed("https://a2.com", entity2, jobId);
        createSeed("https://a3.com", entity2, jobId);

        ConfigObject entity3 = createEntity("Test entity 3");
        ConfigObject invalidSeed = createSeed("https://www.toll.no/ // etat under finansdepartementet", entity3, jobId);

        ConfigObject entity4 = createEntity("Test entity 4");
        ConfigObject notFoundSeed = createSeed("https://static.com/not-found.gif", entity4, jobId);

        RunCrawlRequest request = RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 5));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 8)
                .checkCrawlLogCount("revisit", 50)
                .checkCrawlLogCount("dns", 6)
                .checkCrawlLogCount(58, "response", "revisit")
                .checkPageLogCount(20);

        jes = JobCompletion.executeJob(db, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 5));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 8)
                .checkCrawlLogCount("revisit", 108)
                .checkCrawlLogCount(116, "response", "revisit")
                .checkCrawlLogCount("dns", 6)
                .checkPageLogCount(40);
    }

}
