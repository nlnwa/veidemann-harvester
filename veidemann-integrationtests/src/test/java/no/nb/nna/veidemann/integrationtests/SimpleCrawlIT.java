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
public class SimpleCrawlIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testHarvest() throws InterruptedException, ExecutionException, DbException {
        String jobId = createJob("SimpleCrawlIT", 10, 300, 0).getId();

        ConfigObject entity = createEntity("Test entity 1");
        ConfigObject seed = createSeed("http://a1.com", entity, jobId);

        RunCrawlRequest request = RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        // The goal is to get as low as 14 when we cache 404, 302, etc
        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 5)
                .checkCrawlLogCount("revisit", 15)
                .checkCrawlLogCount("dns", 3)
                .checkPageLogCount(6);

        jes = JobCompletion.executeJob(db, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 5)
                .checkCrawlLogCount("revisit", 35)
                .checkCrawlLogCount("dns", 3)
                .checkPageLogCount(12);
    }

}
