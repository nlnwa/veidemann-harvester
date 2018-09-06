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

import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
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
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setDepth(10).setMaxDurationS(300).setMaxBytes(0).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);
        String jobId = job.getId();

        ConfigProto.CrawlEntity entity = createEntity("Test entity 1");
        ConfigProto.Seed seed = createSeed("http://a1.com", entity, jobId);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        // The goal is to get as low as 14 when we cache 404, 302, etc
        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 5)
                .checkCrawlLogCount("revisit", 15)
                .checkCrawlLogCount("dns", 0)
                .checkPageLogCount(6);

        jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 5)
                .checkCrawlLogCount("revisit", 35)
                .checkCrawlLogCount("dns", 0)
                .checkPageLogCount(12);
    }

}
