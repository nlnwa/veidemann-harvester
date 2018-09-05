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

import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
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
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlConfig crawlConfig = DbHelper.getCrawlConfigForJob(job);

        PolitenessConfig politeness = DbHelper.getPolitenessConfigForCrawlConfig(crawlConfig).toBuilder()
                .setMaxTimeBetweenPageLoadMs(100)
                .setMinTimeBetweenPageLoadMs(1)
                .setDelayFactor(.01f)
                .setRetryDelaySeconds(1)
                .build();
        politeness = controllerClient.savePolitenessConfig(politeness);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setDepth(10).setMaxDurationS(300).setMaxBytes(0).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);
        String jobId = job.getId();

        ConfigProto.CrawlEntity entity1 = createEntity("Test entity 1");
        ConfigProto.Seed seed1 = createSeed("http://a1.com", entity1, jobId);

        ConfigProto.CrawlEntity entity2 = createEntity("Test entity 2");
        ConfigProto.Seed seed2 = createSeed("http://a2.com", entity2, jobId);
        ConfigProto.Seed seed3 = createSeed("http://a3.com", entity2, jobId);

        ConfigProto.CrawlEntity entity3 = createEntity("Test entity 3");
        ConfigProto.Seed invalidSeed = createSeed("https://www.toll.no/ // etat under finansdepartementet", entity3, jobId);

        ConfigProto.CrawlEntity entity4 = createEntity("Test entity 4");
        ConfigProto.Seed notFoundSeed = createSeed("http://static.com/not-found.gif", entity4, jobId);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 5));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 9)
                .checkCrawlLogCount("revisit", 50)
                .checkCrawlLogCount("dns", 6)
                .checkPageLogCount(20);

        jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 5));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 11)
                .checkCrawlLogCount("revisit", 107)
                .checkCrawlLogCount("dns", 6)
                .checkPageLogCount(40);
    }

}
