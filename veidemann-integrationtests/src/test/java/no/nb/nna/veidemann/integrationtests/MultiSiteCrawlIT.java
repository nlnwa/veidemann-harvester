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
        DbHelper dbh = DbHelper.getInstance();
        dbh.configure(db);
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlConfig crawlConfig = dbh.getCrawlConfigForJob(job);

        PolitenessConfig politeness = dbh.getPolitenessConfigForCrawlConfig(crawlConfig).toBuilder()
                .setMaxTimeBetweenPageLoadMs(100)
                .setMinTimeBetweenPageLoadMs(1)
                .setDelayFactor(.01f)
                .setRetryDelaySeconds(1)
                .build();
        politeness = controllerClient.savePolitenessConfig(politeness);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setDepth(10).setMaxDurationS(300).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);
        String jobId = job.getId();

        ConfigProto.CrawlEntity entity1 = controllerClient.saveEntity(
                ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Test entity 1")).build());
        ConfigProto.Seed seed1 = controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://a1.com"))
                .setEntityId(entity1.getId())
                .addJobId(jobId)
                .build());

        ConfigProto.CrawlEntity entity2 = controllerClient.saveEntity(
                ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Test entity 2")).build());
        ConfigProto.Seed seed2 = controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://a2.com"))
                .setEntityId(entity2.getId())
                .addJobId(jobId)
                .build());
        ConfigProto.Seed seed3 = controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://a3.com"))
                .setEntityId(entity2.getId())
                .addJobId(jobId)
                .build());

        ConfigProto.CrawlEntity entity3 = controllerClient.saveEntity(
                ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Test entity 3")).build());
        ConfigProto.Seed invalidSeed = controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("https://www.toll.no/ // etat under finansdepartementet"))
                .setEntityId(entity3.getId())
                .addJobId(jobId)
                .build());

        ConfigProto.CrawlEntity entity4 = controllerClient.saveEntity(
                ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Test entity 4")).build());
        ConfigProto.Seed notFoundSeed = controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://static.com/not-found.gif"))
                .setEntityId(entity4.getId())
                .addJobId(jobId)
                .build());

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 4), new SimpleEntry<>("FAILED", 1));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 7)
                .checkCrawlLogCount("revisit", 50)
                .checkCrawlLogCount("dns", 6)
                .checkPageLogCount(19);

        jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 4), new SimpleEntry<>("FAILED", 1));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 7)
                .checkCrawlLogCount("revisit", 107)
                .checkCrawlLogCount("dns", 6)
                .checkPageLogCount(38);
    }

}
