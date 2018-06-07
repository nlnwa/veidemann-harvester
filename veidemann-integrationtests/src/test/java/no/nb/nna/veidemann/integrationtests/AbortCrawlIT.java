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
import no.nb.nna.veidemann.api.StatusProto.ExecutionId;
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
public class AbortCrawlIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testAbortDepth() throws InterruptedException, ExecutionException, DbException {
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setDepth(2).setMaxDurationS(300).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();

        new CrawlExecutionValidator(db)
                .validate();

        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 4), new SimpleEntry<>("FAILED", 1));

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 7)
                .checkCrawlLogCount("revisit", 14)
                .checkPageLogCount(7);
    }

    @Test
    public void testAbortSize() throws InterruptedException, ExecutionException, DbException {
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setMaxBytes(1024 * 4).setMaxDurationS(300).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();

        new CrawlExecutionValidator(db)
                .validate();

        assertThat(jes.getExecutionsStateMap().get("ABORTED_SIZE")).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testAbortManual() throws InterruptedException, ExecutionException, DbException {
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlLimitsConfig limits = job.getLimits().toBuilder().setMaxDurationS(300).build();
        job = job.toBuilder().setLimits(limits).build();
        job = controllerClient.saveCrawlJob(job);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobCompletion jc = JobCompletion.executeJob(db, statusClient, controllerClient, request);
        Thread.sleep(10000);
        System.out.println("ABORTING");
        statusClient.abortJobExecution(ExecutionId.newBuilder().setId(jc.jobExecutionId).build());
        JobExecutionStatus jes = jc.get();
        Thread.sleep(2000);

        new CrawlExecutionValidator(db)
                .validate();

        assertThat(jes.getExecutionsStateMap().get("ABORTED_MANUAL")).isGreaterThanOrEqualTo(3);
        assertThat(jes.getExecutionsStateMap().get("ABORTED_MANUAL")
                + jes.getExecutionsStateMap().get("FINISHED")
                + jes.getExecutionsStateMap().get("FAILED"))
                .isEqualTo(5);
    }

    private void setupConfigAndSeeds(CrawlJob job) throws DbException {
        DbHelper dbh = DbHelper.getInstance();
        dbh.configure(db);

        String jobId = job.getId();

        CrawlConfig crawlConfig = dbh.getCrawlConfigForJob(job);

        PolitenessConfig politeness = controllerClient.savePolitenessConfig(
                dbh.getPolitenessConfigForCrawlConfig(crawlConfig).toBuilder()
                        .setMaxTimeBetweenPageLoadMs(100)
                        .setMinTimeBetweenPageLoadMs(1)
                        .setDelayFactor(.01f)
                        .setRetryDelaySeconds(1)
                        .build());

        ConfigProto.CrawlEntity entity1 = createEntity("Test entity 1");
        ConfigProto.Seed seed1 = createSeed("http://a1.com", entity1, jobId);

        ConfigProto.CrawlEntity entity2 = createEntity("Test entity 2");
        ConfigProto.Seed seed2 = createSeed("http://a2.com", entity2, jobId);
        ConfigProto.Seed seed3 = createSeed("http://a3.com", entity2, jobId);

        ConfigProto.CrawlEntity entity3 = createEntity("Test entity 3");
        ConfigProto.Seed invalidSeed = createSeed("https://www.toll.no/ // etat under finansdepartementet", entity3, jobId);

        ConfigProto.CrawlEntity entity4 = createEntity("Test entity 4");
        ConfigProto.Seed notFoundSeed = createSeed("http://static.com/not-found.gif", entity4, jobId);
    }
}
