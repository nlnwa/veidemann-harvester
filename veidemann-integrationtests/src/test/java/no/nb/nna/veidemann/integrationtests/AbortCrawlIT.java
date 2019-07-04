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

import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.StatusProto.ExecutionId;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
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
public class AbortCrawlIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testAbortDepth() throws InterruptedException, ExecutionException, DbException {
        ConfigObject job = createJob("AbortDepth", 2, 300, 0);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();

        new CrawlExecutionValidator(jes)
                .validate();

        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 5));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 8)
                .checkCrawlLogCount("revisit", 14)
                .checkCrawlLogCount(22, "response", "revisit")
                .checkPageLogCount(8);
    }

    @Test
    public void testAbortSize() throws InterruptedException, ExecutionException, DbException {
        ConfigObject job = createJob("AbortSize", 10, 300, 1024 * 4);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();

        new CrawlExecutionValidator(jes)
                .validate();

        assertThat(jes.getExecutionsStateMap().get("ABORTED_SIZE")).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testAbortManual() throws InterruptedException, ExecutionException, DbException {
        ConfigObject job = createJob("AbortManual", 10, 300, 0);

        setupConfigAndSeeds(job);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(job.getId())
                .build();

        JobCompletion jc = JobCompletion.executeJob(db, statusClient, controllerClient, request);
        Thread.sleep(2000);
        System.out.println("ABORTING");
        statusClient.abortJobExecution(ExecutionId.newBuilder().setId(jc.jobExecutionId).build());
        JobExecutionStatus jes = jc.get();
        Thread.sleep(8000);

        new CrawlExecutionValidator(jes)
                .validate();

        assertThat(jes.getExecutionsStateMap().get("ABORTED_MANUAL")).isGreaterThanOrEqualTo(3);
        assertThat(jes.getExecutionsStateMap().get("ABORTED_MANUAL")
                + jes.getExecutionsStateMap().get("FINISHED")
                + jes.getExecutionsStateMap().get("FAILED"))
                .isGreaterThanOrEqualTo(4);
    }

    private void setupConfigAndSeeds(ConfigObject job) throws DbException {
        String jobId = job.getId();

        ConfigObject entity1 = createEntity("Test entity 1");
        ConfigObject seed1 = createSeed("http://a1.com", entity1, jobId);

        ConfigObject entity2 = createEntity("Test entity 2");
        ConfigObject seed2 = createSeed("http://a2.com", entity2, jobId);
        ConfigObject seed3 = createSeed("http://a3.com", entity2, jobId);

        ConfigObject entity3 = createEntity("Test entity 3");
        ConfigObject invalidSeed = createSeed("https://www.toll.no/ // etat under finansdepartementet", entity3, jobId);

        ConfigObject entity4 = createEntity("Test entity 4");
        ConfigObject notFoundSeed = createSeed("http://static.com/not-found.gif", entity4, jobId);
    }
}
