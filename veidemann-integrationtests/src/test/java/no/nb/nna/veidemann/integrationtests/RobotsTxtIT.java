/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class RobotsTxtIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    @Ignore
    public void testRobotsObey() throws InterruptedException, ExecutionException, DbException {
        String jobId = createJob("ObeyRobotsTxtIT", 10, 300, 0).getId();
        ConfigObject.Builder politenessBuilder = politeness.toBuilder();
        politenessBuilder.getPolitenessConfigBuilder()
                .setRobotsPolicy(RobotsPolicy.CUSTOM_ROBOTS)
                .setCustomRobots("User-agent *\n" +
                        "Disallow: /\n");
        politeness = configClient.saveConfigObject(politenessBuilder.build());

        ConfigObject entity = createEntity("Test entity 1");
        ConfigObject seed = createSeed("http://a1.com", entity, jobId);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 1)
                .checkCrawlLogCount("revisit", 0)
                .checkCrawlLogCount("dns", 1)
                .checkPageLogCount(0);

        jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 2)
                .checkCrawlLogCount("revisit", 0)
                .checkCrawlLogCount("dns", 1)
                .checkPageLogCount(0);
    }

    @Test
    public void testRobotsClassic() throws InterruptedException, ExecutionException, DbException {
        String jobId = createJob("ObeyClassicRobotsTxtIT", 10, 300, 0).getId();
        ConfigObject.Builder politenessBuilder = politeness.toBuilder();
        politenessBuilder.getPolitenessConfigBuilder()
                .setRobotsPolicy(RobotsPolicy.CUSTOM_ROBOTS_CLASSIC)
                .setCustomRobots("User-agent *\n" +
                        "Disallow: /apache-icon.gif\nDisallow: /bigpic.png\n");
        politeness = configClient.saveConfigObject(politenessBuilder.build());

        ConfigObject entity = createEntity("Test entity 1");
        ConfigObject seed = createSeed("http://a1.com", entity, jobId);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();

        JobExecutionStatus jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 20)
                .checkCrawlLogCount("revisit", 10)
                .checkCrawlLogCount("dns", 2)
                .checkPageLogCount(6);

        jes = JobCompletion.executeJob(db, statusClient, controllerClient, request).get();
        assertThat(jes.getExecutionsStateMap()).contains(new SimpleEntry<>("FINISHED", 1), new SimpleEntry<>("FAILED", 0));

        new CrawlExecutionValidator(jes)
                .validate()
                .checkCrawlLogCount("response", 38)
                .checkCrawlLogCount("revisit", 22)
                .checkCrawlLogCount("dns", 2)
                .checkPageLogCount(12);
    }

}
