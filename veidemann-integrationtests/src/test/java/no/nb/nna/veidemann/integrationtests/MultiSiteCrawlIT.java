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
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbHelper;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class MultiSiteCrawlIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testHarvest() throws InterruptedException, ExecutionException {
        DbHelper dbh = DbHelper.getInstance();
        dbh.configure(db);
        CrawlJob job = controllerClient.listCrawlJobs(ControllerProto.ListRequest.newBuilder()
                .setName("unscheduled").build())
                .getValue(0);

        CrawlConfig crawlConfig = dbh.getCrawlConfigForJob(job);

        PolitenessConfig politeness = dbh.getPolitenessConfigForCrawlConfig(crawlConfig).toBuilder()
                .setMaxTimeBetweenPageLoadMs(100)
                .setDelayFactor(.1f)
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

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .build();

        System.out.println("Job execution result:\n" + JobCompletion.executeJob(statusClient, request).get());

        // TODO: check these values instead of just printing
        System.out.println("WARC RECORDS");
        WarcInspector.getWarcFiles().getRecordStream().forEach(r -> System.out.println(r.header.warcTypeStr + " -- "
                + r.header.warcTargetUriStr + ", ip: " + r.header.warcIpAddress));

        PageLogListReply pageLog = db.listPageLogs(PageLogListRequest.getDefaultInstance());

        System.out.println("\nPAGE LOG");
        pageLog.getValueList().forEach(p -> {
            System.out.println(p.getUri() + ", eid: " + p.getExecutionId());
            p.getResourceList().forEach(r -> System.out.println("  - " + r.getUri() + ", cache: " + r.getFromCache()));
        });

        assertThat(pageLog.getCount()).isEqualTo(18L);

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 8)
                .checkCrawlLogCount("revisit", 51)
                .checkCrawlLogCount("dns", 6);

        System.out.println("Job execution result:\n" + JobCompletion.executeJob(statusClient, request).get());
        pageLog = db.listPageLogs(PageLogListRequest.getDefaultInstance());

        System.out.println("\nPAGE LOG");
        pageLog.getValueList().forEach(p -> {
            System.out.println(p.getUri() + ", eid: " + p.getExecutionId());
            p.getResourceList().forEach(r -> System.out.println("  - " + r.getUri() + ", cache: " + r.getFromCache()));
        });

        new CrawlExecutionValidator(db)
                .validate()
                .checkCrawlLogCount("response", 8)
                .checkCrawlLogCount("revisit", 107)
                .checkCrawlLogCount("dns", 6);
    }

}
