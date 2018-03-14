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
package no.nb.nna.veidemann.commons.db;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsRequest;

import java.util.Optional;

/**
 * Adapter for Veidemann's database.
 */
public interface DbAdapter extends AutoCloseable {

    Optional<CrawledContent> hasCrawledContent(CrawledContent cc);

    CrawlLog saveCrawlLog(CrawlLog cl);

    CrawlLogListReply listCrawlLogs(CrawlLogListRequest request);

    PageLog savePageLog(PageLog pageLog);

    PageLogListReply listPageLogs(PageLogListRequest request);

    ExtractedText addExtractedText(ExtractedText et);

    JobExecutionStatus saveJobExecutionStatus(JobExecutionStatus status);

    JobExecutionStatus getJobExecutionStatus(String jobExecutionId);

    JobExecutionsListReply listJobExecutionStatus(ListJobExecutionsRequest request);

    /**
     * Update the state for a Job Execution to ABORTED_MANUAL.
     *
     * @param jobExecutionId id of the job execution to update
     */
    JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId);

    CrawlExecutionStatus saveExecutionStatus(CrawlExecutionStatus status);

    CrawlExecutionStatus getExecutionStatus(String executionId);

    ExecutionsListReply listExecutionStatus(ListExecutionsRequest request);

    /**
     * Update the state for a Crawl Execution to ABORTED_MANUAL.
     * <p>
     * The frontier should detect this and abort the crawl.
     *
     * @param executionId id of the execution to update
     */
    CrawlExecutionStatus setExecutionStateAborted(String executionId);

    QueuedUri saveQueuedUri(QueuedUri qu);

    void deleteQueuedUri(QueuedUri qu);

    long deleteQueuedUrisForExecution(String executionId);

    long queuedUriCount(String executionId);

    boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since);

    /**
     * Get the first URI wich is ready to be fetched for a CrawlHostGroup.
     *
     * @param crawlHostGroup the CrawlHostGroup for which a URI is requested
     * @return an Optional containing the next URI to be fetched or empty if none are ready yet.
     */
    FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup);

    CrawlHostGroup getOrCreateCrawlHostGroup(String crawlHostGroupId, String politenessId);

    FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup();

    CrawlHostGroup releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs);

    ScreenshotListReply listScreenshots(ScreenshotListRequest request);

    Screenshot saveScreenshot(Screenshot screenshot);

    Empty deleteScreenshot(Screenshot screenshot);

    CrawlEntity getCrawlEntity(GetRequest req);

    CrawlEntity saveCrawlEntity(CrawlEntity msg);

    CrawlEntityListReply listCrawlEntities(ListRequest request);

    Empty deleteCrawlEntity(CrawlEntity entity);

    Seed getSeed(GetRequest req);

    SeedListReply listSeeds(SeedListRequest request);

    Seed saveSeed(Seed seed);

    Empty deleteSeed(Seed seed);

    CrawlJob getCrawlJob(GetRequest req);

    CrawlJobListReply listCrawlJobs(ListRequest request);

    CrawlJob saveCrawlJob(CrawlJob crawlJob);

    Empty deleteCrawlJob(CrawlJob crawlJob);

    CrawlConfig getCrawlConfig(GetRequest req);

    CrawlConfigListReply listCrawlConfigs(ListRequest request);

    CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig);

    Empty deleteCrawlConfig(CrawlConfig crawlConfig);

    CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req);

    CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request);

    CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig);

    Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig);

    PolitenessConfig getPolitenessConfig(GetRequest req);

    PolitenessConfigListReply listPolitenessConfigs(ListRequest request);

    PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig);

    Empty deletePolitenessConfig(PolitenessConfig politenessConfig);

    BrowserConfig getBrowserConfig(GetRequest req);

    BrowserConfigListReply listBrowserConfigs(ListRequest request);

    BrowserConfig saveBrowserConfig(BrowserConfig browserConfig);

    Empty deleteBrowserConfig(BrowserConfig browserConfig);

    BrowserScript getBrowserScript(GetRequest req);

    BrowserScript saveBrowserScript(BrowserScript script);

    Empty deleteBrowserScript(BrowserScript script);

    BrowserScriptListReply listBrowserScripts(ListRequest request);

    CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req);

    CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig);

    Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig);

    CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request);

    LogLevels getLogConfig();

    LogLevels saveLogConfig(LogLevels logLevels);

    ChangeFeed<RunningExecutionsListReply> getExecutionStatusStream(RunningExecutionsRequest request);

    RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request);

    RoleMapping saveRoleMapping(RoleMapping roleMapping);

    Empty deleteRoleMapping(RoleMapping roleMapping);

    @Override
    public void close();

}
