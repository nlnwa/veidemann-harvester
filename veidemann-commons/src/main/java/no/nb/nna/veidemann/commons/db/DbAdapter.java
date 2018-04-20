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

    Optional<CrawledContent> hasCrawledContent(CrawledContent cc) throws DbException;

    CrawlLog saveCrawlLog(CrawlLog cl) throws DbException;

    CrawlLogListReply listCrawlLogs(CrawlLogListRequest request) throws DbException;

    PageLog savePageLog(PageLog pageLog) throws DbException;

    PageLogListReply listPageLogs(PageLogListRequest request) throws DbException;

    ExtractedText addExtractedText(ExtractedText et) throws DbException;

    JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException;

    JobExecutionStatus getJobExecutionStatus(String jobExecutionId) throws DbException;

    JobExecutionsListReply listJobExecutionStatus(ListJobExecutionsRequest request) throws DbException;

    /**
     * Update the state for a Job Execution to ABORTED_MANUAL.
     *
     * @param jobExecutionId id of the job execution to update
     */
    JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId) throws DbException;

    CrawlExecutionStatus saveExecutionStatus(CrawlExecutionStatus status) throws DbException;

    CrawlExecutionStatus getExecutionStatus(String executionId) throws DbException;

    ExecutionsListReply listExecutionStatus(ListExecutionsRequest request) throws DbException;

    /**
     * Update the state for a Crawl Execution to ABORTED_MANUAL.
     * <p>
     * The frontier should detect this and abort the crawl.
     *
     * @param executionId id of the execution to update
     */
    CrawlExecutionStatus setExecutionStateAborted(String executionId) throws DbException;

    QueuedUri saveQueuedUri(QueuedUri qu) throws DbException;

    void deleteQueuedUri(QueuedUri qu) throws DbException;

    long deleteQueuedUrisForExecution(String executionId) throws DbException;

    long queuedUriCount(String executionId) throws DbException;

    boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException;

    /**
     * Get the first URI wich is ready to be fetched for a CrawlHostGroup.
     *
     * @param crawlHostGroup the CrawlHostGroup for which a URI is requested
     * @return an Optional containing the next URI to be fetched or empty if none are ready yet.
     */
    FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup) throws DbException;

    CrawlHostGroup getOrCreateCrawlHostGroup(String crawlHostGroupId, String politenessId) throws DbException;

    FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() throws DbException;

    CrawlHostGroup releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) throws DbException;

    ScreenshotListReply listScreenshots(ScreenshotListRequest request) throws DbException;

    Screenshot saveScreenshot(Screenshot screenshot) throws DbException;

    Empty deleteScreenshot(Screenshot screenshot) throws DbException;

    CrawlEntity getCrawlEntity(GetRequest req) throws DbException;

    CrawlEntity saveCrawlEntity(CrawlEntity msg) throws DbException;

    CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException;

    Empty deleteCrawlEntity(CrawlEntity entity) throws DbException;

    Seed getSeed(GetRequest req) throws DbException;

    SeedListReply listSeeds(SeedListRequest request) throws DbException;

    Seed saveSeed(Seed seed) throws DbException;

    Empty deleteSeed(Seed seed) throws DbException;

    CrawlJob getCrawlJob(GetRequest req) throws DbException;

    CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException;

    CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException;

    Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException;

    CrawlConfig getCrawlConfig(GetRequest req) throws DbException;

    CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException;

    CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException;

    CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException;

    CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException;

    PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException;

    PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    BrowserConfig getBrowserConfig(GetRequest req) throws DbException;

    BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException;

    BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException;

    Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException;

    BrowserScript getBrowserScript(GetRequest req) throws DbException;

    BrowserScript saveBrowserScript(BrowserScript script) throws DbException;

    Empty deleteBrowserScript(BrowserScript script) throws DbException;

    BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException;

    CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException;

    CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException;

    LogLevels getLogConfig() throws DbException;

    LogLevels saveLogConfig(LogLevels logLevels) throws DbException;

    ChangeFeed<RunningExecutionsListReply> getExecutionStatusStream(RunningExecutionsRequest request) throws DbException;

    RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException;

    RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException;

    Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException;

    @Override
    public void close();

}
