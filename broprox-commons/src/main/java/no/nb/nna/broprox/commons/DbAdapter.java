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
package no.nb.nna.broprox.commons;

import java.util.Optional;

import com.google.protobuf.Empty;
import no.nb.nna.broprox.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.api.StatusProto.ExecutionsListReply;
import no.nb.nna.broprox.api.StatusProto.ExecutionsRequest;
import no.nb.nna.broprox.model.ConfigProto.BrowserConfig;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.LogLevels;
import no.nb.nna.broprox.model.ConfigProto.PolitenessConfig;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlHostGroup;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;

/**
 * Adapter for Broprox's database.
 */
public interface DbAdapter extends AutoCloseable {

    Optional<CrawledContent> hasCrawledContent(CrawledContent cc);

    CrawlLog addCrawlLog(CrawlLog cl);

    CrawlLog updateCrawlLog(CrawlLog cl);

    ExtractedText addExtractedText(ExtractedText et);

    CrawlExecutionStatus addExecutionStatus(CrawlExecutionStatus status);

    CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status);

    CrawlExecutionStatus getExecutionStatus(String executionId);

    QueuedUri addQueuedUri(QueuedUri qu);

    QueuedUri updateQueuedUri(QueuedUri qu);

    void deleteQueuedUri(QueuedUri qu);

    long queuedUriCount(String executionId);

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

    Screenshot addScreenshot(Screenshot s);

    CrawlEntity saveCrawlEntity(CrawlEntity msg);

    CrawlEntityListReply listCrawlEntities(ListRequest request);

    Empty deleteCrawlEntity(CrawlEntity entity);

    SeedListReply listSeeds(SeedListRequest request);

    Seed saveSeed(Seed seed);

    Empty deleteSeed(Seed seed);

    CrawlJobListReply listCrawlJobs(CrawlJobListRequest request);

    CrawlJob saveCrawlJob(CrawlJob crawlJob);

    Empty deleteCrawlJob(CrawlJob crawlJob);

    CrawlConfigListReply listCrawlConfigs(ListRequest request);

    CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig);

    Empty deleteCrawlConfig(CrawlConfig crawlConfig);

    CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request);

    CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig);

    Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig);

    PolitenessConfigListReply listPolitenessConfigs(ListRequest request);

    PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig);

    Empty deletePolitenessConfig(PolitenessConfig politenessConfig);

    BrowserConfigListReply listBrowserConfigs(ListRequest request);

    BrowserConfig saveBrowserConfig(BrowserConfig browserConfig);

    Empty deleteBrowserConfig(BrowserConfig browserConfig);

    BrowserScript saveBrowserScript(BrowserScript script);

    Empty deleteBrowserScript(BrowserScript script);

    BrowserScriptListReply listBrowserScripts(BrowserScriptListRequest request);

    CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig);

    Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig);

    CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request);

    LogLevels getLogConfig();

    LogLevels saveLogConfig(LogLevels logLevels);

    ChangeFeed<ExecutionsListReply> getExecutionStatusStream(ExecutionsRequest request);

    @Override
    public void close();

}
