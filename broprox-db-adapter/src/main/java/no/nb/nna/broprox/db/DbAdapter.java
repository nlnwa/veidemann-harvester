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
package no.nb.nna.broprox.db;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.Empty;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;

/**
 * Adapter for Broprox's database.
 */
public interface DbAdapter extends AutoCloseable {

    CrawledContent addCrawledContent(CrawledContent cc);

    Optional<CrawledContent> isDuplicateContent(String digest);

    CrawlLog addCrawlLog(CrawlLog cl);

    CrawlLog updateCrawlLog(CrawlLog cl);

    ExtractedText addExtractedText(ExtractedText et);

    BrowserScript saveBrowserScript(BrowserScript script);

    List<BrowserScript> getBrowserScripts(BrowserScript.Type type);

    CrawlExecutionStatus addExecutionStatus(CrawlExecutionStatus status);

    CrawlExecutionStatus updateExecutionStatus(CrawlExecutionStatus status);

    QueuedUri addQueuedUri(QueuedUri qu);

    QueuedUri updateQueuedUri(QueuedUri qu);

    Screenshot addScreenshot(Screenshot s);

    CrawlEntity saveCrawlEntity(CrawlEntity msg);

    CrawlEntityListReply listCrawlEntities(ListRequest request);

    Empty deleteCrawlEntity(CrawlEntity entity);

    @Override
    public void close();

}
