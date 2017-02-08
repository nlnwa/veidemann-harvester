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

import java.io.Closeable;
import java.util.List;

import no.nb.nna.broprox.db.model.CrawledContent;
import no.nb.nna.broprox.db.model.CrawlLog;
import no.nb.nna.broprox.db.model.ExtractedText;

import java.util.Optional;

import no.nb.nna.broprox.db.model.BrowserScript;
import no.nb.nna.broprox.db.model.QueuedUri;
import no.nb.nna.broprox.db.model.Screenshot;

/**
 * Adapter for Broprox's database.
 */
public interface DbAdapter extends AutoCloseable {

    CrawledContent addCrawledContent(CrawledContent cc);

    Optional<CrawledContent> isDuplicateContent(String digest);

    CrawlLog addCrawlLog(CrawlLog cl);

    CrawlLog updateCrawlLog(CrawlLog cl);

    ExtractedText addExtractedText(ExtractedText et);

    List<BrowserScript> getBrowserScripts(BrowserScript.Type type);

    QueuedUri addQueuedUri(QueuedUri qu);

    QueuedUri updateQueuedUri(QueuedUri qu);

    Screenshot addScreenshot(Screenshot s);

    @Override
    public void close();

}
