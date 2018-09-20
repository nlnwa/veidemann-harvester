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
package no.nb.nna.veidemann.commons.db;

import com.google.protobuf.Empty;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;

import java.util.Optional;

/**
 * Adapter for Veidemann's database.
 */
public interface DbAdapter {

    Optional<CrawledContent> hasCrawledContent(CrawledContent cc) throws DbException;

    CrawlLog saveCrawlLog(CrawlLog cl) throws DbException;

    CrawlLogListReply listCrawlLogs(CrawlLogListRequest request) throws DbException;

    PageLog savePageLog(PageLog pageLog) throws DbException;

    PageLogListReply listPageLogs(PageLogListRequest request) throws DbException;

    ExtractedText addExtractedText(ExtractedText et) throws DbException;

    ScreenshotListReply listScreenshots(ScreenshotListRequest request) throws DbException;

    Screenshot saveScreenshot(Screenshot screenshot) throws DbException;

    Empty deleteScreenshot(Screenshot screenshot) throws DbException;

    /**
     * Set the desired pause state for Veidemann
     *
     * @param value true if Veidemann should pause
     * @return the old state
     * @throws DbException
     */
    boolean setDesiredPausedState(boolean value) throws DbException;

    /**
     * Get the desired pause state for Veidemann
     *
     * @return true if Veidemann should pause
     * @throws DbException
     */
    boolean getDesiredPausedState() throws DbException;

    /**
     * Get the actual pause state for Veidemann
     *
     * @return true if Veidemann is paused
     * @throws DbException
     */
    boolean isPaused() throws DbException;

}
