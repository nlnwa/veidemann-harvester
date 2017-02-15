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
package no.nb.nna.broprox.db.model;

import java.beans.Transient;
import java.time.OffsetDateTime;

import no.nb.nna.broprox.db.DbObject;

/**
 *
 */
public interface QueuedUri extends DbObject<QueuedUri> {

    QueuedUri withId(String id);

    String getId();

    String getExecutionId();

    QueuedUri withExecutionId(String id);

    OffsetDateTime getTimeStamp();

    QueuedUri withTimeStamp(OffsetDateTime timeStamp);

    OffsetDateTime getEarliestCrawlTimeStamp();

    QueuedUri withEarliestCrawlTimeStamp(OffsetDateTime timeStamp);

    String getSurt();

    QueuedUri withSurt(String surt);

    String getUri();

    QueuedUri withUri(String uri);

    /**
     * Get the discoveryPath,
     * <p>
     * <table>
     * <tr><td>R</td><td>Redirect</td></tr>
     * <tr><td>E</td><td>Embed</td></tr>
     * <tr><td>X</td><td>Speculative embed (aggressive/Javascript link extraction)</td></tr>
     * <tr><td>L</td><td>Link</td></tr>
     * <tr><td>P</td><td>Prerequisite (as for DNS or robots.txt before another URI)</td></tr>
     * </table>
     * <p>
     * @return the discovery path
     */
    String getDiscoveryPath();

    QueuedUri withDiscoveryPath(String discoveryPath);

    String getReferrer();

    QueuedUri withReferrer(String uri);

    CrawlConfig getCrawlConfig();

    @Transient
    QueuedUri withCrawlConfig(CrawlConfig config);

}
