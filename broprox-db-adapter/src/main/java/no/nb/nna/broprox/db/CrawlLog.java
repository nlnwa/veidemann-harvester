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

import java.time.OffsetDateTime;

/**
 *
 */
public interface CrawlLog extends DbObject {

    public String getWarcId();

    public CrawlLog withWarcId(String warcId);

    public OffsetDateTime getTimeStamp();

    public CrawlLog withTimeStamp(OffsetDateTime timeStamp);

    public String getSurt();

    public CrawlLog withSurt(String surt);

    public int getStatusCode();

    public CrawlLog withStatusCode(int statusCode);

    public long getSize();

    public CrawlLog withSize(long size);

    public String getRequestedUri();

    public CrawlLog withRequestedUri(String uri);

    public String getResponseUri();

    public CrawlLog withResponseUri(String uri);

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
    public String getDiscoveryPath();

    public CrawlLog withDiscoveryPath(String discoveryPath);

    public String getReferrer();

    public CrawlLog withReferrer(String uri);

    public String getContentType();

    public CrawlLog withContentType(String contentType);

    public OffsetDateTime getFetchTimeStamp();

    public CrawlLog withFetchTimeStamp(OffsetDateTime timeStamp);

    public long getFetchTimeMillis();

    public CrawlLog withFetchTimeMillis(long duration);

    public String getDigest();

    public CrawlLog withDigest(String digest);

    public String getStorageRef();

    public CrawlLog withStorageRef(String storageRef);

    public String getRecordType();

    public CrawlLog withRecordType(String type);
}
