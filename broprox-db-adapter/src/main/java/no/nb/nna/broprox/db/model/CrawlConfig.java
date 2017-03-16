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

import no.nb.nna.broprox.db.DbObject;

/**
 *
 */
public interface CrawlConfig extends DbObject<CrawlConfig> {

    CrawlConfig withWindowWidth(int width);

    int getWindowWidth();

    CrawlConfig withWindowHeight(int height);

    int getWindowHeight();

    CrawlConfig withScope(String scope);

    String getScope();

    CrawlConfig withPageLoadTimeout(long timeout);

    long getPageLoadTimeout();

    CrawlConfig withMinTimeBetweenPageLoadMillis(long time);

    long getMinTimeBetweenPageLoadMillis();

}
