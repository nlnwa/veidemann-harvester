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
package no.nb.nna.veidemann.commons.util;

import org.netpreserve.commons.uri.UriConfigs;
import org.netpreserve.commons.uri.UriFormat;

/**
 *
 */
public class CrawlScopes {

    private CrawlScopes() {
    }

    public static String generateDomainScope(String uri) {
        UriFormat f = UriConfigs.SURT_KEY_FORMAT.ignorePort(true).ignorePath(true).ignoreQuery(true);
        String scope = UriConfigs.SURT_KEY.buildUri(uri).toCustomString(f);
        return scope.substring(0, scope.length() - 1);
    }

}
