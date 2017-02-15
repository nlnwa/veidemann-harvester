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
package no.nb.nna.broprox.frontier.worker;

import no.nb.nna.broprox.db.model.CrawlConfig;
import org.junit.Test;
import org.netpreserve.commons.uri.UriConfigs;
import org.netpreserve.commons.uri.UriFormat;
//import static org.junit.Assert.*;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class CrawlExecutionTest {

    /**
     * Test of getId method, of class CrawlExecution.
     */
    @Test
    public void testGetId() {
        String uri = "http://johnh.nb.no/?C=D;O=D";
        UriFormat f = UriConfigs.SURT_KEY_FORMAT.ignorePort(true).ignorePath(true).ignoreQuery(true);
        String scope = UriConfigs.SURT_KEY.buildUri(uri).toCustomString(f);
        scope = scope.substring(0, scope.length() - 1);
        System.out.println(" surt: " + UriConfigs.SURT_KEY.buildUri(uri).toString());
        System.out.println("scope: " + scope);
//        fail("X");
    }

}
