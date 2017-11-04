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

package no.nb.nna.veidemann.robotsparser;

import java.io.FileReader;
import java.io.IOException;

import no.nb.nna.veidemann.robotsparser.RobotsTxt;
import no.nb.nna.veidemann.robotsparser.RobotsTxtParser;
import org.junit.Test;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class RobotsTxtParserTest {

    @Test
    public void testSomeMethod() throws IOException {
        RobotsTxtParser parser = new RobotsTxtParser();
        RobotsTxt robots = parser.parse(new FileReader("src/test/resources/examples/robotstxt/robots1.txt"));

        Uri denied = UriConfigs.WHATWG.buildUri("http://example.com/denied");
        Uri allowed = UriConfigs.WHATWG.buildUri("http://example.com/allowed");

        assertThat(robots.isAllowed("Googlebot/2.1 (+http://www.google.com/bot.html)", denied)).isFalse();
        assertThat(robots.isAllowed("Googlebot/2.1 (+http://www.google.com/bot.html)", allowed)).isTrue();
    }

}