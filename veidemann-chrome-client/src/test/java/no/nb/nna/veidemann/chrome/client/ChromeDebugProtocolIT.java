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
package no.nb.nna.veidemann.chrome.client;

import org.junit.BeforeClass;
import org.junit.Test;

public class ChromeDebugProtocolIT {

    static String chromeHost;

    static int chromePort;

    static String testSitesHttpHost;

    static int testSitesHttpPort;

    static String testSitesDnsHost;

    static int testSitesDnsPort;


    @BeforeClass
    public static void init() {
        chromeHost = System.getProperty("browser.host");
        chromePort = Integer.parseInt(System.getProperty("browser.port"));
        testSitesHttpHost = System.getProperty("testsites.http.host");
        testSitesHttpPort = Integer.parseInt(System.getProperty("testsites.http.port"));
        testSitesDnsHost = System.getProperty("testsites.dns.host");
        testSitesDnsPort = Integer.parseInt(System.getProperty("testsites.dns.port"));
    }

    /**
     * Test of render method, of class BrowserController.
     */
    @Test
    public void testRender() throws Exception {
        System.out.println("Chrome address: " + chromeHost + ":" + chromePort);
        ChromeDebugProtocol chrome = new ChromeDebugProtocol(chromeHost, chromePort, null);
        System.out.println("111111");
        Thread.sleep(1000);
        Session session = chrome.newSession(1280, 1024);
        System.out.println("111111");
        Thread.sleep(1000);
        System.out.println(session.version());
        System.out.println(session.toString());

        System.out.println("---- " + session.browser.getVersion().get());

        ChromeDebugProtocol chrome2 = new ChromeDebugProtocol(chromeHost, chromePort, null);
        System.out.println("222222");
        Thread.sleep(1000);
        Session session2 = chrome2.newSession(1280, 1024);
        System.out.println("222222");
        Thread.sleep(1000);
        System.out.println(session2.version());
        System.out.println(session2.toString());

        System.out.println("---- " + session2.browser.getVersion().get());

        System.out.println("111111");
        Thread.sleep(1000);
        System.out.println(session.version());
        System.out.println(session.toString());

        System.out.println("---- " + session.browser.getVersion().get());

        session.network.enable(0,0,0).get();
        session.page.enable().get();
        session.page.navigate("http://a1.com", "", "");
        System.out.println("111111");
        Thread.sleep(20000);
    }
}

