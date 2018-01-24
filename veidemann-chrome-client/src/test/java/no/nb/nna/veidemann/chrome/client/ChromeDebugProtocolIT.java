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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChromeDebugProtocolIT {

    static String chromeHost;

    static int chromePort;

    @BeforeClass
    public static void init() {
        chromeHost = System.getProperty("browser.host");
        chromePort = Integer.parseInt(System.getProperty("browser.port"));
    }

    /**
     * Test of render method, of class BrowserController.
     */
    @Test
    public void testRender() throws Exception {
        System.out.println("Chrome address: " + chromeHost + ":" + chromePort);
        ChromeDebugProtocol chrome = new ChromeDebugProtocol(chromeHost, chromePort, null);
        List<Session> sessions = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sessions.add(chrome.newSession(1280, 1024));
        }
//        System.out.println(session.version());
//        System.out.println(session.toString());
//        System.out.println("---- " + session.browser.getVersion().get());

        long sleep = 10;
        AtomicInteger crashed = new AtomicInteger();
        AtomicInteger navigated = new AtomicInteger();

        for (Session session : sessions) {
//        session.network.enable(0,0,0).get();
            session.page.enable().get();

            session.inspector.onTargetCrashed(c -> {
                crashed.incrementAndGet();
                System.out.println("Session crached " + c.toString());
            });
            session.page.onFrameNavigated(fn -> {
                navigated.incrementAndGet();
                System.out.println(fn.getFrame().getId());
            });
        }

        for (Session session : sessions) {
            session.page.navigate("http://a1.com", "", "").get().getFrameId();
            Thread.sleep(sleep);
        }
        Thread.sleep(5000);
        System.out.println("Crashed: " + crashed.get() + ", Navigated: " + navigated.get());

        crashed.set(0);
        navigated.set(0);
        for (Session session : sessions) {
            session.page.navigate("http://a1.com", "", "").get().getFrameId();
            Thread.sleep(sleep);
        }
        Thread.sleep(5000);
        System.out.println("Crashed: " + crashed.get() + ", Navigated: " + navigated.get());

        for (Session session : sessions) {
            session.close();
        }
    }
}

