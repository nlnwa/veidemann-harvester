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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ChromeDebugProtocolIT {

    static String chromeHost;

    static int chromePort;

    AtomicBoolean stop = new AtomicBoolean(false);

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
        chrome.target().setDiscoverTargets(true).get();
//        chrome.target.onTargetCreated(t -> System.out.println(t));
//        chrome.target.onTargetDestroyed(t -> System.out.println(t));
//        chrome.target.onTargetInfoChanged(t -> System.out.println(t));

        List<Sess> sessions = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            sessions.add(new Sess(chrome.newSession(1280, 1024)));
        }
//        System.out.println(session.version());
//        System.out.println(session.toString());
//        System.out.println("---- " + session.browser.getVersion().get());

        long sleep = 10;

        for (Sess session : sessions) {
            if (stop.get()) {
                break;
            }
            session.navigate();
            Thread.sleep(sleep);
        }
        Thread.sleep(5000);
        long crashed = sessions.stream().filter(s -> s.crashed).count();
        long navigated = sessions.stream().filter(s -> s.navigated.get() == 1).count();
        System.out.println("Crashed: " + crashed + ", Navigated: " + navigated);

//        for (Sess session : sessions) {
//            session.navigate();
//            Thread.sleep(sleep);
//        }
        sessions.stream().filter(s -> s.crashed).forEach(s -> s.navigate());

        Thread.sleep(5000);
        crashed = sessions.stream().filter(s -> s.crashed).count();
        navigated = sessions.stream().filter(s -> s.navigated.get() == 2).count();
        System.out.println("Crashed: " + crashed + ", Navigated: " + navigated);

//        for (Sess session : sessions) {
//            session.session.close();
//        }
    }

    class Sess {
        Session session;
        boolean crashed;
        AtomicInteger navigated = new AtomicInteger();

        Sess(Session session) throws ExecutionException, InterruptedException {
            this.session = session;
            session.page().enable().get();
            session.inspector().enable().get();
            session.inspector().onTargetCrashed(c -> {
                crashed = true;
//                session.close();
//                stop.set(true);
            });
            session.page().onFrameNavigated(n -> navigated.incrementAndGet());
        }

        void navigate() {
//            if (!session.isClosed()) {
            try {
                crashed = false;
                session.page().navigate("http://a1.com", "", "").get(5, TimeUnit.SECONDS).getFrameId();
//                session.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            }
//        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Sess{");
            sb.append("crashed=").append(crashed);
            sb.append(", navigated=").append(navigated);
            sb.append('}');
            return sb.toString();
        }
    }
}

