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

import no.nb.nna.veidemann.chrome.client.ws.Cdp;
import no.nb.nna.veidemann.chrome.client.ws.WebsocketClient;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.fail;

public class ChromeDebugProtocolIT {

    static String chromeHost;

    static int chromePort;

    static ChromeDebugProtocolConfig config;

    AtomicBoolean stop = new AtomicBoolean(false);

    @BeforeClass
    public static void init() {
        chromeHost = System.getProperty("browser.host");
        chromePort = Integer.parseInt(System.getProperty("browser.port"));
        config = new ChromeDebugProtocolConfig(chromeHost, chromePort).withMaxOpenSessions(25);
    }

    /**
     * Test of render method, of class BrowserController.
     */
    @Test
    public void testRender() throws Exception {
        System.out.println("Chrome address: " + chromeHost + ":" + chromePort);
        ChromeDebugProtocol chrome = new ChromeDebugProtocol(config);
        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();

        chrome.target().setDiscoverTargets(true).get();
//        chrome.target.onTargetCreated(t -> System.out.println(t));
//        chrome.target.onTargetDestroyed(t -> System.out.println(t));
//        chrome.target.onTargetInfoChanged(t -> System.out.println(t));

        List<Sess> sessions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sessions.add(new Sess(chrome.newSession(1280, 1024)));
            System.out.println("Targets: " + chrome.target().getTargets().get().targetInfos().size());
        }

        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();
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
        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();
        Thread.sleep(5000);
        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();
        long crashed = sessions.stream().filter(s -> s.crashed).count();
        long navigated = sessions.stream().filter(s -> s.navigated.get() == 1).count();
        System.out.println("Crashed: " + crashed + ", Navigated: " + navigated);

//        for (Sess session : sessions) {
//            session.navigate();
//            Thread.sleep(sleep);
//        }
//        sessions.stream().filter(s -> s.crashed).forEach(s -> s.navigate());
        sessions.stream().forEach(s -> {
            s.navigate();
            s.session.close();
        });

        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();
        Thread.sleep(5000);
        chrome.target().getTargets().get().targetInfos().forEach(t -> System.out.println(t));
        System.out.println();
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

        Sess(Session session) throws ExecutionException, InterruptedException, IOException {
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
                session.page().navigate("http://a1.com", "", "").get(20, TimeUnit.SECONDS).frameId();
//                session.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException | IOException e) {
                System.out.println("Failed navigation: " + e);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
//        }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Sess{");
            sb.append("crashed=").append(crashed);
            sb.append(", navigated=").append(navigated);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Test of ping method, of class ChromeDebugClient.
     */
    @Test
    @Ignore
    public void testPing() throws InterruptedException, URISyntaxException {
        System.out.println("ping");
        WebsocketClient instance = new WebsocketClient(null, new URI("ws://echo.websocket.org"), config);
        instance.ping();
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of sendMessage method, of class ChromeDebugClient.
     */
    @Test
    @Ignore
    public void testCall() throws InterruptedException, ExecutionException, URISyntaxException {
        System.out.println("sendMessage");
        String msg = "Hello World";
//        Cdp instance = new Cdp("ws://localhost:9222/devtools/page/cdb3c308-6abe-4159-b09e-4e464b499a92", null, true);
        Cdp instance = null;
        try {
            System.out.println(">>> " + instance.call("mm", null).get());
        } catch (Exception e) {
            System.out.println("E: " + e);
        }
        System.out.println(">>> " + instance.call("mm", null)
                .handle((r, t) -> {
                    System.out.println("  E: " + t + ", R: " + r);
                    return instance.call("mm", null)
                            .handle((r2, t2) -> {
                                System.out.println("  E: " + t2 + ", R: " + r2);
                                return instance.call("mm", null);
                            });
                })
                .handle((r, t) -> {
                    System.out.println("  E: " + t + ", R: " + r);
                    return instance.call("mm", null);
                })
                .get());
//        Thread.sleep(5000);
        instance.onClose("");
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}

