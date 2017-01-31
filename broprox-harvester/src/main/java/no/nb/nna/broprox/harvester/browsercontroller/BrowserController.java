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
package no.nb.nna.broprox.harvester.browsercontroller;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.chrome.client.ws.CompleteMany;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class BrowserController implements AutoCloseable {

    private final ChromeDebugProtocol chrome;

    private final String chromeHost;

    private final int chromePort;

    public BrowserController(String chromeHost, int chromePort) throws IOException {
        this.chromeHost = chromeHost;
        this.chromePort = chromePort;
        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
    }

    public byte[] render(String url, int w, int h, int timeout, int sleep) throws ExecutionException, InterruptedException, IOException, TimeoutException {

        try (Session tab = chrome.newSession(w, h)) {
            new CompleteMany(
                    tab.debugger.enable(),
                    tab.page.enable(),
                    tab.runtime.enable(),
                    tab.network.enable(null, null),
                    tab.network.setCacheDisabled(true),
                    tab.runtime.evaluate("navigator.userAgent;", null, false, false, null, false, false, false, false)
                    .thenAccept(e -> {
                        tab.network.setUserAgentOverride(((String) e.result.value)
                                .replace("HeadlessChrome", tab.version()));
                    }),
                    tab.debugger
                    .setBreakpointByUrl(1, null, "https?://www.google-analytics.com/analytics.js", null, null),
                    tab.debugger.setBreakpointByUrl(1, null, "https?://www.google-analytics.com/ga.js", null, null),
                    tab.network.setExtraHTTPHeaders(Collections.singletonMap("x-ray", "foo")),
                    tab.page.setControlNavigations(Boolean.TRUE)
            ).get(timeout, MILLISECONDS);

            tab.debugger.onPaused(p -> {
                String scriptId = p.callFrames.get(0).location.scriptId;
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT BLE PAUSET: " + scriptId);
                tab.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
                tab.debugger.resume();
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT RESUMED: " + scriptId);
            });

            System.out.println("=====================================");

            CompletableFuture<PageDomain.LoadEventFired> loaded = tab.page.onLoadEventFired();

            tab.page.onNavigationRequested(nr -> {
                System.out.println("NAV REQUESTED " + nr);
//                try {
                    tab.network.setExtraHTTPHeaders(Collections.singletonMap("Discovery-Path", "E"));
//                } catch (InterruptedException | ExecutionException ex) {
//                    throw new RuntimeException(ex);
//                }
//                tab.page.setControlNavigations(Boolean.FALSE);
                tab.page.processNavigation("Proceed", nr.navigationId);
            });

//            tab.page.onNavigationRequested(nr -> {
//                System.out.println("NAV REQUESTED");
//                tab.page.processNavigation(nr.url, nr.navigationId);
//            });
            tab.page.navigate(url).get(timeout, MILLISECONDS);

            loaded.get(timeout, MILLISECONDS);
            // disable scrollbars
            tab.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                    null, null, null, null, null, null, null, null)
                    .get(timeout, MILLISECONDS);

            // wait a little for any onload javascript to fire
            Thread.sleep(sleep);

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : tab.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs.type);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
            String data = tab.page.captureScreenshot().get(timeout, MILLISECONDS).data;
            return Base64.getDecoder().decode(data);
//                return null;
        }
    }

    @Override
    public void close() throws Exception {
        chrome.close();
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException, Exception {
        try (BrowserController ab = new BrowserController("localhost", 9222);) {
            ab.render("https://www.nb.no", 500, 500, 10000, 1000);
        }
    }

}
