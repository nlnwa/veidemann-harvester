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
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import chropro.Chropro;
import chropro.PageDomain;
import com.google.gson.Gson;

/**
 *
 */
public class BrowserController implements AutoCloseable {

    private final Chropro chrome;

    private final String chromeHost;

    private final int chromePort;

    public BrowserController(String chromeHost, int chromePort) throws IOException {
        this.chromeHost = chromeHost;
        this.chromePort = chromePort;
        this.chrome = getBaseConnection();
    }

    public byte[] render(String url, int w, int h, int timeout, int sleep) throws ExecutionException, InterruptedException, IOException, TimeoutException {
        String contextId;

        try {
            contextId = chrome.target.createBrowserContext().get(timeout, TimeUnit.MILLISECONDS).browserContextId;
        } catch (ExecutionException e) {
            contextId = null; // browser contexts are only supported on headless chrome
        }
        String targetId = null;

        try {
            targetId = chrome.target.createTarget("about:blank", w, h, contextId)
                    .get(timeout, TimeUnit.MILLISECONDS).targetId;

            // Chrome is buggy and won't let us connect unless we've refreshed the json endpoint
            new URL("http://" + chromeHost + ":" + chromePort + "/json").openStream().close();
            try (Chropro tab = new Chropro("ws://" + chromeHost + ":" + chromePort + "/devtools/page/" + targetId)) {

                tab.page.enable().get(timeout, TimeUnit.MILLISECONDS);
                CompletableFuture<PageDomain.LoadEventFired> loaded = tab.page.onLoadEventFired();

                tab.page.navigate(url).get(timeout, TimeUnit.MILLISECONDS);

                loaded.get(timeout, TimeUnit.MILLISECONDS);

                // disable scrollbars
                tab.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                        null, null, null, null, null, null, null, null)
                        .get(timeout, TimeUnit.MILLISECONDS);

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
                String data = tab.page.captureScreenshot().get(timeout, TimeUnit.MILLISECONDS).data;
                return Base64.getDecoder().decode(data);
//                return null;
            }
        } finally {
            if (targetId != null) {
                chrome.target.closeTarget(targetId).get(timeout, TimeUnit.MILLISECONDS);
            }
            if (contextId != null) {
                chrome.target.disposeBrowserContext(contextId).get(timeout, TimeUnit.MILLISECONDS);
            }
        }
    }

    private Chropro getBaseConnection() throws MalformedURLException, IOException {
        Gson gson = new Gson();
        List l = gson.fromJson(new InputStreamReader(new URL("http://" + chromeHost + ":" + chromePort + "/json")
                .openStream()), List.class);
        if (l.isEmpty()) {
            throw new RuntimeException();
        }
        Map<String, String> m = (Map) l.get(0);
        return new Chropro(m.get("webSocketDebuggerUrl"));
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
