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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.RuntimeDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.chrome.client.ws.CompleteMany;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.BrowserScript;
import no.nb.nna.broprox.db.model.QueuedUri;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class BrowserController implements AutoCloseable {

    private final ChromeDebugProtocol chrome;

    private final String chromeHost;

    private final int chromePort;

    private final DbAdapter db;

    public BrowserController(String chromeHost, int chromePort, DbAdapter db) throws IOException {
        this.chromeHost = chromeHost;
        this.chromePort = chromePort;
        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
        this.db = db;
    }

//    public byte[] render(String url, int w, int h, long timeout, int sleep) throws ExecutionException, InterruptedException, IOException, TimeoutException {
    public byte[] render(QueuedUri queuedUri, int w, int h, long timeout, int sleep) throws ExecutionException, InterruptedException, IOException, TimeoutException {

        try (Session session = chrome.newSession(w, h)) {
            new CompleteMany(
                    session.debugger.enable(),
                    session.page.enable(),
                    session.runtime.enable(),
                    session.network.enable(null, null),
                    session.network.setCacheDisabled(true),
                    session.runtime
                    .evaluate("navigator.userAgent;", null, false, false, null, false, false, false, false)
                    .thenAccept(e -> {
                        session.network.setUserAgentOverride(((String) e.result.value)
                                .replace("HeadlessChrome", session.version()));
                    }),
                    session.debugger
                    .setBreakpointByUrl(1, null, "https?://www.google-analytics.com/analytics.js", null, null),
                    session.debugger.setBreakpointByUrl(1, null, "https?://www.google-analytics.com/ga.js", null, null),
                    session.network.setExtraHTTPHeaders(Collections.singletonMap("x-ray", "foo")),
                    session.page.setControlNavigations(Boolean.TRUE)
            ).get(timeout, MILLISECONDS);

            session.debugger.onPaused(p -> {
                String scriptId = p.callFrames.get(0).location.scriptId;
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT BLE PAUSET: " + scriptId);
                session.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
                session.debugger.resume();
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT RESUMED: " + scriptId);
            });


            PageExecution pex = new PageExecution(queuedUri, session, timeout);
            pex.navigatePage();
            pex.saveScreenshot(db);

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : session.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
            String script = db.getBrowserScripts(BrowserScript.Type.EXTRACT_OUTLINKS).get(0).getScript();
            pex.extractOutlinks(db, script);
            pex.getDocumentUrl();
            pex.scrollToTop();
                return null;
        }
    }

    @Override
    public void close() {
        chrome.close();
    }

}
