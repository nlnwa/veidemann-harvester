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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.opentracing.tag.Tags;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.chrome.client.ws.CompleteMany;
import no.nb.nna.broprox.commons.OpenTracingWrapper;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.harvester.BroproxHeaderConstants;
import no.nb.nna.broprox.harvester.OpenTracingSpans;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class BrowserController implements AutoCloseable, BroproxHeaderConstants {

    private final ChromeDebugProtocol chrome;

    private final String chromeHost;

    private final int chromePort;

    private final DbAdapter db;

    private final int sleep = 500;

    public BrowserController(String chromeHost, int chromePort, DbAdapter db) throws IOException {
        this.chromeHost = chromeHost;
        this.chromePort = chromePort;
        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
        this.db = db;
    }

    public List<QueuedUri> render(String executionId, QueuedUri queuedUri, CrawlConfig config)
            throws ExecutionException, InterruptedException, IOException, TimeoutException {

        System.out.println("RENDER " + executionId + " :: " + queuedUri.getUri());
        OpenTracingWrapper otw = new OpenTracingWrapper("BrowserController", Tags.SPAN_KIND_CLIENT)
                .setParentSpan(OpenTracingSpans.get(executionId));

        try (Session session = chrome.newSession(
                config.getBrowserConfig().getWindowWidth(),
                config.getBrowserConfig().getWindowHeight())) {

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
                    session.page.setControlNavigations(Boolean.TRUE)
            ).get(config.getBrowserConfig().getPageLoadTimeoutMs(), MILLISECONDS);

            session.debugger.onPaused(p -> {
                String scriptId = p.callFrames.get(0).location.scriptId;
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT BLE PAUSET: " + scriptId);
                session.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
                session.debugger.resume();
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT RESUMED: " + scriptId);
            });

            PageExecution pex = new PageExecution(executionId, queuedUri, session, config.getBrowserConfig()
                    .getPageLoadTimeoutMs(), db, sleep);
            otw.run("navigatePage", pex::navigatePage);
            otw.run("saveScreenshot", pex::saveScreenshot);

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : session.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
            String script = db.getBrowserScripts(BrowserScript.Type.EXTRACT_OUTLINKS).get(0).getScript();
            List<QueuedUri> outlinks = otw.map("extractOutlinks", pex::extractOutlinks, script);
            pex.getDocumentUrl();
            pex.scrollToTop();
            return outlinks;
        }
    }

    @Override
    public void close() {
        chrome.close();
    }

}
