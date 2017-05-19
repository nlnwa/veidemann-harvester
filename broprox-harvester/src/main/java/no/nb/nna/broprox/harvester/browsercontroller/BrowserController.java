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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.harvester.OpenTracingSpans;
import no.nb.nna.broprox.harvester.proxy.RobotsServiceClient;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class BrowserController implements AutoCloseable, BroproxHeaderConstants {

    private final ChromeDebugProtocol chrome;

    private final DbAdapter db;

    private final Map<String, BrowserScript> scriptCache = new HashMap<>();

    private final RobotsServiceClient robotsServiceClient;

    public BrowserController(String chromeHost, int chromePort, DbAdapter db,
            final RobotsServiceClient robotsServiceClient) throws IOException {

        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
        this.db = db;
        this.robotsServiceClient = robotsServiceClient;
    }

    public List<QueuedUri> render(String executionId, QueuedUri queuedUri, CrawlConfig config)
            throws ExecutionException, InterruptedException, IOException, TimeoutException {
        List<QueuedUri> outlinks;
        System.out.println("RENDER " + executionId + " :: " + queuedUri.getUri());
        OpenTracingWrapper otw = new OpenTracingWrapper("BrowserController", Tags.SPAN_KIND_CLIENT)
                .setParentSpan(OpenTracingSpans.get(executionId));

        // Check robots.txt
        if (robotsServiceClient.isAllowed(executionId, queuedUri.getUri(), config)) {

            try (Session session = chrome.newSession(
                    config.getBrowserConfig().getWindowWidth(),
                    config.getBrowserConfig().getWindowHeight())) {

                CompletableFuture.allOf(
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
                        session.debugger
                                .setBreakpointByUrl(1, null, "https?://www.google-analytics.com/ga.js", null, null),
                        session.page.setControlNavigations(Boolean.TRUE)
                ).get(config.getBrowserConfig().getPageLoadTimeoutMs(), MILLISECONDS);

                // set cookies
                CompletableFuture.allOf(queuedUri.getCookiesList().stream()
                        .map(c -> session.network
                        .setCookie(queuedUri.getUri(), c.getName(), c.getValue(), c.getDomain(),
                                c.getPath(), c.getSecure(), c.getHttpOnly(), c.getSameSite(), c.getExpires()))
                        .collect(Collectors.toList()).toArray(new CompletableFuture[]{}))
                        .get(config.getBrowserConfig().getPageLoadTimeoutMs(), MILLISECONDS);

                session.debugger.onPaused(p -> {
                    String scriptId = p.callFrames.get(0).location.scriptId;
                    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT BLE PAUSET: " + scriptId);
                    session.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
                    session.debugger.resume();
                    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SCRIPT RESUMED: " + scriptId);
                });

                PageExecution pex = new PageExecution(executionId, queuedUri, session, config.getBrowserConfig()
                        .getPageLoadTimeoutMs(), db, config.getBrowserConfig().getSleepAfterPageloadMs());
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
                List<BrowserScript> scripts = getScripts(config);
                outlinks = otw.map("extractOutlinks", pex::extractOutlinks, scripts);
                pex.getDocumentUrl();
                pex.scrollToTop();
            }
        } else {
            outlinks = Collections.EMPTY_LIST;
        }
        return outlinks;

    }

    private List<BrowserScript> getScripts(CrawlConfig config) {
        List<BrowserScript> scripts = new ArrayList<>();
        for (String scriptId : config.getBrowserConfig().getScriptIdList()) {
            BrowserScript script = scriptCache.get(scriptId);
            if (script == null) {
                ControllerProto.BrowserScriptListRequest req = ControllerProto.BrowserScriptListRequest.newBuilder()
                        .setId(scriptId)
                        .build();
                script = db.listBrowserScripts(req).getValue(0);
                scriptCache.put(scriptId, script);
            }
            scripts.add(script);
        }
        if (config.getBrowserConfig().hasScriptSelector()) {
            ControllerProto.BrowserScriptListRequest req = ControllerProto.BrowserScriptListRequest.newBuilder()
                    .setSelector(config.getBrowserConfig().getScriptSelector())
                    .build();
            for (BrowserScript script : db.listBrowserScripts(req).getValueList()) {
                if (!scriptCache.containsKey(script.getId())) {
                    scriptCache.put(script.getId(), script);
                }
                scripts.add(script);
            }
        }
        return scripts;
    }

    @Override
    public void close() {
        chrome.close();
    }

}
