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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.opentracing.tag.Tags;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.harvester.BrowserSessionRegistry;
import no.nb.nna.broprox.harvester.OpenTracingSpans;
import no.nb.nna.broprox.commons.client.RobotsServiceClient;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class BrowserController implements AutoCloseable, BroproxHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserController.class);

    private final ChromeDebugProtocol chrome;

    private final DbAdapter db;

    private final BrowserSessionRegistry sessionRegistry;

    private final Map<String, BrowserScript> scriptCache = new HashMap<>();

    private final RobotsServiceClient robotsServiceClient;

    public BrowserController(final String chromeHost, final int chromePort, final DbAdapter db,
            final RobotsServiceClient robotsServiceClient, final BrowserSessionRegistry sessionRegistry)
            throws IOException {

        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
        this.db = db;
        this.robotsServiceClient = robotsServiceClient;
        this.sessionRegistry = sessionRegistry;
    }

    public HarvestPageReply render(QueuedUri queuedUri, CrawlConfig config)
            throws ExecutionException, InterruptedException, IOException, TimeoutException {
        HarvestPageReply.Builder resultBuilder = HarvestPageReply.newBuilder();

        MDC.put("eid", queuedUri.getExecutionId());
        MDC.put("uri", queuedUri.getUri());

        OpenTracingWrapper otw = new OpenTracingWrapper("BrowserController", Tags.SPAN_KIND_CLIENT)
                .setParentSpan(OpenTracingSpans.get(queuedUri.getExecutionId()));

        // Check robots.txt
        if (robotsServiceClient.isAllowed(queuedUri, config)) {

            try (BrowserSession session = new BrowserSession(chrome, config, queuedUri.getExecutionId())) {
                sessionRegistry.put(session);

                session.setBreakpoints();
                session.setCookies(queuedUri);
                session.loadPage(queuedUri);
                if (session.isPageRenderable()) {
//                // disable scrollbars
//                session.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
//                        null, null, null, null, null, null, null, null)
//                        .get(protocolTimeout, MILLISECONDS);

                    if (config.getExtra().getCreateSnapshot()) {
                        LOG.debug("Save screenshot");
                        otw.run("saveScreenshot", session::saveScreenshot, db);
                    }

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : session.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
                    LOG.debug("Extract outlinks");

                    List<BrowserScript> scripts = getScripts(config);
                    resultBuilder.addAllOutlinks(otw.map("extractOutlinks", session::extractOutlinks, scripts));
                    resultBuilder.setBytesDownloaded(session.getPageRequests().getBytesDownloaded());
                    resultBuilder.setUriCount(session.getPageRequests().getUriDownloadedCount());

                    session.scrollToTop();
                }
                sessionRegistry.remove(session);
            }
        } else {
            LOG.info("Precluded by robots.txt");

            // Precluded by robots.txt
            if (db != null) {
                MessagesProto.CrawlLog crawlLog = MessagesProto.CrawlLog.newBuilder()
                        .setRequestedUri(queuedUri.getUri())
                        .setSurt(queuedUri.getSurt())
                        .setRecordType("response")
                        .setStatusCode(-9998)
                        .setFetchTimeStamp(ProtoUtils.getNowTs())
                        .build();
                db.addCrawlLog(crawlLog);
            }
        }

        MDC.clear();
        return resultBuilder.build();
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
