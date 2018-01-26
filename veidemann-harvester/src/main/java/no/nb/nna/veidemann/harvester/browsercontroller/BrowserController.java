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
package no.nb.nna.veidemann.harvester.browsercontroller;

import io.opentracing.Span;
import io.opentracing.contrib.OpenTracingContextKey;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.HarvesterProto.HarvestPageReply;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocol;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class BrowserController implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserController.class);

    private final ChromeDebugProtocol chrome;

    private final DbAdapter db;

    private final BrowserSessionRegistry sessionRegistry;

    private final Map<String, BrowserScript> scriptCache = new HashMap<>();

    public BrowserController(final String chromeHost, final int chromePort, final DbAdapter db,
                             final BrowserSessionRegistry sessionRegistry)
            throws IOException {

        ChromeDebugProtocolConfig chromeDebugProtocolConfig = new ChromeDebugProtocolConfig(chromeHost, chromePort)
                .withTracer(GlobalTracer.get());
        this.chrome = new ChromeDebugProtocol(chromeDebugProtocolConfig);
        this.db = db;
        this.sessionRegistry = sessionRegistry;
    }

    public HarvestPageReply render(QueuedUri queuedUri, CrawlConfig config)
            throws ExecutionException, InterruptedException, IOException, TimeoutException {

        Span span = GlobalTracer.get()
                .buildSpan("render")
                .asChildOf(OpenTracingContextKey.activeSpan())
                .withTag(Tags.COMPONENT.getKey(), "BrowserController")
                .withTag("executionId", queuedUri.getExecutionId())
                .withTag("uri", queuedUri.getUri())
                .startManual();

        HarvestPageReply.Builder resultBuilder = HarvestPageReply.newBuilder();

        MDC.put("eid", queuedUri.getExecutionId());
        MDC.put("uri", queuedUri.getUri());

        BrowserSession session = new BrowserSession(db, chrome, config, queuedUri, span);
        try {
            sessionRegistry.put(session);

            session.setBreakpoints();
            session.setCookies();
            session.loadPage();
            session.getCrawlLogs().waitForMatcherToFinish();

            if (session.isPageRenderable()) {
                if (config.getExtra().getCreateSnapshot()) {
                    LOG.debug("Save screenshot");
                    session.saveScreenshot(db);
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

                try {
                    List<BrowserScript> scripts = getScripts(config);
                    resultBuilder.addAllOutlinks(session.extractOutlinks(scripts));
                } catch (Throwable t) {
                    LOG.error("Failed extracting outlinks", t);
                }

                session.scrollToTop();

            } else {
                LOG.info("Page is not renderable");
            }
            try {

                LOG.debug("======== PAGELOG ========\n{}", session.getUriRequests());

                PageLog.Builder pageLog = PageLog.newBuilder()
                        .setUri(queuedUri.getUri())
                        .setExecutionId(queuedUri.getExecutionId());
                if (session.getUriRequests().getInitialRequest() == null) {
                    LOG.error("Missing initial request");
                } else {
                    pageLog.setWarcId(session.getUriRequests().getInitialRequest().getWarcId())
                            .setReferrer(session.getUriRequests().getInitialRequest().getReferrer());
                }

                session.getUriRequests().getPageLogResources().forEach(r -> pageLog.addResource(r));
                resultBuilder.getOutlinksOrBuilderList().forEach(o -> pageLog.addOutlink(o.getUri()));
                db.savePageLog(pageLog.build());
            } catch (Throwable t) {
                LOG.error("Failed writing pagelog", t);
            }
        } catch (Throwable t) {
            LOG.error("Failed loading page", t);
        } finally {
            session.close();
            sessionRegistry.remove(session);
            span.finish();
        }

        resultBuilder.setBytesDownloaded(session.getUriRequests().getBytesDownloaded());
        resultBuilder.setUriCount(session.getUriRequests().getUriDownloadedCount());

        LOG.trace("======== PAGELOAD RESULT ========\n{}", resultBuilder.build());
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
