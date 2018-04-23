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
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocol;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
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

    private final BrowserSessionRegistry sessionRegistry;

    private final Map<String, BrowserScript> scriptCache = new HashMap<>();

    public BrowserController(final String chromeHost, final int chromePort, final int maxOpenSessions,
                             final DbAdapter db, final BrowserSessionRegistry sessionRegistry) {
        DbHelper.getInstance().configure(db);

        LOG.info("Connecting browser at {}:{}", chromeHost, chromePort);
        ChromeDebugProtocolConfig chromeDebugProtocolConfig = new ChromeDebugProtocolConfig(chromeHost, chromePort)
                .withTracer(GlobalTracer.get())
                .withMaxOpenSessions(maxOpenSessions)
                .withProtocolTimeoutMs(10000)
                .withWorkerThreads(32);

        this.chrome = new ChromeDebugProtocol(chromeDebugProtocolConfig);
        this.sessionRegistry = sessionRegistry;
    }

    public RenderResult render(QueuedUri queuedUri, CrawlConfig config)
            throws ExecutionException, IOException, TimeoutException {

        Span span = GlobalTracer.get()
                .buildSpan("render")
//                .asChildOf(OpenTracingContextKey.activeSpan())
                .withTag(Tags.COMPONENT.getKey(), "BrowserController")
                .withTag("executionId", queuedUri.getExecutionId())
                .withTag("uri", queuedUri.getUri())
                .startManual();

        RenderResult result = new RenderResult();

        MDC.put("eid", queuedUri.getExecutionId());
        MDC.put("uri", queuedUri.getUri());

        try {
            BrowserConfig browserConfig = DbHelper.getInstance().getBrowserConfigForCrawlConfig(config);
            BrowserSession session = new BrowserSession(chrome, browserConfig, queuedUri, span);
            try {
                sessionRegistry.put(session);

                session.setBreakpoints();
                session.setCookies();
                session.loadPage();
                session.getCrawlLogs().waitForMatcherToFinish();

                if (session.isPageRenderable()) {
                    if (config.getExtra().getCreateSnapshot()) {
                        LOG.debug("Save screenshot");
                        session.saveScreenshot();
                    }

                    LOG.debug("Extract outlinks");
                    try {
                        List<BrowserScript> scripts = getScripts(browserConfig);
                        result.withOutlinks(session.extractOutlinks(scripts));
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
                    result.getOutlinks().forEach(o -> pageLog.addOutlink(o.getUri()));
                    DbHelper.getInstance().getDb().savePageLog(pageLog.build());
                } catch (Throwable t) {
                    LOG.error("Failed writing pagelog", t);
                }

                result.withBytesDownloaded(session.getUriRequests().getBytesDownloaded())
                        .withUriCount(session.getUriRequests().getUriDownloadedCount());
            } finally {
                session.close();
                sessionRegistry.remove(session);
                span.finish();
            }
        } catch (Throwable t) {
            LOG.error("Failed loading page", t);
            result.withError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
        }

//        result.withBytesDownloaded(session.getUriRequests().getBytesDownloaded())
//                .withUriCount(session.getUriRequests().getUriDownloadedCount());

        MDC.clear();
        return result;
    }

    private List<BrowserScript> getScripts(BrowserConfig browserConfig) {
        List<BrowserScript> scripts = new ArrayList<>();
        try {
            for (String scriptId : browserConfig.getScriptIdList()) {
                BrowserScript script = scriptCache.get(scriptId);
                if (script == null) {
                    ControllerProto.GetRequest req = ControllerProto.GetRequest.newBuilder()
                            .setId(scriptId)
                            .build();
                    script = DbHelper.getInstance().getDb().getBrowserScript(req);
                    scriptCache.put(scriptId, script);
                }
                scripts.add(script);
            }
            ListRequest req = ListRequest.newBuilder()
                    .addAllLabelSelector(browserConfig.getScriptSelectorList())
                    .build();
            for (BrowserScript script : DbHelper.getInstance().getDb().listBrowserScripts(req).getValueList()) {
                if (!scriptCache.containsKey(script.getId())) {
                    scriptCache.put(script.getId(), script);
                }
                scripts.add(script);
            }
        } catch (DbException e) {
            LOG.warn("Could not get browser scripts from DB", e);
        }
        return scripts;
    }

    @Override
    public void close() {
        chrome.close();
    }

}
