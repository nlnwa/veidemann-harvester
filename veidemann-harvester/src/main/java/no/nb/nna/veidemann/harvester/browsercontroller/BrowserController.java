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
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.FrontierClient.ProxySession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BrowserController implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserController.class);

    private final String browserWSEndpoint;

    private final ChromeDebugProtocol chrome;

    final ChromeDebugProtocolConfig chromeDebugProtocolConfig;

    private final BrowserSessionRegistry sessionRegistry;

    private final Map<String, BrowserScript> scriptCache = new HashMap<>();

    public BrowserController(final String browserWSEndpoint, final BrowserSessionRegistry sessionRegistry) {
        this.browserWSEndpoint = browserWSEndpoint;

        chromeDebugProtocolConfig = new ChromeDebugProtocolConfig()
                .withTracer(GlobalTracer.get())
                .withProtocolTimeoutMs(30000);

        this.chrome = new ChromeDebugProtocol();
        this.sessionRegistry = sessionRegistry;
    }

    public RenderResult render(ProxySession proxySession, QueuedUri queuedUri, CrawlConfig config) throws IOException {
        LOG.trace("Connecting to browser with: " + proxySession.getBrowserWsEndpoint());
        ChromeDebugProtocolConfig protocolConfig = chromeDebugProtocolConfig.withBrowserWSEndpoint(proxySession.getBrowserWsEndpoint());

        return render(proxySession.getProxyId(), protocolConfig, queuedUri, config);
    }

    public RenderResult render(int proxyId, ChromeDebugProtocolConfig protocolConfig,
                               QueuedUri queuedUri, CrawlConfig config) throws IOException {

        Span span = GlobalTracer.get()
                .buildSpan("render")
                .withTag(Tags.COMPONENT.getKey(), "BrowserController")
                .withTag("executionId", queuedUri.getExecutionId())
                .withTag("uri", queuedUri.getUri())
                .startManual();

        RenderResult result = new RenderResult();

        MDC.put("eid", queuedUri.getExecutionId());
        MDC.put("uri", queuedUri.getUri());

        BrowserConfig browserConfig = null;
        BrowserSession session = null;
        try {
            browserConfig = DbHelper.getBrowserConfigForCrawlConfig(config);
            session = new BrowserSession(proxyId, chrome.connect(protocolConfig),
                    browserConfig, queuedUri, span);
        } catch (Exception t) {
            if (session != null) {
                session.close();
            }
            span.finish();

            LOG.error("Failed creating session", t);
            result.withError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));

            return result;
        }

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
                } catch (Exception t) {
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
                DbService.getInstance().getDbAdapter().savePageLog(pageLog.build());
            } catch (Exception t) {
                LOG.error("Failed writing pagelog", t);
            }

            result.withBytesDownloaded(session.getUriRequests().getBytesDownloaded())
                    .withUriCount(session.getUriRequests().getUriDownloadedCount())
                    .withPageFetchTimeMs(session.getCrawlLogs().getFetchTimeMs());
        } catch (Exception t) {
            LOG.error("Failed loading page", t);
            result.withError(ExtraStatusCodes.RUNTIME_EXCEPTION.toFetchError(t.toString()));
        }

        try {
            session.close();
            sessionRegistry.remove(session);
        } finally {
            span.finish();
        }

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
                    script = DbService.getInstance().getConfigAdapter().getBrowserScript(req);
                    scriptCache.put(scriptId, script);
                }
                scripts.add(script);
            }
            ListRequest req = ListRequest.newBuilder()
                    .addAllLabelSelector(browserConfig.getScriptSelectorList())
                    .build();
            for (BrowserScript script : DbService.getInstance().getConfigAdapter().listBrowserScripts(req).getValueList()) {
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
