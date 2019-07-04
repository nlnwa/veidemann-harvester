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
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollection;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.frontier.v1.PageLog;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocol;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.CollectionNameGenerator;
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

    private final Map<ConfigRef, ConfigObject> scriptCache = new HashMap<>();

    private final ContentWriterClient contentWriterClient;

    public BrowserController(final String browserWSEndpoint, final BrowserSessionRegistry sessionRegistry,
                             final ContentWriterClient contentWriterClient) {
        this.browserWSEndpoint = browserWSEndpoint;
        this.contentWriterClient = contentWriterClient;

        chromeDebugProtocolConfig = new ChromeDebugProtocolConfig()
                .withTracer(GlobalTracer.get())
                .withProtocolTimeoutMs(30000);

        this.chrome = new ChromeDebugProtocol();
        this.sessionRegistry = sessionRegistry;
    }

    public RenderResult render(ProxySession proxySession, QueuedUri queuedUri, ConfigObject crawlConfig) throws IOException, DbException {
        LOG.trace("Connecting to browser with: " + proxySession.getBrowserWsEndpoint());
        ChromeDebugProtocolConfig protocolConfig = chromeDebugProtocolConfig.withBrowserWSEndpoint(proxySession.getBrowserWsEndpoint());

        return render(proxySession.getProxyId(), protocolConfig, queuedUri, crawlConfig);
    }

    public RenderResult render(int proxyId, ChromeDebugProtocolConfig protocolConfig,
                               QueuedUri queuedUri, ConfigObject crawlConfig) throws IOException {

        Span span = GlobalTracer.get()
                .buildSpan("render")
                .withTag(Tags.COMPONENT.getKey(), "BrowserController")
                .withTag("executionId", queuedUri.getExecutionId())
                .withTag("uri", queuedUri.getUri())
                .startManual();

        RenderResult result = new RenderResult();

        MDC.put("eid", queuedUri.getExecutionId());
        MDC.put("uri", queuedUri.getUri());

        ConfigObject browserConfig = null;
        BrowserSession session = null;
        try {
            browserConfig = DbService.getInstance().getConfigAdapter()
                    .getConfigObject(crawlConfig.getCrawlConfig().getBrowserConfigRef());
            ConfigObject politenessConfig = DbService.getInstance().getConfigAdapter()
                    .getConfigObject(crawlConfig.getCrawlConfig().getPolitenessRef());
            session = new BrowserSession(proxyId, chrome.connect(protocolConfig), crawlConfig,
                    browserConfig, politenessConfig, getScripts(browserConfig), queuedUri, span);
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
                if (crawlConfig.getCrawlConfig().getExtra().getCreateScreenshot()) {
                    LOG.debug("Save screenshot");
                    session.saveScreenshot(contentWriterClient);
                }

                LOG.debug("Extract outlinks");
                try {
                    result.withOutlinks(session.extractOutlinks());
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
                            .setReferrer(session.getUriRequests().getInitialRequest().getReferrer())
                            .setCollectionFinalName(session.getUriRequests().getInitialRequest().getCrawlLog().getCollectionFinalName());
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

    private List<ConfigObject> getScripts(ConfigObject browserConfig) {
        List<ConfigObject> scripts = new ArrayList<>();
        try {
            for (ConfigRef scriptRef : browserConfig.getBrowserConfig().getScriptRefList()) {
                ConfigObject script = scriptCache.get(scriptRef);
                if (script == null) {
                    script = DbService.getInstance().getConfigAdapter().getConfigObject(scriptRef);
                    scriptCache.put(scriptRef, script);
                }
                scripts.add(script);
            }
            ListRequest req = ListRequest.newBuilder()
                    .setKind(Kind.browserScript)
                    .addAllLabelSelector(browserConfig.getBrowserConfig().getScriptSelectorList())
                    .build();
            try (ChangeFeed<ConfigObject> r = DbService.getInstance().getConfigAdapter().listConfigObjects(req)) {
                r.stream().forEach(script -> {
                    ConfigRef scriptRef = ConfigRef.newBuilder().setKind(Kind.browserScript).setId(script.getId()).build();
                    if (!scriptCache.containsKey(scriptRef)) {
                        scriptCache.put(scriptRef, script);
                    }
                    scripts.add(script);
                });
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
