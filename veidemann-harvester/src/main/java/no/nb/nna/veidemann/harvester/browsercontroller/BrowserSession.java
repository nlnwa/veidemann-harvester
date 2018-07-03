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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.opentracing.BaseSpan;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.chrome.client.BrowserClient;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.DebuggerDomain;
import no.nb.nna.veidemann.chrome.client.NetworkDomain.CookieParam;
import no.nb.nna.veidemann.chrome.client.NetworkDomain.RequestPattern;
import no.nb.nna.veidemann.chrome.client.PageDomain;
import no.nb.nna.veidemann.chrome.client.PageSession;
import no.nb.nna.veidemann.chrome.client.RuntimeDomain;
import no.nb.nna.veidemann.chrome.client.SessionClosedException;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 *
 */
public class BrowserSession implements AutoCloseable, VeidemannHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserSession.class);

    final int proxyId;

    final QueuedUri queuedUri;

    final BrowserClient browser;

    final PageSession session;

    final long protocolTimeout;

    final Map<String, List<DebuggerDomain.Location>> breakpoints = new HashMap<>();

    final UriRequestRegistry uriRequests;

    private final CrawlLogRegistry crawlLogs;

    volatile boolean closed = false;

    public BrowserSession(int proxyId, BrowserClient browser, BrowserConfig browserConfig, QueuedUri queuedUri, BaseSpan span)
            throws IOException, ExecutionException, TimeoutException {

        this.proxyId = proxyId;
        this.queuedUri = Objects.requireNonNull(queuedUri);

        this.browser = browser;
        // Ensure that we at least wait a second even if the configuration says less.
        long maxIdleTime = Math.max(browserConfig.getSleepAfterPageloadMs(), 1000);
        crawlLogs = new CrawlLogRegistry(this, browserConfig.getPageLoadTimeoutMs(), maxIdleTime);
        uriRequests = new UriRequestRegistry(crawlLogs, queuedUri, span);
        protocolTimeout = browserConfig.getPageLoadTimeoutMs();

        session = browser.newPage(browserConfig.getWindowWidth(), browserConfig.getWindowHeight());

        LOG.debug("Browser page created");

        String userAgent = browserConfig.getUserAgent();

        // Set userAgent to config value if it exist, otherwise just replace HeadlessChrome with ChromeVersion
        // like the real browser.
        if (userAgent.isEmpty()) {
            userAgent = (String) session.runtime().evaluate("navigator.userAgent;").run().result().value();
            userAgent = userAgent.replace("HeadlessChrome", session.version());
        }
        session.network().setUserAgentOverride(userAgent).run();

//        session.debugger().enable().run();
        session.network().enable().run();
        session.page().enable().run();
        session.runtime().enable().run();
//        session.security().enable().run();

//        session.debugger().setBreakpointsActive(true).run();
//        session.debugger().setAsyncCallStackDepth(0).run();
        session.security().setIgnoreCertificateErrors(true).run();
        session.network().setCacheDisabled(true).run();

        // Request patterns for enabling interception on requests and responses
        RequestPattern rp1 = new RequestPattern();
        RequestPattern rp2 = new RequestPattern();
        rp2.withInterceptionStage("HeadersReceived");
        List<RequestPattern> requestPatterns = ImmutableList.of(rp1, rp2);

        // set up listeners
        session.network().onRequestWillBeSent(r -> {
            uriRequests.onRequestWillBeSent(r);
        });
        session.network().onLoadingFinished(f -> uriRequests.onLoadingFinished(f));
        session.network().onLoadingFailed(f -> uriRequests.onLoadingFailed(f));
        session.network().onResponseReceived(l -> uriRequests.onResponseReceived(l));
        session.network().onDataReceived(d -> uriRequests.onDataReceived(d));

        session.page().setDownloadBehavior("allow").withDownloadPath("/dev/null").run();

        LOG.debug("Browser session configured");
    }

    public String getJobExecutionId() {
        return queuedUri.getJobExecutionId();
    }

    public String getExecutionId() {
        return queuedUri.getExecutionId();
    }

    public int getProxyId() {
        return proxyId;
    }

    public long getProtocolTimeout() {
        return protocolTimeout;
    }

    public UriRequestRegistry getUriRequests() {
        return uriRequests;
    }

    public void setBreakpoints() throws TimeoutException, ExecutionException, ClientClosedException, SessionClosedException {
        // TODO: This should be part of configuration
//        SetBreakpointByUrl b = session.debugger().setBreakpointByUrl(1).withUrlRegex("https?://www.google-analytics.com/analytics.js").run();
//        breakpoints.put(b.breakpointId(), b.locations());
//        b = session.debugger().setBreakpointByUrl(1).withUrlRegex("https?://www.google-analytics.com/ga.js").run();
//        breakpoints.put(b.breakpointId(), b.locations());
//
//        //session.debugger.onBreakpointResolved(b -> breakpoints.put(b.breakpointId, b.location));
//        session.debugger().onPaused(p -> {
//            String scriptId = p.callFrames().get(0).location().scriptId();
//            LOG.info("Script paused: {}", scriptId);
//            try {
//                SetScriptSource scriptSource = session.debugger().setScriptSource(scriptId, "console.log(\"google analytics is no more!\");").run();
//                LOG.info("Inserted script: {}", scriptSource);
//            } catch (ClientClosedException | SessionClosedException | ExecutionException | TimeoutException e) {
//                LOG.error(e.getMessage(), e);
//            }
//            try {
//                session.debugger().resume().runAsync();
//            } catch (ClientClosedException | SessionClosedException e) {
//                LOG.error(e.getMessage(), e);
//            }
//            LOG.debug("Script resumed: " + scriptId);
//        });
    }

    public void setCookies() throws TimeoutException, ExecutionException, ClientClosedException, SessionClosedException {
        LOG.debug("Restoring {} browser cookies", queuedUri.getCookiesCount());
        if (queuedUri.getCookiesCount() > 0) {
            List<CookieParam> l = queuedUri.getCookiesList().stream()
                    .map(c -> {
                                CookieParam nc = new CookieParam(c.getName(), c.getValue())
                                        .withUrl(queuedUri.getUri())
                                        .withDomain(c.getDomain())
                                        .withPath(c.getPath())
                                        .withSecure(c.getSecure())
                                        .withHttpOnly(c.getHttpOnly())
                                        .withSameSite(c.getSameSite())
                                        .withExpires(c.getExpires());
                                return nc;
                            }
                    )
                    .collect(Collectors.toList());

            session.network().setCookies(l).run();
        }

        LOG.debug("Browser cookies restored");
    }

    public void loadPage() throws ClientClosedException, SessionClosedException {
        try {
            // TODO: Handling of dialogs should be configurable
            session.page().onJavascriptDialogOpening(js -> {
                LOG.debug("JS dialog: {} :: {}", js.type(), js.message());
                boolean accept = false;
                if ("alert".equals(js.type())) {
                    accept = true;
                }
                try {
                    session.page().handleJavaScriptDialog(accept);
                } catch (ClientClosedException | SessionClosedException e) {
                    LOG.error(e.getMessage(), e);
                }
            });

            session.network().setExtraHTTPHeaders(ImmutableMap.of(EXECUTION_ID, queuedUri.getExecutionId(), JOB_EXECUTION_ID, queuedUri.getJobExecutionId())).run();
            session.page().navigate(queuedUri.getUri()).withReferrer(queuedUri.getReferrer()).withTransitionType("link").run();
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public CrawlLogRegistry getCrawlLogs() {
        return crawlLogs;
    }

    public String getDocumentUrl() throws ClientClosedException, SessionClosedException {
        try {
            return (String) session.runtime()
                    .evaluate("document.URL")
                    .run().result().value();
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void scrollToTop() throws ClientClosedException, SessionClosedException {
        try {
            RuntimeDomain.EvaluateResponse ev = session.runtime()
                    .evaluate("window.scrollTo(0, 0);").run();
            LOG.debug("Scroll to top: {}", ev);
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean isPageRenderable() {
        if (uriRequests.getRootRequest() == null) {
            return false;
        }
        return uriRequests.getRootRequest().isRenderable();
    }

    public void saveScreenshot() throws ClientClosedException, SessionClosedException {
        try {
            PageDomain.CaptureScreenshotResponse screenshot = session.page().captureScreenshot().withFormat("png").run();
            byte[] img = Base64.getDecoder().decode(screenshot.data());

            DbHelper.getInstance().getDb().saveScreenshot(MessagesProto.Screenshot.newBuilder()
                    .setImg(ByteString.copyFrom(img))
                    .setExecutionId(getExecutionId())
                    .setUri(uriRequests.getRootRequest().getUrl())
                    .build());
        } catch (ExecutionException | TimeoutException | DbException ex) {
            throw new RuntimeException(ex);
        }
    }

    List<MessagesProto.Cookie> extractCookies() throws ClientClosedException, SessionClosedException {
        try {
            List<MessagesProto.Cookie> cookies = session.network().getAllCookies().run()
                    .cookies().stream()
                    .map(c -> {
                        MessagesProto.Cookie.Builder cb = MessagesProto.Cookie.newBuilder();
                        if (c.name() != null) {
                            cb.setName(c.name());
                        }
                        if (c.value() != null) {
                            cb.setValue(c.value());
                        }
                        if (c.domain() != null) {
                            cb.setDomain(c.domain());
                        }
                        if (c.path() != null) {
                            cb.setPath(c.path());
                        }
                        if (c.expires() != null) {
                            cb.setExpires(c.expires());
                        }
                        if (c.size() != null) {
                            cb.setSize(c.size());
                        }
                        if (c.httpOnly() != null) {
                            cb.setHttpOnly(c.httpOnly());
                        }
                        if (c.secure() != null) {
                            cb.setSecure(c.secure());
                        }
                        if (c.session() != null) {
                            cb.setSession(c.session());
                        }
                        if (c.sameSite() != null) {
                            cb.setSameSite(c.sameSite());
                        }
                        return cb.build();
                    })
                    .collect(Collectors.toList());
            LOG.debug("Extracted cookies: {}", cookies);
            return cookies;
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<MessagesProto.QueuedUri> extractOutlinks(List<ConfigProto.BrowserScript> scripts) throws ClientClosedException, SessionClosedException {
        ConfigProto.Label outlinksLabel = ApiTools.buildLabel("type", "extract_outlinks");
        List<MessagesProto.Cookie> cookies = extractCookies();
        try {
            List<MessagesProto.QueuedUri> outlinks = new ArrayList<>();
            for (ConfigProto.BrowserScript script : scripts) {
                if (ApiTools.hasLabel(script.getMeta(), outlinksLabel)) {
                    LOG.debug("Executing link extractor script '{}'", script.getMeta().getName());
                    RuntimeDomain.EvaluateResponse ev = session.runtime()
                            .evaluate(script.getScript()).withReturnByValue(Boolean.TRUE).run();

                    LOG.trace("Outlinks: {}", ev.result().value());
                    if (ev.result().value() != null) {
                        String resultString = ((String) ev.result().value()).trim();
                        if (!resultString.isEmpty()) {
                            String[] links = resultString.split("\n+");
                            String path = uriRequests.getRootRequest().getDiscoveryPath() + "L";
                            for (int i = 0; i < links.length; i++) {
                                if (!uriRequests.getInitialRequest().getUrl().equals(links[i])) {
                                    outlinks.add(MessagesProto.QueuedUri.newBuilder()
                                            .setJobExecutionId(getJobExecutionId())
                                            .setExecutionId(getExecutionId())
                                            .setUri(links[i])
                                            .setReferrer(uriRequests.getRootRequest().getUrl())
                                            .setDiscoveredTimeStamp(ProtoUtils.odtToTs(OffsetDateTime.now()))
                                            .setDiscoveryPath(path)
                                            .addAllCookies(cookies)
                                            .build());
                                }
                            }
                        }
                    }
                }
            }
            return outlinks;
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void runBehaviour() {
//            behavior_script = brozzler.behavior_script(
//                    page_url, behavior_parameters)
//            self.run_behavior(behavior_script, timeout=900)
    }

    public void tryLogin(String username, String password) throws ClientClosedException, SessionClosedException {
        try {
            RuntimeDomain.EvaluateResponse ev = session.runtime()
                    .evaluate("window.scrollTo(0, 0);").run();
        } catch (ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    //    def run_behavior(self, behavior_script, timeout=900):
//        self.send_to_chrome(
//                method='Runtime.evaluate', suppress_logging=True,
//                params={'expression': behavior_script})
//
//        start = time.time()
//        while True:
//            elapsed = time.time() - start
//            if elapsed > timeout:
//                logging.info(
//                        'behavior reached hard timeout after %.1fs', elapsed)
//                return
//
//            brozzler.sleep(7)
//
//            self.websock_thread.expect_result(self._command_id.peek())
//            msg_id = self.send_to_chrome(
//                     method='Runtime.evaluate', suppress_logging=True,
//                     params={'expression': 'umbraBehaviorFinished()'})
//            try:
//                self._wait_for(
//                        lambda: self.websock_thread.received_result(msg_id),
//                        timeout=5)
//                msg = self.websock_thread.pop_result(msg_id)
//                if (msg and 'result' in msg
//                        and not ('exceptionDetails' in msg['result'])
//                        and not ('wasThrown' in msg['result']
//                            and msg['result']['wasThrown'])
//                        and 'result' in msg['result']
//                        and type(msg['result']['result']['value']) == bool
//                        and msg['result']['result']['value']):
//                    self.logger.info('behavior decided it has finished')
//                    return
//            except BrowsingTimeout:
//                pass
//
//    def try_login(self, username, password, timeout=300):
//        try_login_js = brozzler.jinja2_environment().get_template(
//                'try-login.js.j2').render(
//                        username=username, password=password)
//
//        self.websock_thread.got_page_load_event = None
//        self.send_to_chrome(
//                method='Runtime.evaluate', suppress_logging=True,
//                params={'expression': try_login_js})
//
//        # wait for tryLogin to finish trying (should be very very quick)
//        start = time.time()
//        while True:
//            self.websock_thread.expect_result(self._command_id.peek())
//            msg_id = self.send_to_chrome(
//                method='Runtime.evaluate',
//                params={'expression': 'try { __brzl_tryLoginState } catch (e) { "maybe-submitted-form" }'})
//            try:
//                self._wait_for(
//                        lambda: self.websock_thread.received_result(msg_id),
//                        timeout=5)
//                msg = self.websock_thread.pop_result(msg_id)
//                if (msg and 'result' in msg
//                        and 'result' in msg['result']):
//                    result = msg['result']['result']['value']
//                    if result == 'login-form-not-found':
//                        # we're done
//                        return
//                    elif result in ('submitted-form', 'maybe-submitted-form'):
//                        # wait for page load event below
//                        self.logger.info(
//                                'submitted a login form, waiting for another '
//                                'page load event')
//                        break
//                    # else try again to get __brzl_tryLoginState
//
//            except BrowsingTimeout:
//                pass
//
//            if time.time() - start > 30:
//                raise BrowsingException(
//                        'timed out trying to check if tryLogin finished')
//
//        # if we get here, we submitted a form, now we wait for another page
//        # load event
//        self._wait_for(
//                lambda: self.websock_thread.got_page_load_event,
//                timeout=timeout)


    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        if (browser != null) {
            browser.close();
        }
        uriRequests.close();
    }

}
