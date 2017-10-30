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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.opentracing.BaseSpan;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.DebuggerDomain;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.RuntimeDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.commons.util.ApiTools;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.MessagesProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class BrowserSession implements AutoCloseable, BroproxHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserSession.class);

    final String executionId;

    final Session session;

    final long protocolTimeout;

    final long sleepAfterPageLoad;

    final Map<String, List<DebuggerDomain.Location>> breakpoints = new HashMap<>();

    final UriRequestRegistry uriRequests;

    String discoveryPath;

    String referrer;

    // TODO: Should be configurable
    boolean followRedirects = true;

    public BrowserSession(ChromeDebugProtocol chrome, ConfigProto.CrawlConfig config, String executionId, BaseSpan span) {
        uriRequests = new UriRequestRegistry(span);
        this.executionId = Objects.requireNonNull(executionId);
        protocolTimeout = config.getBrowserConfig().getPageLoadTimeoutMs();
        sleepAfterPageLoad = config.getBrowserConfig().getSleepAfterPageloadMs();

        try {
            session = chrome.newSession(
                    config.getBrowserConfig().getWindowWidth(),
                    config.getBrowserConfig().getWindowHeight());

            LOG.debug("Browser session created");

            String userAgent = config.getBrowserConfig().getUserAgent();

            // Set userAgent to config value if it exist, otherwise just replace HeadlessChrome with ChromeVersion
            // like the real browser.
            CompletableFuture<Void> setUserAgent;
            if (userAgent.isEmpty()) {
                setUserAgent = session.runtime
                        .evaluate("navigator.userAgent;", null, false, false, null, false, false, false, false)
                        .thenAccept(e -> {
                            session.network.setUserAgentOverride(((String) e.result.value)
                                    .replace("HeadlessChrome", session.version()));
                        });
            } else {
                setUserAgent = session.network.setUserAgentOverride(userAgent);
            }

            CompletableFuture.allOf(
                    session.debugger.enable(),
                    session.page.enable(),
                    session.runtime.enable(),
                    session.network.enable(null, null),
                    session.network.setCacheDisabled(true),
                    setUserAgent,
                    session.page.setControlNavigations(Boolean.TRUE)
            ).get(config.getBrowserConfig().getPageLoadTimeoutMs(), MILLISECONDS);

            // set up listeners
            session.page.onNavigationRequested(nr -> this.onNavigationRequested(nr));
            session.network.onRequestWillBeSent(r -> uriRequests.onRequestWillBeSent(r, discoveryPath));
            session.network.onLoadingFailed(f -> uriRequests.onLoadingFailed(f));
            session.network.onResponseReceived(l -> uriRequests.onResponseReceived(l));

            LOG.debug("Browser session configured");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        } catch (TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getReferrer() {
        return referrer;
    }

    public long getProtocolTimeout() {
        return protocolTimeout;
    }

    public UriRequestRegistry getUriRequests() {
        return uriRequests;
    }

    public void setBreakpoints() throws TimeoutException, ExecutionException, InterruptedException {
        // TODO: This should be part of configuration
        CompletableFuture.allOf(
                session.debugger
                        .setBreakpointByUrl(1, null, "https?://www.google-analytics.com/analytics.js", null, null)
                        .thenAccept(b -> breakpoints.put(b.breakpointId, b.locations)),
                session.debugger
                        .setBreakpointByUrl(1, null, "https?://www.google-analytics.com/ga.js", null, null)
                        .thenAccept(b -> breakpoints.put(b.breakpointId, b.locations))
        ).get(protocolTimeout, MILLISECONDS);

        //session.debugger.onBreakpointResolved(b -> breakpoints.put(b.breakpointId, b.location));
        session.debugger.onPaused(p -> {
            String scriptId = p.callFrames.get(0).location.scriptId;
            LOG.debug("Script paused: " + scriptId);
            session.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
            session.debugger.resume();
            LOG.debug("Script resumed: " + scriptId);
        });
    }

    public void setCookies(MessagesProto.QueuedUri queuedUri) throws TimeoutException, ExecutionException, InterruptedException {
        CompletableFuture.allOf(queuedUri.getCookiesList().stream()
                .map(c -> session.network
                .setCookie(queuedUri.getUri(), c.getName(), c.getValue(), c.getDomain(),
                        c.getPath(), c.getSecure(), c.getHttpOnly(), c.getSameSite(), c.getExpires()))
                .collect(Collectors.toList()).toArray(new CompletableFuture[]{}))
                .get(protocolTimeout, MILLISECONDS);

        LOG.debug("Browser cookies initialized");
    }

    public void loadPage(MessagesProto.QueuedUri queuedUri) {
        discoveryPath = queuedUri.getDiscoveryPath();
        referrer = queuedUri.getReferrer();

        try {
            CompletableFuture<PageDomain.FrameStoppedLoading> loaded = session.page.onFrameStoppedLoading();

            // TODO: Handling of dialogs should be configurable
            session.page.onJavascriptDialogOpening(js -> {
                LOG.debug("JS dialog: {} :: {}", js.type, js.message);
                boolean accept = false;
                if ("alert".equals(js.type)) {
                    accept = true;
                }
                session.page.handleJavaScriptDialog(accept, null);
            });

            session.network.setExtraHTTPHeaders(ImmutableMap.of(EXECUTION_ID, queuedUri.getExecutionId()))
                    .get(protocolTimeout, MILLISECONDS);
            session.page.navigate(queuedUri.getUri()).get(protocolTimeout, MILLISECONDS);

            loaded.get(protocolTimeout, MILLISECONDS);

            // wait a little for any onload javascript to fire
            Thread.sleep(sleepAfterPageLoad);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    void onNavigationRequested(PageDomain.NavigationRequested nr) {
        if (!nr.isRedirect || followRedirects) {
            session.page.processNavigation("Proceed", nr.navigationId);
        } else {
            session.page.processNavigation("CancelAndIgnore", nr.navigationId);
        }
    }

    public String getDocumentUrl() {
        try {
            RuntimeDomain.Evaluate ev = session.runtime
                    .evaluate("document.URL", null, null, null, null, null, null, null, null)
                    .get(protocolTimeout, MILLISECONDS);
            return (String) ev.result.value;
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void scrollToTop() {
        try {
            RuntimeDomain.Evaluate ev = session.runtime
                    .evaluate("window.scrollTo(0, 0);", null, null, null, null, null, null, null, null)
                    .get(protocolTimeout, MILLISECONDS);
            LOG.debug("Scroll to top: {}", ev);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean isPageRenderable() {
        return uriRequests.getRootRequest().isRenderable();
    }

    public void saveScreenshot(DbAdapter db) {
        try {
            PageDomain.CaptureScreenshot screenshot = session.page.captureScreenshot()
                    .get(protocolTimeout, MILLISECONDS);
            byte[] img = Base64.getDecoder().decode(screenshot.data);

            db.saveScreenshot(MessagesProto.Screenshot.newBuilder()
                    .setImg(ByteString.copyFrom(img))
                    .setExecutionId(executionId)
                    .setUri(uriRequests.getRootRequest().getUrl())
                    .build());
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    List<MessagesProto.Cookie> extractCookies() {
        try {
            return session.network.getCookies().get(protocolTimeout, MILLISECONDS).cookies.stream()
                    .map(c -> {
                        MessagesProto.Cookie.Builder cb = MessagesProto.Cookie.newBuilder();
                        if (c.name != null) {
                            cb.setName(c.name);
                        }
                        if (c.value != null) {
                            cb.setValue(c.value);
                        }
                        if (c.domain != null) {
                            cb.setDomain(c.domain);
                        }
                        if (c.path != null) {
                            cb.setPath(c.path);
                        }
                        if (c.expires != null) {
                            cb.setExpires(c.expires);
                        }
                        if (c.size != null) {
                            cb.setSize(c.size);
                        }
                        if (c.httpOnly != null) {
                            cb.setHttpOnly(c.httpOnly);
                        }
                        if (c.secure != null) {
                            cb.setSecure(c.secure);
                        }
                        if (c.session != null) {
                            cb.setSession(c.session);
                        }
                        if (c.sameSite != null) {
                            cb.setSameSite(c.sameSite);
                        }
                        return cb.build();
                    })
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<MessagesProto.QueuedUri> extractOutlinks(List<ConfigProto.BrowserScript> scripts) {
        ConfigProto.Label outlinksLabel = ApiTools.buildLabel("type", "extract_outlinks");
        List<MessagesProto.Cookie> cookies = extractCookies();
        try {
            List<MessagesProto.QueuedUri> outlinks = new ArrayList<>();
            for (ConfigProto.BrowserScript script : scripts) {
                if (ApiTools.hasLabel(script.getMeta(), outlinksLabel)) {
                    LOG.debug("Executing link extractor script '{}'", script.getMeta().getName());
                    RuntimeDomain.Evaluate ev = session.runtime
                            .evaluate(script.getScript(), null, null, null, null, Boolean.TRUE, null, null, null)
                            .get(protocolTimeout, MILLISECONDS);

                    LOG.trace("Outlinks: {}", ev.result.value);
                    if (ev.result.value != null) {
                        String resultString = ((String) ev.result.value).trim();
                        if (!resultString.isEmpty()) {
                            String[] links = resultString.split("\n+");
                            String path = uriRequests.getRootRequest().getDiscoveryPath() + "L";
                            for (int i = 0; i < links.length; i++) {
                                outlinks.add(MessagesProto.QueuedUri.newBuilder()
                                        .setExecutionId(executionId)
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
            return outlinks;
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void runBehaviour() {
//            behavior_script = brozzler.behavior_script(
//                    page_url, behavior_parameters)
//            self.run_behavior(behavior_script, timeout=900)
    }

    public void tryLogin(String username, String password) {
        try {
            RuntimeDomain.Evaluate ev = session.runtime
                    .evaluate("window.scrollTo(0, 0);", null, null, null, null, null, null, null, null)
                    .get(protocolTimeout, MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
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
    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
        uriRequests.close();
    }

}
