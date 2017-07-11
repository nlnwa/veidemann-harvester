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

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.HttpHeaderNames;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.RuntimeDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.commons.util.ApiTools;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.Label;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class PageExecution implements BroproxHeaderConstants {

    private static final Logger LOG = LoggerFactory.getLogger(PageExecution.class);

    private final QueuedUri queuedUri;

    private final Session session;

    private final long timeout;

    private final long sleep;

    private final Map<String, Object> extraHeaders = new HashMap<>();

    private final String discoveryPath;

    private final DbAdapter db;

    public PageExecution(QueuedUri queuedUri, Session session, long timeout, DbAdapter db, long sleep) {
        this.queuedUri = queuedUri;
        this.session = session;
        this.timeout = timeout;
        this.sleep = sleep;
        this.db = db;

        discoveryPath = queuedUri.getDiscoveryPath();

        extraHeaders.put(EXECUTION_ID, queuedUri.getExecutionId());
        extraHeaders.put(DISCOVERY_PATH, discoveryPath);
        extraHeaders.put(HttpHeaderNames.REFERER.toString(), queuedUri.getReferrer());
    }

    public void navigatePage() {
        try {
            CompletableFuture<PageDomain.FrameStoppedLoading> loaded = session.page.onFrameStoppedLoading();

            session.page.onNavigationRequested(nr -> {
                LOG.debug("Navigation requested {}", nr.url);
                extraHeaders.put(DISCOVERY_PATH, discoveryPath + "E");
                session.network.setExtraHTTPHeaders(extraHeaders);
                session.page.processNavigation("Proceed", nr.navigationId);
            });

            session.page.onJavascriptDialogOpening(js -> {
                System.out.println("JS DIALOG: " + js.type + " :: " + js.message);
                boolean accept = false;
                if ("alert".equals(js.type)) {
                    accept = true;
                }
                session.page.handleJavaScriptDialog(accept, null);
            });

            session.network.setExtraHTTPHeaders(extraHeaders).get(timeout, MILLISECONDS);
            session.page.navigate(queuedUri.getUri()).get(timeout, MILLISECONDS);

            loaded.get(timeout, MILLISECONDS);
            // disable scrollbars
            session.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                    null, null, null, null, null, null, null, null)
                    .get(timeout, MILLISECONDS);

            // wait a little for any onload javascript to fire
            Thread.sleep(sleep);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    List<MessagesProto.Cookie> extractCookies() {
        try {
        return session.network.getCookies().get(timeout, MILLISECONDS).cookies.stream()
                .map(c -> {
                    return MessagesProto.Cookie.newBuilder()
                            .setName(c.name)
                            .setValue(c.value)
                            .setDomain(c.domain)
                            .setPath(c.path)
                            .setExpires(c.expires)
                            .setSize(c.size)
                            .setHttpOnly(c.httpOnly)
                            .setSecure(c.secure)
                            .setSession(c.session)
                            .setSameSite(c.sameSite)
                            .build();
                })
                .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<QueuedUri> extractOutlinks(List<BrowserScript> scripts) {
        Label outlinksLabel = ApiTools.buildLabel("type", "extract_outlinks");
        List<MessagesProto.Cookie> cookies = extractCookies();
        try {
            List<QueuedUri> outlinks = new ArrayList<>();
            for (BrowserScript script : scripts) {
                if (ApiTools.hasLabel(script.getMeta(), outlinksLabel)) {

                    RuntimeDomain.Evaluate ev = session.runtime
                            .evaluate(script.getScript(), null, null, null, null, Boolean.TRUE, null, null, null)
                            .get(timeout, MILLISECONDS);

                    if (ev.result.value != null) {
                        String resultString = ((String) ev.result.value).trim();
                        if (!resultString.isEmpty()) {
                            String[] links = resultString.split("\n+");
                            String path = discoveryPath + "L";
                            for (int i = 0; i < links.length; i++) {
                                outlinks.add(QueuedUri.newBuilder()
                                        .setExecutionId(queuedUri.getExecutionId())
                                        .setUri(links[i])
                                        .setReferrer(queuedUri.getUri())
                                        .setTimeStamp(ProtoUtils.odtToTs(OffsetDateTime.now()))
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

    public String getDocumentUrl() {
        try {
            RuntimeDomain.Evaluate ev = session.runtime
                    .evaluate("document.URL", null, null, null, null, null, null, null, null).get(timeout, MILLISECONDS);
            return (String) ev.result.value;
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void scrollToTop() {
        try {
            RuntimeDomain.Evaluate ev = session.runtime
                    .evaluate("window.scrollTo(0, 0);", null, null, null, null, null, null, null, null)
                    .get(timeout, MILLISECONDS);
            System.out.println("Scroll to top: " + ev);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void saveScreenshot() {
        try {
            PageDomain.CaptureScreenshot screenshot = session.page.captureScreenshot().get(timeout, MILLISECONDS);
            byte[] img = Base64.getDecoder().decode(screenshot.data);

            db.addScreenshot(Screenshot.newBuilder()
                    .setImg(ByteString.copyFrom(img))
                    .setExecutionId(queuedUri.getExecutionId())
                    .setUri(queuedUri.getUri())
                    .build());
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
                    .get(timeout, MILLISECONDS);
            System.out.println("Document URL: " + ev.result.value);
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
}
