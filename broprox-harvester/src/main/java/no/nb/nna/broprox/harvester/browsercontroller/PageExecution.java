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
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.RuntimeDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.QueuedUri;
import no.nb.nna.broprox.db.model.Screenshot;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class PageExecution {

    private final QueuedUri queuedUri;

    private final Session session;

    private final long timeout;

    private final String discoveryPath;

    public PageExecution(QueuedUri queuedUri, Session session, long timeout) {
        this.queuedUri = queuedUri;
        this.session = session;
        this.timeout = timeout;

        String dp = queuedUri.getDiscoveryPath();
        if (dp == null) {
            discoveryPath = "";
        } else {
            discoveryPath = dp;
        }
    }

    public void navigatePage() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("=====================================");

        CompletableFuture<PageDomain.LoadEventFired> loaded = session.page.onLoadEventFired();

        session.page.onNavigationRequested(nr -> {
            System.out.println("NAV REQUESTED " + nr);
//                try {
            session.network.setExtraHTTPHeaders(Collections.singletonMap("Discovery-Path", discoveryPath + "E"));
//                } catch (InterruptedException | ExecutionException ex) {
//                    throw new RuntimeException(ex);
//                }
//                tab.page.setControlNavigations(Boolean.FALSE);
            session.page.processNavigation("Proceed", nr.navigationId);
        });

//            tab.page.onNavigationRequested(nr -> {
//                System.out.println("NAV REQUESTED");
//                tab.page.processNavigation(nr.url, nr.navigationId);
//            });
        session.page.navigate(queuedUri.getUri()).get(timeout, MILLISECONDS);

        loaded.get(timeout, MILLISECONDS);
        // disable scrollbars
        session.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                null, null, null, null, null, null, null, null)
                .get(timeout, MILLISECONDS);

        // wait a little for any onload javascript to fire
//            Thread.sleep(sleep);
    }

    public void extractOutlinks(DbAdapter db, String script) throws InterruptedException, ExecutionException, TimeoutException {
        RuntimeDomain.Evaluate ev = session.runtime
                .evaluate(script, null, null, null, null, Boolean.TRUE, null, null, null).get(timeout, MILLISECONDS);
        String[] links = ((String) ev.result.value).split("\n");
        String path = discoveryPath + "L";
        for (String l : links) {
            QueuedUri uri = DbObjectFactory.create(QueuedUri.class)
                    .withUri(l)
                    .withReferrer(queuedUri.getUri())
                    .withTimeStamp(OffsetDateTime.now())
                    .withDiscoveryPath(path);
            db.addQueuedUri(uri);
        }
    }

    public void getDocumentUrl() throws InterruptedException, ExecutionException, TimeoutException {
        RuntimeDomain.Evaluate ev = session.runtime
                .evaluate("document.URL", null, null, null, null, null, null, null, null).get(timeout, MILLISECONDS);
        System.out.println("Document URL: " + ev.result.value);
    }

    public void scrollToTop() throws InterruptedException, ExecutionException, TimeoutException {
        RuntimeDomain.Evaluate ev = session.runtime
                .evaluate("window.scrollTo(0, 0);", null, null, null, null, null, null, null, null)
                .get(timeout, MILLISECONDS);
        System.out.println("Scroll to top: " + ev);
    }

    public void saveScreenshot(DbAdapter db) throws InterruptedException, ExecutionException, TimeoutException {
        session.page.captureScreenshot().thenAccept(s -> {
            byte[] img = Base64.getDecoder().decode(s.data);
            db.addScreenshot(DbObjectFactory.create(Screenshot.class)
                    .withImg(img)
                    .withExecutionId(queuedUri.getExecutionId())
                    .withUri(queuedUri.getUri()));
        });
    }

    public void runBehaviour() throws InterruptedException, ExecutionException, TimeoutException {
//            behavior_script = brozzler.behavior_script(
//                    page_url, behavior_parameters)
//            self.run_behavior(behavior_script, timeout=900)
    }

    public void tryLogin(String username, String password) throws InterruptedException, ExecutionException, TimeoutException {
        RuntimeDomain.Evaluate ev = session.runtime
                .evaluate("window.scrollTo(0, 0);", null, null, null, null, null, null, null, null)
                .get(timeout, MILLISECONDS);
        System.out.println("Document URL: " + ev.result.value);
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
