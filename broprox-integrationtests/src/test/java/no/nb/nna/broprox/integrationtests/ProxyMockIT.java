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
package no.nb.nna.broprox.integrationtests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.gson.Gson;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.Session;
import no.nb.nna.broprox.chrome.client.ws.CompleteMany;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.harvester.BroproxHeaderConstants;
import no.nb.nna.broprox.harvester.browsercontroller.PageExecution;
import org.junit.Ignore;
import org.junit.Test;
//import static org.junit.Assert.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class ProxyMockIT implements BroproxHeaderConstants {

    private long timeout = 1000;

    @Test
    @Ignore
    public void testSomeMethod() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        ProxyMock pm = new ProxyMock(9211);
        System.out.println("THIS HOST: " + System.getProperty("this.host"));
        String chromeHost = System.getProperty("chrome-debug.host");
        int chromePort = Integer.parseInt(System.getProperty("chrome-debug.port"));
        System.out.println("Chrome host: " + chromeHost);
        System.out.println("Chrome port: " + chromePort);
        ChromeDebugProtocol chrome = new ChromeDebugProtocol(chromeHost, chromePort);

        QueuedUri qUri = QueuedUri.newBuilder()
                .addExecutionIds(QueuedUri.IdSeq.newBuilder().setId("foo").setSeq(1))
                .addExecutionIds(QueuedUri.IdSeq.newBuilder().setId("bar").setSeq(2))
                .build();

//        List<String> l = new ArrayList<>();
//        l.add("foo");
//        l.add("bar");
        Gson gson = new Gson();
        try (Session session = chrome.newSession(900, 900)) {
            new CompleteMany(
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
                    session.debugger.setBreakpointByUrl(1, null, "https?://www.google-analytics.com/ga.js", null, null),
                    session.network.setExtraHTTPHeaders(Collections.singletonMap(ALL_EXECUTION_IDS,
                            ProtoUtils.protoListToJson(qUri.getExecutionIdsList()))),
                    session.page.setControlNavigations(Boolean.TRUE)
            ).get(timeout, MILLISECONDS);

            session.debugger.onPaused(p -> {
                String scriptId = p.callFrames.get(0).location.scriptId;
                session.debugger.setScriptSource(scriptId, "console.log(\"google analytics is no more!\");", null);
                session.debugger.resume();
            });

            navigatePage(session, "http://test.foo");
            navigatePage(session, "http://test2.foo");

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : session.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
        }

//        Thread.sleep(30000);
        fail("The test case is a prototype.");
    }

    public void navigatePage(Session session, String url) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<PageDomain.FrameStoppedLoading> loaded = session.page.onFrameStoppedLoading();

        String discoveryPath = "";
        session.page.onNavigationRequested(nr -> {
            session.network.setExtraHTTPHeaders(Collections.singletonMap("Discovery-Path", discoveryPath + "E"));
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

        System.out.println("NAV");
        System.out.println(session.page.navigate(url).get(timeout, MILLISECONDS));

        loaded.get(timeout, MILLISECONDS);
        // disable scrollbars
        session.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                null, null, null, null, null, null, null, null)
                .get(timeout, MILLISECONDS);

        // wait a little for any onload javascript to fire
//            Thread.sleep(500);
    }

}
