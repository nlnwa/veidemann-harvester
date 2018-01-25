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

package no.nb.nna.veidemann.chrome.client.ws;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class CdpTest {

    /**
     * Test of ping method, of class ChromeDebugClient.
     */
    @Test
    @Ignore
    public void testPing() throws InterruptedException, URISyntaxException {
        System.out.println("ping");
        WebsocketClient instance = new WebsocketClient(null, new URI("ws://echo.websocket.org"));
        instance.ping();
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of sendMessage method, of class ChromeDebugClient.
     */
    @Test
    @Ignore
    public void testCall() throws InterruptedException, ExecutionException, URISyntaxException {
        System.out.println("sendMessage");
        String msg = "Hello World";
//        Cdp instance = new Cdp("ws://localhost:9222/devtools/page/cdb3c308-6abe-4159-b09e-4e464b499a92", null, true);
        Cdp instance = null;
        try {
            System.out.println(">>> " + instance.call("mm", null).get());
        } catch (Exception e) {
            System.out.println("E: " + e);
        }
        System.out.println(">>> " + instance.call("mm", null)
                .handle((r, t) -> {System.out.println("  E: " + t + ", R: " + r); return instance.call("mm", null)
                .handle((r2, t2) -> {System.out.println("  E: " + t2 + ", R: " + r2); return instance.call("mm", null);});})
                .handle((r, t) -> {System.out.println("  E: " + t + ", R: " + r); return instance.call("mm", null);})
                .get());
//        Thread.sleep(5000);
        instance.close("");
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}