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

import com.google.common.net.InetAddresses;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ContentWriterProto;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteResponseMeta;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.HarvesterProto;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.proxy.InMemoryAlreadyCrawledCache;
import no.nb.nna.veidemann.harvester.proxy.RecordingProxy;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littleshoot.proxy.HostResolver;
import org.mockito.invocation.InvocationOnMock;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class BrowserControllerIT {

    static String browserHost;

    static int browserPort;

    static int proxyPort;

    static String testSitesHttpHost;

    static int testSitesHttpPort;

    static String testSitesDnsHost;

    static int testSitesDnsPort;

    @BeforeClass
    public static void init() {
        browserHost = System.getProperty("browser.host");
        browserPort = Integer.parseInt(System.getProperty("browser.port"));
        proxyPort = Integer.parseInt(System.getProperty("proxy.port"));
        testSitesHttpHost = System.getProperty("testsites.http.host");
        testSitesHttpPort = Integer.parseInt(System.getProperty("testsites.http.port"));
        testSitesDnsHost = System.getProperty("testsites.dns.host");
        testSitesDnsPort = Integer.parseInt(System.getProperty("testsites.dns.port"));
    }

    /**
     * Test of render method, of class BrowserController.
     */
    @Test
    public void testRender() {
        try {
            BrowserSessionRegistry sessionRegistry = new BrowserSessionRegistry();

            ContentWriterClient contentWriterClient = mock(ContentWriterClient.class);
            ContentWriterClient.ContentWriterSession contentWriterSession = mock(ContentWriterClient.ContentWriterSession.class);
            when(contentWriterClient.createSession()).thenReturn(contentWriterSession);
            ContentWriterProto.WriteResponseMeta.Builder response = ContentWriterProto.WriteResponseMeta.newBuilder()
                    .putRecordMeta(0, WriteResponseMeta.RecordMeta.newBuilder()
                            .setRecordNum(0)
                            .setWarcId("WARC_ID_REQUEST").build())
                    .putRecordMeta(1, WriteResponseMeta.RecordMeta.newBuilder()
                            .setRecordNum(0)
                            .setWarcId("WARC_ID_RESPONSE").build());

            when(contentWriterSession.finish()).thenReturn(response.build());

            DbAdapter db = getDbMock();

            MessagesProto.QueuedUri queuedUri = MessagesProto.QueuedUri.newBuilder()
                    .setUri("http://a1.com")
                    .setExecutionId("testId")
                    .setDiscoveryPath("L")
                    .setReferrer("http://example.org/")
                    .build();

            ConfigProto.CrawlConfig config = getDefaultConfig();

            File tmpDir = Files.createDirectories(Paths.get("target", "it-workdir")).toFile();
            tmpDir.deleteOnExit();

            Thread.sleep(1000);

            try (RecordingProxy proxy = new RecordingProxy(tmpDir, proxyPort, db, contentWriterClient,
                    new TestHostResolver(), sessionRegistry, new InMemoryAlreadyCrawledCache());

                 BrowserController controller = new BrowserController(browserHost, browserPort, db,
                         sessionRegistry);) {

                HarvesterProto.HarvestPageReply result = controller.render(queuedUri, config);

//                System.out.println("=========*\n" + result);
                // TODO review the generated test code and remove the default call to fail.
//            fail("The test case is a prototype.");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Exception was not expected", ex);
        }
    }

    private ConfigProto.CrawlConfig getDefaultConfig() {
        ConfigProto.BrowserConfig browserConfig = ConfigProto.BrowserConfig.newBuilder()
                .setMeta(ApiTools.buildMeta("Default", "Default browser configuration"))
                .setUserAgent("veidemann/1.0")
                .setWindowHeight(900)
                .setWindowWidth(900)
                .setPageLoadTimeoutMs(20000)
                .setSleepAfterPageloadMs(500)
                .setScriptSelector(ConfigProto.Selector.newBuilder().addLabel(ApiTools.buildLabel("scope", "default")))
                .build();

        ConfigProto.PolitenessConfig politenessConfig = ConfigProto.PolitenessConfig.newBuilder()
                .setMeta(ApiTools.buildMeta("Default", "Default politeness configuration"))
                .setRobotsPolicy(ConfigProto.PolitenessConfig.RobotsPolicy.OBEY_ROBOTS)
                .setMinimumRobotsValidityDurationS(86400)
                .setMinTimeBetweenPageLoadMs(1000)
                .build();

        ConfigProto.CrawlConfig config = ConfigProto.CrawlConfig.newBuilder()
                .setMeta(ApiTools.buildMeta("Default", "Default crawl configuration"))
                .setBrowserConfig(browserConfig)
                .setPoliteness(politenessConfig)
                .setExtra(ConfigProto.ExtraConfig.newBuilder().setCreateSnapshot(true).setExtractText(true))
                .build();

        return config;
    }

    private DbAdapter getDbMock() {
        DbAdapter db = mock(DbAdapter.class);
        when(db.hasCrawledContent(any())).thenReturn(Optional.empty());
        when(db.saveCrawlLog(any())).thenAnswer((InvocationOnMock i) -> {
            CrawlLog cl = i.getArgument(0);
            cl = cl.toBuilder().setWarcId("WARC_ID").build();
//            System.out.println("CL: " + cl);
            return cl;
        });
        when(db.savePageLog(any(PageLog.class))).then(a -> {
            PageLog o = a.getArgument(0);
            System.out.println("PageLOG::::");
            System.out.println(o);
            System.out.println("::::PageLOG");
            return o;
        });
        when(db.listBrowserScripts(any())).thenReturn(ControllerProto.BrowserScriptListReply.newBuilder()
                .addValue(ConfigProto.BrowserScript.newBuilder()
                        .setMeta(ApiTools.buildMeta("extract-outlinks.js", "", ApiTools
                                .buildLabel("type", "extract_outlinks")))
                        .setScript("var __brzl_framesDone = new Set();\n"
                                + "var __brzl_compileOutlinks = function(frame) {\n"
                                + "  __brzl_framesDone.add(frame);\n"
                                + "  if (frame && frame.document) {\n"
                                + "    var outlinks = Array.prototype.slice.call(frame.document.querySelectorAll('a[href]'));\n"
                                + "    for (var i = 0; i < frame.frames.length; i++) {\n"
                                + "      if (frame.frames[i] && !__brzl_framesDone.has(frame.frames[i])) {\n"
                                + "        outlinks = outlinks.concat(__brzl_compileOutlinks(frame.frames[i]));\n"
                                + "      }\n"
                                + "    }\n"
                                + "  }\n"
                                + "  return outlinks;\n"
                                + "}\n"
                                + "__brzl_compileOutlinks(window).join('\\n');\n")
                        .build())
                .build());
        return db;
    }

    private class TestHostResolver implements HostResolver {

        @Override
        public InetSocketAddress resolve(String host, int port) throws UnknownHostException {
            InetSocketAddress resolvedAddress = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), testSitesHttpPort);
//            InetSocketAddress resolvedAddress = new InetSocketAddress(InetAddress.getByName(host), port);
            System.out.println("H: " + testSitesHttpHost);
            System.out.println("H: " + host + ":" + port + " => " + resolvedAddress);
            return resolvedAddress;
        }

    }
}
