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
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ContentWriterProto;
import no.nb.nna.veidemann.api.ContentWriterProto.RecordType;
import no.nb.nna.veidemann.api.ContentWriterProto.WriteResponseMeta;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import no.nb.nna.veidemann.harvester.proxy.RecordingProxy;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.littleshoot.proxy.HostResolver;
import org.mockito.invocation.InvocationOnMock;
import org.netpreserve.commons.uri.UriConfigs;

import java.io.File;
import java.net.InetAddress;
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
@Ignore
public class BrowserControllerIT {

    static String browserHost;

    static int browserPort;

    static String proxyIp;

    static int proxyPort;

    static String cacheHost;

    static int cachePort;

    static String testSitesHttpHost;

    static int testSitesHttpPort;

    static String testSitesDnsHost;

    static int testSitesDnsPort;

    @BeforeClass
    public static void init() {
        browserHost = System.getProperty("browser.host");
        browserPort = Integer.parseInt(System.getProperty("browser.port"));
        proxyIp = System.getProperty("proxy.host");
        proxyPort = Integer.parseInt(System.getProperty("proxy.port"));
        cacheHost = System.getProperty("cache.host");
        cachePort = Integer.parseInt(System.getProperty("cache.port"));
//        proxyPort = 41355;
        testSitesHttpHost = System.getProperty("testsites.http.host");
        testSitesHttpPort = Integer.parseInt(System.getProperty("testsites.http.port"));
        testSitesDnsHost = System.getProperty("testsites.dns.host");
        testSitesDnsPort = Integer.parseInt(System.getProperty("testsites.dns.port"));

        System.out.println("BROWSER: " + browserHost + ":" + browserPort);
        System.out.println("  CACHE: " + cacheHost + ":" + cachePort);
        System.out.println("  PROXY: " + proxyIp + ":" + proxyPort);
        System.out.println("  SITES: " + testSitesHttpHost + ":" + testSitesHttpPort);
        System.out.println("    DNS: " + testSitesDnsHost + ":" + testSitesDnsPort);
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
                            .setWarcId("WARC_ID_REQUEST")
                            .setType(RecordType.REQUEST)
                            .build())
                    .putRecordMeta(1, WriteResponseMeta.RecordMeta.newBuilder()
                            .setRecordNum(0)
                            .setWarcId("WARC_ID_RESPONSE")
                            .setType(RecordType.RESPONSE)
                            .build());

            when(contentWriterSession.finish()).thenReturn(response.build());
            when(contentWriterSession.sendHeader(any())).thenReturn(contentWriterSession);
            when(contentWriterSession.sendMetadata(any())).thenReturn(contentWriterSession);
            when(contentWriterSession.sendPayload(any())).thenReturn(contentWriterSession);

            DbAdapter db = getDbMock();

            MessagesProto.QueuedUri queuedUri = MessagesProto.QueuedUri.newBuilder()
                    .setUri("http://a1.com")
//                    .setUri("https://www.nb.no")
//                    .setUri("http://example.org")
//                    .setUri("http://www.nb.no/sbfil/tekst/Bokselskap_HTML.zip")
//                    .setUri("https://www.nb.no/Fakta/Om-NB/Ledige-stillinger")
//                    .setUri("http://www.nb.no/nbsok/search?page=0&menuOpen=false&instant=true&action=search&currentHit=0&currentSesamid=&searchString=%22fredrikke+marie+qvam%22") // Too many redirects
//                    .setUri("https://www.nb.no/eventyr/?page_id=5") // Mixed-content
//                    .setUri("https://www.nb.no/utstillinger/leksikon/tema01.php") // 404 problemer + data: scheme
//                    .setUri("https://www.nb.no/baser/bjornson/talekart.html") // 404 problemer + data: scheme
                    .setExecutionId("testId")
                    .setJobExecutionId("testId")
//                    .setDiscoveryPath("L")
//                    .setReferrer("http://example.org/")
                    .build();

            ConfigProto.CrawlConfig config = getDefaultConfig();

            File tmpDir = Files.createDirectories(Paths.get("target", "it-workdir")).toFile();
            tmpDir.deleteOnExit();

            Thread.sleep(1000);

            String proxyParam = "--proxy-server=http://" + proxyIp + ":" + proxyPort + "&--ignore-certificate-errors";
            String browserWSEndpoint = "ws://" + browserHost + ":" + browserPort + "/?" + proxyParam;

            ChromeDebugProtocolConfig protocolConfig = new ChromeDebugProtocolConfig(browserWSEndpoint);

            try (RecordingProxy proxy = new RecordingProxy(2, tmpDir, proxyPort, contentWriterClient,
                    new TestHostResolver(), sessionRegistry, cacheHost, cachePort);

                 BrowserController controller = new BrowserController(browserWSEndpoint, sessionRegistry);) {

                RenderResult result = controller.render(0, protocolConfig, queuedUri, config);
                System.out.println("##### " + result);
                int pagesHarvested = 1;
//                int totalPages = result.getOutlinksCount() + 1;
//                result.getOutlinks().forEach(qu -> {
//                    String surt = UriConfigs.SURT_KEY.buildUri(qu.getUri()).toString();
////                    if (!surt.startsWith("(no,nb,")) {
//                        System.out.println("OOS: " + surt + " --- " + qu.getUri());
////                        totalPages--;
////                        continue;
////                    }
//
////                    pagesHarvested++;
////                    System.out.println("URI " + pagesHarvested + " of " + totalPages + ": " + qu.getUri());
//                    try {
//                        Thread.sleep(1000);
//                        RenderResult result2 = controller.render(qu, config);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
////                    if (pagesHarvested > 1) {
////                        break;
////                    }
//                });

//                System.out.println("=========*\n" + result);
                // TODO review the generated test code and remove the default call to fail.
//            fail("The test case is a prototype.");
            } catch (Exception ex) {
                System.out.println("¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤");
                ex.printStackTrace();
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
                .setWindowHeight(1280)
                .setWindowWidth(1024)
                .setPageLoadTimeoutMs(20000)
                .setSleepAfterPageloadMs(10000)
                .addScriptSelector("scope:default")
                .build();

        ConfigProto.PolitenessConfig politenessConfig = ConfigProto.PolitenessConfig.newBuilder()
                .setMeta(ApiTools.buildMeta("Default", "Default politeness configuration"))
                .setRobotsPolicy(ConfigProto.PolitenessConfig.RobotsPolicy.OBEY_ROBOTS)
                .setMinimumRobotsValidityDurationS(86400)
                .setMinTimeBetweenPageLoadMs(1000)
                .build();

        ConfigProto.CrawlConfig config = ConfigProto.CrawlConfig.newBuilder()
                .setMeta(ApiTools.buildMeta("Default", "Default crawl configuration"))
                .setBrowserConfigId(browserConfig.getId())
                .setPolitenessId(politenessConfig.getId())
                .setExtra(ConfigProto.ExtraConfig.newBuilder().setCreateSnapshot(true).setExtractText(true))
                .build();

        return config;
    }

    private DbAdapter getDbMock() throws DbException {
        DbAdapter db = mock(DbAdapter.class);
        ConfigAdapter configAdapter = mock(ConfigAdapter.class);

        when(configAdapter.getBrowserConfig(any())).thenReturn(BrowserConfig.newBuilder()
                .setWindowWidth(900)
                .setWindowHeight(900)
                .build());
        when(db.hasCrawledContent(any())).thenReturn(Optional.empty());
        when(db.saveCrawlLog(any())).thenAnswer((InvocationOnMock i) -> {
            CrawlLog cl = i.getArgument(0);
            cl = cl.toBuilder().setWarcId("WARC_ID").build();
//            System.out.println("CL: " + cl);
            return cl;
        });
        when(db.savePageLog(any(PageLog.class))).then(a -> {
            PageLog o = a.getArgument(0);
//            System.out.println("PageLOG::::");
//            System.out.println(o);
            System.out.println("Uri: " + o.getUri());
            System.out.println("Referrer: " + o.getReferrer());
            System.out.println("resource count: " + o.getResourceCount());
            System.out.println("outlinks count: " + o.getOutlinkCount());
//            System.out.println("::::PageLOG");
            return o;
        });
        when(configAdapter.listBrowserScripts(any())).thenReturn(ControllerProto.BrowserScriptListReply.newBuilder()
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
            System.out.println("H: " + host + ":" + port + " => " + resolvedAddress);
            return resolvedAddress;
        }

    }
}
