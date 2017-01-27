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
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.gson.Gson;
import no.nb.nna.broprox.chrome.client.ChromeDebugProtocol;
import no.nb.nna.broprox.chrome.client.PageDomain;
import no.nb.nna.broprox.chrome.client.Session;

/**
 *
 */
public class BrowserController implements AutoCloseable {

    private final ChromeDebugProtocol chrome;

    private final String chromeHost;

    private final int chromePort;

    private static final String versionScript = "var nVer = navigator.appVersion;\n" +
"var nAgt = navigator.userAgent;\n" +
"var browserName  = navigator.appName;\n" +
"var fullVersion  = ''+parseFloat(navigator.appVersion); \n" +
"var majorVersion = parseInt(navigator.appVersion,10);\n" +
"var nameOffset,verOffset,ix;\n" +
"\n" +
"// In Opera 15+, the true version is after \"OPR/\"\n" +
"if ((verOffset=nAgt.indexOf(\"OPR/\"))!=-1) {\n" +
" browserName = \"Opera\";\n" +
" fullVersion = nAgt.substring(verOffset+4);\n" +
"}\n" +
"// In older Opera, the true version is after \"Opera\" or after \"Version\"\n" +
"else if ((verOffset=nAgt.indexOf(\"Opera\"))!=-1) {\n" +
" browserName = \"Opera\";\n" +
" fullVersion = nAgt.substring(verOffset+6);\n" +
" if ((verOffset=nAgt.indexOf(\"Version\"))!=-1) \n" +
"   fullVersion = nAgt.substring(verOffset+8);\n" +
"}\n" +
"// In MSIE, the true version is after \"MSIE\" in userAgent\n" +
"else if ((verOffset=nAgt.indexOf(\"MSIE\"))!=-1) {\n" +
" browserName = \"Microsoft Internet Explorer\";\n" +
" fullVersion = nAgt.substring(verOffset+5);\n" +
"}\n" +
"// In Chrome, the true version is after \"Chrome\"\n" +
"else if ((verOffset=nAgt.indexOf(\"Chrome\"))!=-1) {\n" +
" browserName = \"Chrome\";\n" +
" fullVersion = nAgt.substring(verOffset+7);\n" +
"}\n" +
"// In Safari, the true version is after \"Safari\" or after \"Version\"\n" +
"else if ((verOffset=nAgt.indexOf(\"Safari\"))!=-1) {\n" +
" browserName = \"Safari\";\n" +
" fullVersion = nAgt.substring(verOffset+7);\n" +
" if ((verOffset=nAgt.indexOf(\"Version\"))!=-1) \n" +
"   fullVersion = nAgt.substring(verOffset+8);\n" +
"}\n" +
"// In Firefox, the true version is after \"Firefox\"\n" +
"else if ((verOffset=nAgt.indexOf(\"Firefox\"))!=-1) {\n" +
" browserName = \"Firefox\";\n" +
" fullVersion = nAgt.substring(verOffset+8);\n" +
"}\n" +
"// In most other browsers, \"name/version\" is at the end of userAgent\n" +
"else if ( (nameOffset=nAgt.lastIndexOf(' ')+1) < \n" +
"          (verOffset=nAgt.lastIndexOf('/')) ) \n" +
"{\n" +
" browserName = nAgt.substring(nameOffset,verOffset);\n" +
" fullVersion = nAgt.substring(verOffset+1);\n" +
" if (browserName.toLowerCase()==browserName.toUpperCase()) {\n" +
"  browserName = navigator.appName;\n" +
" }\n" +
"}\n" +
"// trim the fullVersion string at semicolon/space if present\n" +
"if ((ix=fullVersion.indexOf(\";\"))!=-1)\n" +
"   fullVersion=fullVersion.substring(0,ix);\n" +
"if ((ix=fullVersion.indexOf(\" \"))!=-1)\n" +
"   fullVersion=fullVersion.substring(0,ix);\n" +
"\n" +
"majorVersion = parseInt(''+fullVersion,10);\n" +
"if (isNaN(majorVersion)) {\n" +
" fullVersion  = ''+parseFloat(navigator.appVersion); \n" +
" majorVersion = parseInt(navigator.appVersion,10);\n" +
"}\n" +
"\n" +
"document.write(''\n" +
" +'Browser name  = '+browserName+'<br>'\n" +
" +'Full version  = '+fullVersion+'<br>'\n" +
" +'Major version = '+majorVersion+'<br>'\n" +
" +'navigator.appName = '+navigator.appName+'<br>'\n" +
" +'navigator.userAgent = '+navigator.userAgent+'<br>'\n" +
")";

    public BrowserController(String chromeHost, int chromePort) throws IOException {
        this.chromeHost = chromeHost;
        this.chromePort = chromePort;
        this.chrome = new ChromeDebugProtocol(chromeHost, chromePort);
    }

    public byte[] render(String url, int w, int h, int timeout, int sleep) throws ExecutionException, InterruptedException, IOException, TimeoutException {
        try (Session tab = chrome.newSession(w, h)) {
                tab.page.enable().get(timeout, TimeUnit.MILLISECONDS);
                CompletableFuture<PageDomain.LoadEventFired> loaded = tab.page.onLoadEventFired();

                tab.page.navigate(url).get(timeout, TimeUnit.MILLISECONDS);

                loaded.get(timeout, TimeUnit.MILLISECONDS);

                // disable scrollbars
                tab.runtime.evaluate("document.getElementsByTagName('body')[0].style.overflow='hidden'",
                        null, null, null, null, null, null, null, null)
                        .get(timeout, TimeUnit.MILLISECONDS);

                // wait a little for any onload javascript to fire
                Thread.sleep(sleep);

//                System.out.println("LINKS >>>>>>");
//                for (PageDomain.FrameResource fs : tab.page.getResourceTree().get().frameTree.resources) {
//                    System.out.println("T: " + fs.type);
//                    if ("Script".equals(fs.type)) {
//                        System.out.println(">: " + fs.toString());
//                    }
//                }
//                System.out.println("<<<<<<");
                String data = tab.page.captureScreenshot().get(timeout, TimeUnit.MILLISECONDS).data;
                return Base64.getDecoder().decode(data);
//                return null;
            }
    }

    @Override
    public void close() throws Exception {
        chrome.close();
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException, Exception {
        try (BrowserController ab = new BrowserController("localhost", 9222);) {
            ab.render("https://www.nb.no", 500, 500, 10000, 1000);
        }
    }

}
