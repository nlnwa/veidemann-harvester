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
package no.nb.nna.broprox.harvester.proxy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;

import net.lightbody.bmp.mitm.CertificateInfo;
import net.lightbody.bmp.mitm.RootCertificateGenerator;
import net.lightbody.bmp.mitm.keys.ECKeyGenerator;
import net.lightbody.bmp.mitm.manager.ImpersonatingMitmManager;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.harvester.BrowserSessionRegistry;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Recording proxy.
 */
public class RecordingProxy implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RecordingProxy.class);

    private final HttpProxyServer server;

    private final AlreadyCrawledCache cache;

    /**
     * Construct a new Recording Proxy.
     * <p>
     * @param workDir a directory for storing temporary files
     * @param port the port to listen to
     * @throws IOException is thrown if certificate directory could not be created
     */
    public RecordingProxy(File workDir, int port, DbAdapter db, final ContentWriterClient contentWriterClient,
            final List<String> dnsServers, BrowserSessionRegistry sessionRegistry) throws IOException {

        LOG.info("Starting recording proxy listening on port {}.", port);

        File certificateDir = new File(workDir, "certificates");
        Files.createDirectories(certificateDir.toPath());

        CertificateInfo certInfo = new CertificateInfo()
                .commonName("Broprox Web Traffic Recorder")
                .organization("Broprox")
                .organizationalUnit("Certificate Authority")
                .notBefore(new Date(System.currentTimeMillis() - 365L * 24L * 60L * 60L * 1000L))
                .notAfter(new Date(System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L));

        File certFile = new File(certificateDir, "BroproxCA.pem");

        // create a dyamic CA root certificate generator using Elliptic Curve keys
        RootCertificateGenerator ecRootCertGenerator = RootCertificateGenerator.builder()
                .certificateInfo(certInfo)
                .keyGenerator(new ECKeyGenerator()) // use EC keys, instead of the default RSA
                .build();

        // save the dynamically-generated CA root certificate for installation in a browser
        ecRootCertGenerator.saveRootCertificateAsPemFile(certFile);

        // tell the MitmManager to use the root certificate we just generated, and to use EC keys when
        // creating impersonated server certs
        ImpersonatingMitmManager mitmManager = ImpersonatingMitmManager.builder()
                .rootCertificateSource(ecRootCertGenerator)
                .serverKeyGenerator(new ECKeyGenerator())
                .trustAllServers(true)
                .build();

        cache = new AlreadyCrawledCache();

        server = DefaultHttpProxyServer.bootstrap()
                .withAllowLocalOnly(false)
                .withPort(port)
                .withTransparent(true)
                .withServerResolver(new DnsLookup(db, contentWriterClient, dnsServers))
                .withManInTheMiddle(mitmManager)
                .withFiltersSource(new RecorderFilterSourceAdapter(db, contentWriterClient, sessionRegistry, cache))
                .start();
    }

    @Override
    public void close() {
        LOG.info("Shutting down recording proxy.");
        server.stop();
    }

    public void cleanCache(String executionId) {
        cache.cleanExecution(executionId);
    }

}
