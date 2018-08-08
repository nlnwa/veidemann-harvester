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
package no.nb.nna.veidemann.harvester.proxy;

import io.netty.handler.codec.http.HttpRequest;
import net.lightbody.bmp.mitm.CertificateInfo;
import net.lightbody.bmp.mitm.RootCertificateGenerator;
import net.lightbody.bmp.mitm.TrustSource;
import net.lightbody.bmp.mitm.keys.ECKeyGenerator;
import net.lightbody.bmp.mitm.manager.ImpersonatingMitmManager;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import org.littleshoot.proxy.ChainedProxy;
import org.littleshoot.proxy.ChainedProxyAdapter;
import org.littleshoot.proxy.ChainedProxyManager;
import org.littleshoot.proxy.HostResolver;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.HttpProxyServerBootstrap;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.littleshoot.proxy.impl.ThreadPoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;

/**
 * A Recording proxy.
 */
public class RecordingProxy implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RecordingProxy.class);

    private final List<HttpProxyServer> servers = new ArrayList<>();

    private final String cacheHost;

    private final int cachePort;

    /**
     * Construct a new Recording Proxy.
     * <p>
     *
     * @param workDir a directory for storing temporary files
     * @param port    the port to listen to
     * @throws IOException is thrown if certificate directory could not be created
     */
    public RecordingProxy(final int serverCount, File workDir, int port, final ContentWriterClient contentWriterClient,
                          final HostResolver hostResolver, BrowserSessionRegistry sessionRegistry,
                          String cacheHost, int cachePort) throws IOException {

        LOG.info("Starting recording proxy listening on port {}.", port);

        this.cacheHost = cacheHost;
        this.cachePort = cachePort;

        File certificateDir = new File(workDir, "certificates");
        Files.createDirectories(certificateDir.toPath());

        CertificateInfo certInfo = new CertificateInfo()
                .commonName("Veidemann Web Traffic Recorder")
                .organization("Veidemann")
                .organizationalUnit("Certificate Authority")
                .notBefore(new Date(System.currentTimeMillis() - 365L * 24L * 60L * 60L * 1000L))
                .notAfter(new Date(System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L));

        File genratedCertFile = new File(certificateDir, "VeidemannCA.pem");
        File cacheCaCertFile = new File(certificateDir, "cache-selfsignedCA.crt");

        // create a dyamic CA root certificate generator using Elliptic Curve keys
        RootCertificateGenerator certificateAndKeySource = RootCertificateGenerator.builder()
                .certificateInfo(certInfo)
                .keyGenerator(new ECKeyGenerator()) // use EC keys, instead of the default RSA
                .build();
        // save the dynamically-generated CA root certificate for installation in a browser
        certificateAndKeySource.saveRootCertificateAsPemFile(genratedCertFile);

        TrustSource trustSource = TrustSource.defaultTrustSource().add(cacheCaCertFile);

        // tell the MitmManager to use the root certificate we just generated, and to use EC keys when
        // creating impersonated server certs
        ImpersonatingMitmManager mitmManager = ImpersonatingMitmManager.builder()
                .rootCertificateSource(certificateAndKeySource)
                .serverKeyGenerator(new ECKeyGenerator())
                .trustSource(trustSource)
                .trustAllServers(true)
                .build();

        HttpProxyServerBootstrap serverBootstrap = DefaultHttpProxyServer.bootstrap()
                .withAllowLocalOnly(false)
                .withPort(port)
                .withTransparent(true)
                .withServerResolver(hostResolver)
                .withManInTheMiddle(mitmManager)
                .withMaxChunkSize(1024 * 1024)
                .withMaxHeaderSize(1024 * 32)
                .withConnectTimeout(60000)
                .withIdleConnectionTimeout(10)
                .withThreadPoolConfiguration(new ThreadPoolConfiguration().withAcceptorThreads(4).withClientToProxyWorkerThreads(16).withProxyToServerWorkerThreads(16))
                .withChainProxyManager(new ChainedProxyManager() {
                    @Override
                    public void lookupChainedProxies(HttpRequest httpRequest, Queue<ChainedProxy> chainedProxies) {
                        chainedProxies.add(new ChainedProxyAdapter() {
                            @Override
                            public InetSocketAddress getChainedProxyAddress() {
                                return InetSocketAddress.createUnresolved(cacheHost, cachePort);
                            }
                        });
//                        chainedProxies.add(FALLBACK_TO_DIRECT_CONNECTION);
                    }
                });

        HttpProxyServer server = serverBootstrap.withFiltersSource(
                new RecorderFilterSourceAdapter(0, contentWriterClient, sessionRegistry, hostResolver)).start();
        servers.add(server);
        LOG.info("Started proxy 0 on port: {}", server.getListenAddress().getPort());

        for (int i = 1; i < serverCount; i++) {
            server = server.clone().withFiltersSource(
                    new RecorderFilterSourceAdapter(i, contentWriterClient, sessionRegistry, hostResolver)).start();
            servers.add(server);
            LOG.info("Started proxy {} on port: {}", i, server.getListenAddress().getPort());
        }

        LOG.info("Recording proxy started.");
    }

    @Override
    public void close() {
        LOG.info("Shutting down recording proxy.");
        for (HttpProxyServer server : servers) {
            server.stop();
        }
    }

}
