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

import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

/**
 *
 */
public class ProxyMock {

    private final HttpProxyServer server;

    public ProxyMock(int port) {
//        File certificateDir = new File(workDir, "certificates");

        server = DefaultHttpProxyServer.bootstrap()
                .withAllowLocalOnly(false)
                .withPort(port)
//                .withTransparent(true)
//                .withManInTheMiddle(new CertificateSniffingMitmManager(new SelfSignedAuthority(certificateDir)))
                .withFiltersSource(new LoggingProxy())
                .start();
        System.out.println("PROXY STARTED ON PORT " + port);
    }

}