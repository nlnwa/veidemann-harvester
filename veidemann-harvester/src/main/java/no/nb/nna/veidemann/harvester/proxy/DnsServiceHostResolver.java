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

import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.commons.client.DnsServiceClient;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import org.littleshoot.proxy.HostResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 *
 */
public class DnsServiceHostResolver implements HostResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DnsServiceHostResolver.class);

    private final DnsServiceClient dnsServiceClient;
    private final BrowserSessionRegistry browserSessionRegistry;
    private final int proxyId;

    public DnsServiceHostResolver(final DnsServiceClient dnsServiceClient, final BrowserSessionRegistry browserSessionRegistry, final int proxyId) {
        this.dnsServiceClient = dnsServiceClient;
        this.browserSessionRegistry = browserSessionRegistry;
        this.proxyId = proxyId;
    }

    @Override
    public InetSocketAddress resolve(String host, int port) throws UnknownHostException {
        ConfigRef collectionRef = browserSessionRegistry.get(proxyId).getCollectionRef();
        return dnsServiceClient.resolve(host, port, collectionRef);
    }

    public InetSocketAddress resolve(String host, int port, ConfigRef collectionRef) throws UnknownHostException {
        return dnsServiceClient.resolve(host, port, collectionRef);
    }
}
