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

package no.nb.nna.veidemann.commons.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.DnsResolverGrpc;
import no.nb.nna.veidemann.api.DnsResolverProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DnsServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DnsServiceClient.class);

    private final ManagedChannel channel;

    private final DnsResolverGrpc.DnsResolverBlockingStub blockingStub;

    private final DnsResolverGrpc.DnsResolverStub asyncStub;

    public DnsServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("DNS service client pointing to " + host + ":" + port);
    }

    public DnsServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up DNS service client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = DnsResolverGrpc.newBlockingStub(channel);
        asyncStub = DnsResolverGrpc.newStub(channel);
    }

    public InetSocketAddress resolve(String host, int port) throws UnknownHostException {
        try {
            // Ensure host is never null
            host = host == null ? "" : host;
            DnsResolverProto.ResolveRequest request = DnsResolverProto.ResolveRequest.newBuilder()
                    .setHost(host)
                    .setPort(port)
                    .build();
            DnsResolverProto.ResolveReply reply = blockingStub.resolve(request);
            InetSocketAddress address = new InetSocketAddress(
                    InetAddress.getByAddress(reply.getHost(), reply.getRawIp().toByteArray()), reply.getPort());
            return address;
        } catch (StatusRuntimeException ex) {
            LOG.debug("RPC failed: " + ex.getStatus(), ex);
            throw new UnknownHostException(host);
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }


}
