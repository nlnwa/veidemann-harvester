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
package no.nb.nna.veidemann.dnsresolver;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.DnsResolverGrpc;
import no.nb.nna.veidemann.api.DnsResolverProto.ResolveReply;
import no.nb.nna.veidemann.api.DnsResolverProto.ResolveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public class DnsService extends DnsResolverGrpc.DnsResolverImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(DnsService.class);

    private final DnsLookup dnsLookup;

    public DnsService(DnsLookup dnsLookup) {
        this.dnsLookup = dnsLookup;
    }

    @Override
    public void resolve(ResolveRequest request, StreamObserver<ResolveReply> respObserver) {
        try {
            InetSocketAddress address = dnsLookup.resolve(request.getHost(), request.getPort());
            ResolveReply reply = ResolveReply.newBuilder()
                    .setHost(address.getHostString())
                    .setPort(address.getPort())
                    .setTextualIp(address.getAddress().getHostAddress())
                    .setRawIp(ByteString.copyFrom(address.getAddress().getAddress()))
                    .build();
            respObserver.onNext(reply);
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

}
