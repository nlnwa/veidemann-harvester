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

import java.net.URI;
import java.util.concurrent.Callable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.opentracing.tag.Tags;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import no.nb.nna.broprox.commons.opentracing.OpenTracingJersey;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

/**
 *
 */
public class ContentWriterClient implements AutoCloseable {

    final static Client CLIENT = ClientBuilder.newClient()
            .register(MultiPartFeature.class);

    final WebTarget contentWriterTarget;

    public ContentWriterClient(final String host, final int port) {
        contentWriterTarget = CLIENT.target(UriBuilder.fromPath("/").host(host).port(port).scheme("http").build());
    }

    public URI writeRecord(CrawlLog logEntry, ByteBuf headers, ByteBuf payload) {
        OpenTracingWrapper otw = new OpenTracingWrapper("ContentWriterClient", Tags.SPAN_KIND_CLIENT);
        try {
            return otw.call("writeWarcRecord", new WriteRecordTask(logEntry, headers, payload));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        CLIENT.close();
    }

    private class WriteRecordTask implements Callable<URI> {

        final CrawlLog logEntry;

        final ByteBuf headers;

        final ByteBuf payload;

        public WriteRecordTask(CrawlLog logEntry, ByteBuf headers, ByteBuf payload) {
            this.logEntry = logEntry;
            this.headers = headers;
            this.payload = payload;
        }

        @Override
        public URI call() throws Exception {
            final MultiPart multipart;
            try {
                multipart = new FormDataMultiPart()
                        .field("logEntry", JsonFormat.printer().print(logEntry));
            } catch (InvalidProtocolBufferException ex) {
                throw new RuntimeException(ex);
            }

            if (headers != null) {
                final StreamDataBodyPart headersPart = new StreamDataBodyPart("headers", new ByteBufInputStream(headers));
                multipart.bodyPart(headersPart);
            }

            if (payload != null) {
                final StreamDataBodyPart payloadPart = new StreamDataBodyPart("payload", new ByteBufInputStream(payload));
                multipart.bodyPart(payloadPart);
            }

            MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
            OpenTracingJersey.injectSpanHeaders(httpHeaders);

            Response storageRef = contentWriterTarget.path("warcrecord")
                    .request()
                    .headers(httpHeaders)
                    .post(Entity.entity(multipart, multipart.getMediaType()), Response.class);

            if (storageRef.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                throw new WebApplicationException(storageRef);
            }

            return storageRef.getLocation();
        }
    }
}
