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
package no.nb.nna.broprox.contentwriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import io.opentracing.SpanContext;
import io.opentracing.tag.Tags;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import no.nb.nna.broprox.commons.opentracing.OpenTracingJersey;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.contentwriter.text.TextExtracter;
import no.nb.nna.broprox.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.broprox.contentwriter.warc.WarcWriterPool;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
@Path("/")
public class FileUploadResource {

    static final byte[] CRLF = {CR, LF};

    private static final Logger LOG = LoggerFactory.getLogger(FileUploadResource.class);

    @Context
    WarcWriterPool warcWriterPool;

    @Context
    DbAdapter db;

    @Context
    TextExtracter textExtracter;

    @Context
    HttpHeaders httpHeaders;

    @Context
    UriInfo uriInfo;

    public FileUploadResource() {
    }

    @Path("warcrecord")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response postWarcRecord(
            @FormDataParam("logEntry") final String logEntryJson,
            @FormDataParam("headers") final InputStream headers,
            @FormDataParam("payload") final FormDataBodyPart payload) {

        SpanContext parentSpan = OpenTracingJersey.extractSpanHeaders(httpHeaders.getRequestHeaders());
        OpenTracingWrapper otw = new OpenTracingWrapper("FileUploadResource", Tags.SPAN_KIND_SERVER)
                .setParentSpan(parentSpan);

        try (WarcWriterPool.PooledWarcWriter pooledWarcWriter = warcWriterPool.borrow()) {
            return otw.call("postWarcRecord", () -> {
                long size = 0L;

                CrawlLog.Builder logEntryBuilder = CrawlLog.newBuilder();
                JsonFormat.parser().merge(logEntryJson, logEntryBuilder);

                if (logEntryBuilder.getWarcId().isEmpty()) {
                    LOG.error("Missing WARC ID: {}", logEntryJson);
                    throw new WebApplicationException("Missing WARC ID", Response.Status.BAD_REQUEST);
                }

                SingleWarcWriter warcWriter = pooledWarcWriter.getWarcWriter();

                URI ref = warcWriter.writeHeader(logEntryBuilder.build());
                logEntryBuilder.setStorageRef(ref.toString());

                CrawlLog logEntry = db.updateCrawlLog(logEntryBuilder.build());

                if (headers != null) {
                    size += warcWriter.addPayload(headers);
                }

                if (payload != null) {
                    // If both headers and payload are present, add separator
                    if (headers != null) {
                        size += warcWriter.addPayload(CRLF);
                    }

                    ForkJoinTask<Long> writeWarcJob = ForkJoinPool.commonPool().submit(new Callable<Long>() {
                        @Override
                        public Long call() throws Exception {
                            return warcWriter.addPayload(payload.getValueAs(InputStream.class));
                        }

                    });
                    ForkJoinTask<Void> extractTextJob = ForkJoinPool.commonPool().submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            textExtracter.analyze(payload.getValueAs(InputStream.class), logEntry, db);
                            return null;
                        }

                    });

                    size += writeWarcJob.get();
                    extractTextJob.get();
                }

                try {
                    warcWriter.closeRecord();
                } catch (IOException ex) {
                    if (logEntry.getSize() != size) {
                        throw new WebApplicationException("Size doesn't match metadata. Expected " + logEntry
                                .getSize()
                                + ", but was " + size, Response.Status.NOT_ACCEPTABLE);
                    } else {
                        LOG.error(ex.getMessage(), ex);
                        throw new WebApplicationException(ex, Response.Status.NOT_ACCEPTABLE);
                    }
                }

                return Response.created(ref).build();
            });
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new WebApplicationException(ex.getMessage(), ex);
        }
    }

    @GET
    @Path("warcs")
    public String getFiles() {
        List<WarcFileDescriptor> files = Arrays.stream(warcWriterPool.getTargetDir().listFiles())
                .map(f -> new WarcFileDescriptor(f.getName(),
                f.length(), uriInfo.getPath() + "/" + f.getName()))
                .collect(Collectors.toList());
        Gson gson = new Gson();
        return gson.toJson(files);
    }

    @GET
    @Path("warcs/{fileName}")
    public File getFile(@PathParam("fileName") String fileName) {
        return new File(warcWriterPool.getTargetDir(), fileName);
    }

    @DELETE
    @Path("warcs")
    public Response deleteWarcs() {
        try {
            if (ContentWriter.getSettings().isUnsafe()) {
                warcWriterPool.restart(true);
                return Response.ok().build();
            } else {
                return Response.status(Response.Status.METHOD_NOT_ALLOWED).build();
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new WebApplicationException(ex.getMessage(), ex);
        }
    }

}
