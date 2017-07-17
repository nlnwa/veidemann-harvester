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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.io.ByteStreams;
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
import javax.ws.rs.Produces;
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
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
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

    private static final String htmlHeader = "<html><head>"
            + "<title>WARC files</title>"
            + "<style type=\"text/css\">"
            + "p,td,th,a{font-size:11px; font-family:Helvetica,Arial,sans-serif; font-weight:lighter; text-align:left}"
            + "th{font-weight:bold;}"
            + "a{text-decoration: none;}"
            + "a:hover {color: LimeGreen;text-decoration: none;}"
            + "tr:hover {background-color: #e0e0e9;text-decoration: none;}"
            + "</style>"
            + "</head><body>";

    private static final String htmlFooter = "</body></html>";

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

                    long payloadSize = writeWarcJob.get();
                    LOG.debug("Payload of size {}b written for {}", payloadSize, logEntry.getRequestedUri());
                    size += payloadSize;
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
    @Produces(MediaType.APPLICATION_JSON)
    public String getFilesJson() {
        List<WarcFileDescriptor> files = Arrays.stream(warcWriterPool.getTargetDir().listFiles())
                .map(f -> new WarcFileDescriptor(f.getName(),
                f.length(), uriInfo.getPath() + "/" + f.getName()))
                .collect(Collectors.toList());
        Gson gson = new Gson();
        return gson.toJson(files);
    }

    @GET
    @Path("warcs")
    @Produces(MediaType.TEXT_HTML)
    public String getFilesHtml() {
        StringBuilder html = new StringBuilder(htmlHeader)
                .append("<h3>WARC files</h3>")
                .append("<table><tr><th>Name</th><th>Size</th><th>Table of contents</th></tr>");

        String path = "/" + (uriInfo.getPath().endsWith("/") ? uriInfo.getPath() : uriInfo.getPath() + "/");
        Arrays.stream(warcWriterPool.getTargetDir().listFiles())
                .forEach(f -> {
                    String fileUrl = path + f.getName();
                    html.append("<tr><th><a href='")
                            .append(fileUrl)
                            .append("'>")
                            .append(f.getName())
                            .append("</a></th><th>")
                            .append(f.length())
                            .append("</th><th><a href='")
                            .append(fileUrl).append("/toc")
                            .append("'>")
                            .append("toc")
                            .append("</a></th></tr>");
                });
        html.append("</table>");
        return html.append(htmlFooter).toString();
    }

    @GET
    @Path("warcs/{fileName}")
    public File getFile(@PathParam("fileName") String fileName) {
        return new File(warcWriterPool.getTargetDir(), fileName);
    }

    @GET
    @Path("warcs/{fileName}/toc")
    @Produces(MediaType.TEXT_HTML)
    public String getFileToc(@PathParam("fileName") String fileName) {
        StringBuilder html = new StringBuilder(htmlHeader)
                .append("<h3>Table of contents for ")
                .append(fileName)
                .append("</h3><table><tr><th>ID</th><th>URI</th><th>Type</th><th>WARC Headers</th><th>HTTP Headers</th></tr>");

        String path = "/" + (uriInfo.getPath().replaceFirst("/toc/?", "/"));
        try (Stream<WarcRecord> s = getContent(fileName);) {
            s.forEach(r -> {
                String recordUrl;
                try {
                    recordUrl = path + URLEncoder.encode(r.header.warcRecordIdStr, "UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
                html.append("<tr><td><a href='")
                        .append(recordUrl)
                        .append("'>")
                        .append(r.header.warcRecordIdStr.replace("<", "&lt;"))
                        .append("</a></td><td>")
                        .append(r.header.warcTargetUriStr)
                        .append("</td><td>")
                        .append(r.header.warcTypeStr)
                        .append("</td><td><a href='")
                        .append(recordUrl).append("/warcheader")
                        .append("'>")
                        .append("warcheader</a></td>");
                r.getPayload();
                System.out
                        .println("ID: " + r.header.warcRecordIdStr + ", Payload: " + r.hasPayload() + ", HTTP: " + (r
                                .getHttpHeader() != null));
                if (r.getHttpHeader() != null) {
                    html.append("<td><a href='")
                            .append(recordUrl).append("/httpheader")
                            .append("'>")
                            .append("http header")
                            .append("</a></td>");
                }
                html.append("</tr>");
            });
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
        html.append("</table>");
        return html.append(htmlFooter).toString();
    }

    @GET
    @Path("warcs/{fileName}/{id}/warcheader")
    @Produces(MediaType.TEXT_HTML)
    public String getWarcHeader(@PathParam("fileName") String fileName, @PathParam("id") String id) {
        StringBuilder html = new StringBuilder(htmlHeader)
                .append("<h3>WARC headers for for ")
                .append(fileName).append(" :: ").append(id)
                .append("</h3><pre>");

        try (Stream<WarcRecord> s = getContent(fileName);) {
            s.filter(r -> id.equals(r.header.warcRecordIdStr))
                    .forEach(r -> {
                        html.append(new String(r.header.headerBytes, StandardCharsets.UTF_8));
                    });
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
        html.append("</pre>");
        return html.append(htmlFooter).toString();
    }

    @GET
    @Path("warcs/{fileName}/{id}/httpheader")
    @Produces(MediaType.TEXT_HTML)
    public String getHttpHeader(@PathParam("fileName") String fileName, @PathParam("id") String id) {
        StringBuilder html = new StringBuilder(htmlHeader)
                .append("<h3>HTTP headers for for ")
                .append(fileName).append(" :: ").append(id)
                .append("</h3><pre>");

        try (Stream<WarcRecord> s = getContent(fileName);) {
            s.filter(r -> id.equals(r.header.warcRecordIdStr))
                    .forEach(r -> {
                        html.append(r.getHttpHeader().toString());
                    });
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
        html.append("</pre>");
        return html.append(htmlFooter).toString();
    }

    @GET
    @Path("warcs/{fileName}/{id}")
//    @Produces(MediaType.TEXT_PLAIN)
    public Response getContent(@PathParam("fileName") String fileName, @PathParam("id") String id) {
        try (Stream<WarcRecord> s = getContent(fileName);) {
            List<Response> resp = s.filter(r -> id.equals(r.header.warcRecordIdStr))
                    .map(r -> {
                        String type = MediaType.TEXT_PLAIN;
                        if (r.getHttpHeader() != null) {
                            type = r.getHttpHeader().getHeader("content-type").value;
                        }
                        System.out.println("ID: " + r.header.warcRecordIdStr);
                        System.out.println("TYPE: " + type);
                        System.out.println("HAS PAYLOAD: " + r.hasPayload());
                        System.out.println("HAS HTTP: " + (r.getHttpHeader() != null));

                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        try {
                            ByteStreams.copy(r.getPayloadContent(), baos);
                        } catch (IOException ex) {
                            LOG.error(ex.toString(), ex);
                            throw new RuntimeException(ex);
                        }

                        return Response.ok(baos.toByteArray(), type).build();
//                        return Response.ok((StreamingOutput) (OutputStream output) -> {
//                            ByteStreams.copy(r.getPayloadContent(), output);
//                        }, type).build();
                    })
                    .collect(Collectors.toList());
            return resp.get(0);
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
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

    public Stream<WarcRecord> getContent(String fileName) {
        File warcFile = new File(warcWriterPool.getTargetDir(), fileName);
        try {
            FileInputStream in = new FileInputStream(warcFile);
            WarcReader warcReader = WarcReaderFactory.getReader(in);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(warcReader.iterator(), 0), false)
                    .onClose(() -> {
                        warcReader.close();
                        try {
                            in.close();
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
        } catch (Exception e) {
            System.out.println("---------------");
            e.printStackTrace();
        }
        return null;
    }

}
