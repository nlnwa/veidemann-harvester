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
package no.nb.nna.broprox.contentexplorer;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import no.nb.nna.broprox.commons.db.DbAdapter;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
@Path("/")
public class ContentExplorerResource {

    private static final Logger LOG = LoggerFactory.getLogger(ContentExplorerResource.class);

    @Context
    DbAdapter db;

    @Context
    File warcDir;

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

    public ContentExplorerResource() {
    }

    @GET
    @Path("warcs")
    @Produces(MediaType.APPLICATION_JSON)
    public String getFilesJson() {
        List<WarcFileDescriptor> files = Arrays.stream(warcDir.listFiles())
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
        Arrays.stream(warcDir.listFiles())
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
        return new File(warcDir, fileName);
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
        try (Stream<WarcRecord> s = getRecords(fileName);) {
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
                .append("<h3>WARC headers for ")
                .append(fileName).append(" :: ").append(id.replaceAll("<", "&lt;"))
                .append("</h3><pre>");

        try (Stream<WarcRecord> s = getRecords(fileName);) {
            s.filter(r -> id.equals(r.header.warcRecordIdStr))
                    .forEach(r -> {
                        html.append(new String(r.header.headerBytes, StandardCharsets.UTF_8).replaceAll("<", "&lt;"));
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
                .append("<h3>HTTP headers for ")
                .append(fileName).append(" :: ").append(id.replaceAll("<", "&lt;"))
                .append("</h3><pre>");

        try (Stream<WarcRecord> s = getRecords(fileName);) {
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
        try (Stream<WarcRecord> s = getRecords(fileName);) {
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

    @GET
    @Path("storageref/{ref}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getContentForRef(@PathParam("ref") String storageRef) {
        try (WarcRecordContainer r = getRecord(storageRef);) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ByteStreams.copy(r.warcRecord.getPayloadContent(), baos);
            } catch (IOException ex) {
                LOG.error(ex.toString(), ex);
                throw new RuntimeException(ex);
            }

            String type = MediaType.TEXT_PLAIN;
            return Response.ok(baos.toByteArray(), type).build();
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
    }

    @GET
    @Path("storageref/{ref}/warcheader")
    @Produces(MediaType.TEXT_HTML)
    public String getWarcHeaderForRef(@PathParam("ref") String storageRef) {
        try (WarcRecordContainer r = getRecord(storageRef);) {
            StringBuilder html = new StringBuilder(htmlHeader)
                    .append("<h3>WARC headers for ")
                    .append(storageRef.replaceAll("<", "&lt;"))
                    .append("</h3><pre>");

            html.append(new String(r.warcRecord.header.headerBytes, StandardCharsets.UTF_8).replaceAll("<", "&lt;"));
            html.append("</pre>");
            return html.append(htmlFooter).toString();
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new WebApplicationException(ex, 404);
        }
    }

    public Stream<WarcRecord> getRecords(String fileName) {
        File warcFile = new File(warcDir, fileName);
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

    public WarcRecordContainer getRecord(String storageRef) {
        if (storageRef.startsWith("warcfile:")) {
            String storageRefNoScheme = storageRef.substring(9);
            if (!storageRefNoScheme.contains(":")) {
                throw new WebApplicationException("Invalid storageref: " + storageRef, Status.BAD_REQUEST);
            }
            String fileName = storageRefNoScheme.substring(0, storageRefNoScheme.lastIndexOf(":"));
            long offset = Long.parseLong(storageRefNoScheme.substring(storageRefNoScheme.lastIndexOf(":") + 1));
            File warcFile = new File(warcDir, fileName);
            try {
                FileInputStream in;
                try {
                    in = new FileInputStream(warcFile);
                } catch (FileNotFoundException ex) {
                    in = new FileInputStream(warcFile + ".open");
                }
                in.skip(offset);
                WarcReader warcReader = WarcReaderFactory.getReaderCompressed();
                WarcRecord record = warcReader.getNextRecordFrom(in, offset);
                return new WarcRecordContainer(in, warcReader, record);
            } catch (Exception e) {
                System.out.println("---------------");
                e.printStackTrace();
            }
            return null;
        } else {
            throw new WebApplicationException("Unknown scheme: " + storageRef, Status.BAD_REQUEST);
        }
    }

    private class WarcRecordContainer implements AutoCloseable {
        private final InputStream in;
        private final WarcReader warcReader;
        private final WarcRecord warcRecord;

        public WarcRecordContainer(InputStream in, WarcReader warcReader, WarcRecord warcRecord) {
            this.in = in;
            this.warcReader = warcReader;
            this.warcRecord = warcRecord;
        }

        @Override
        public void close() {
            try {
                warcRecord.close();
            } catch (Exception ex) {
            }
            try {
                warcReader.close();
            } catch (Exception ex) {
            }
            try {
                in.close();
            } catch (Exception ex) {
            }
        }
    }
}
