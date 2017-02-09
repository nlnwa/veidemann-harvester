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

import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import no.nb.nna.broprox.contentwriter.text.TextExtracter;
import no.nb.nna.broprox.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.broprox.contentwriter.warc.WarcWriterPool;
import no.nb.nna.broprox.db.model.CrawlLog;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.DbObjectFactory;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 *
 */
@Path("/")
public class FileUploadResource {

    static final byte[] CRLF = {CR, LF};

    @Context
    WarcWriterPool warcWriterPool;

    @Context
    DbAdapter db;

    @Context
    TextExtracter textExtracter;

    public FileUploadResource() {
    }

    @Path("warcrecord")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response postWarcRecord(
            @FormDataParam("logEntry") final String logEntryJson,
            @FormDataParam("headers") final InputStream headers,
            @FormDataParam("payload") final FormDataBodyPart payload) {

        long size = 0L;

        try (WarcWriterPool.PooledWarcWriter pooledWarcWriter = warcWriterPool.borrow()) {
            CrawlLog logEntry = DbObjectFactory.of(CrawlLog.class, logEntryJson).get();
            SingleWarcWriter warcWriter = pooledWarcWriter.getWarcWriter();

            URI ref = warcWriter.writeHeader(logEntry);
            logEntry.withStorageRef(ref.toString());
            db.updateCrawlLog(logEntry);

            if (headers != null) {
                size += warcWriter.addPayload(headers);
                size += warcWriter.addPayload(CRLF);
            }
            if (payload != null) {
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
//                size += warcWriter.addPayload(payload.getValueAs(InputStream.class));
//                textExtracter.analyze(payload.getValueAs(InputStream.class), logEntry, db);
            }
            warcWriter.closeRecord();
            if (logEntry.getSize() != size) {
                throw new WebApplicationException("Size doesn't match metadata", Response.Status.NOT_ACCEPTABLE);
            }
            return Response.created(ref).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new WebApplicationException(ex.getMessage(), ex);
        }
    }

}
