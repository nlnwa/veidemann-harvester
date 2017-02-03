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
import java.net.URISyntaxException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import no.nb.nna.broprox.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.broprox.contentwriter.warc.WarcWriterPool;
import no.nb.nna.broprox.db.CrawlLog;
import no.nb.nna.broprox.db.DbObjectFactory;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 *
 */
@Path("/")
public class FileUploadResource {
    @Context
    WarcWriterPool warcWriterPool;

    @Path("warcrecord")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response postWarcRecord(
            @FormDataParam("logEntry") String logEntry,
            @FormDataParam("payload") InputStream payload) {

        SingleWarcWriter warcWriter = null;
        try {
            warcWriter = warcWriterPool.borrow();
            URI ref = warcWriter.writeHeader(DbObjectFactory.of(CrawlLog.class, logEntry).get());
            warcWriter.addPayload(payload);
            warcWriter.closeRecord();
            return Response.created(ref).build();
        } catch (InterruptedException ex) {
            throw new WebApplicationException(ex);
        } finally {
            warcWriterPool.release(warcWriter);
        }
    }

}
