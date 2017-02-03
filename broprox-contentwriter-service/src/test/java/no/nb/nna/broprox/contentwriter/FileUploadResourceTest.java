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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import no.nb.nna.broprox.contentwriter.warc.WarcWriterPool;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class FileUploadResourceTest extends JerseyTest {
    String logEntry = "{\n"
                        + "\"contentType\":  \"image/gif\" ,\n"
                        + "\"digest\":  \"sha1:dc1cdfa814046ea64609c438e1777f55ff3aa86c\" ,\n"
                        + "\"discoveryPath\":  \"E\" ,\n"
                        + "\"fetchTimeMillis\": 16,\n"
                        + "\"fetchTimeStamp\": {\"dateTime\":{\"date\":{\"year\":2017,\"month\":2,\"day\":2},\"time\":{"
                        + "\"hour\":9,\"minute\":32,\"second\":57,\"nano\":515000000}},\"offset\":{\"totalSeconds\":3600}},\n"
                        + "\"warcId\":  \"dff5cbea-84b3-4faf-ab62-e047feef5636\" ,\n"
                        + "\"recordType\":  \"revisit\" ,\n"
                        + "\"referrer\": \"http://johnh.nb.no/\",\n"
                        + "\"requestedUri\": \"http://johnh.nb.no/icons/unknown.gif\",\n"
                        + "\"size\": 9,\n"
                        + "\"statusCode\": 200,\n"
                        + "\"surt\":  \"(no,nb,johnh,)/icons/unknown.gif\" ,\n"
                        + "\"timeStamp\": {\"dateTime\":{\"date\":{\"year\":2017,\"month\":2,\"day\":2},\"time\":{\"hour\":"
                        + "9,\"minute\":32,\"second\":57,\"nano\":515000000}},\"offset\":{\"totalSeconds\":3600}}\n"
                        + "}";

    WarcWriterPool warcWriterPool;

    @Override
    protected Application configure() {
        System.out.println("Configure");
        boolean compress = true;
        long maxFileSize = 1024;
        int poolSize = 1;
        File targetDir = new File("target/test");
        warcWriterPool = new WarcWriterPool(targetDir, maxFileSize, compress, poolSize);

        return new ResourceConfig()
                .register(MultiPartFeature.class)
                .register(FileUploadResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(warcWriterPool);
                    }

                });
    }

    @After
    public void close() throws Exception {
        System.out.println("Cleanup");
        warcWriterPool.close();
    }

    /**
     * Test of postWarcRecord method, of class FileUploadResource.
     */
    @Test
    @Ignore
    public void testPostWarcRecord() {
        System.out.println("postWarcRecord");
        InputStream payload = null;

        final StreamDataBodyPart filePart = new StreamDataBodyPart("payload", new ByteArrayInputStream("Test data"
                .getBytes()));

        final MultiPart multipart = new FormDataMultiPart()
                .field("logEntry", logEntry)
                .bodyPart(filePart);

        Response storageRef = target("warcrecord")
                .register(MultiPartFeature.class)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()), Response.class);

        System.out.println("REF: " + storageRef.getLocation());
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}
