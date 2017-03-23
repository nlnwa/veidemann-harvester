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
package no.nb.nna.broprox.contentwriter.warc;

import java.io.File;
import java.io.IOException;

import no.nb.nna.broprox.db.model.CrawlLog;
import no.nb.nna.broprox.db.DbObjectFactory;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class SingleWarcWriterTest {

    /**
     * Test of write method, of class SingleWarcWriter.
     */
    @Test
    @Ignore
    public void testWrite() throws Exception {
        System.out.println("write");
        String data = "Test data";

        boolean compress = true;
        long maxFileSize = 1024;
        File targetDir = new File("target/test");

        targetDir.mkdirs();
        for (File file : targetDir.listFiles()) {
            file.delete();
        }

        try (SingleWarcWriter instance = new SingleWarcWriter(targetDir, maxFileSize, compress, 1);) {
            for (int i = 0; i < 10; i++) {
                DbObjectFactory.of(CrawlLog.class, "{\n"
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
                        + "}")
                        .ifPresent(l -> {
                            System.out.println("REF: " + instance.writeHeader(l));
                            instance.addPayload(data.getBytes());
                    try {
                        instance.closeRecord();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                        });

            }
        }
        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

}
