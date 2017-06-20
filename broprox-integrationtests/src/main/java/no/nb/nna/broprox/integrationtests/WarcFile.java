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
package no.nb.nna.broprox.integrationtests;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 *
 */
public class WarcFile {

    private String name;

    private long size;

    private String uri;

    WarcFile(Object o) {
        if (o instanceof Map) {
            Map m = (Map) o;
            name = (String) m.get("name");
            size = ((Double) m.get("size")).longValue();
            uri = (String) m.get("uri");
        } else {
            throw new IllegalArgumentException("expected java.util.Map, found " + o.getClass());
        }
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public Stream<WarcRecord> getContent() {
        HttpUrl url = WarcInspector.WARC_SERVER_URL.resolve("warcs/" + name);
        Request request = new Request.Builder().url(url).build();
        try {
            Response response = WarcInspector.CLIENT.newCall(request).execute();
            if (response.isSuccessful()) {
                WarcReader warcReader = WarcReaderFactory.getReader(response.body().byteStream());
                return StreamSupport.stream(Spliterators.spliteratorUnknownSize(warcReader.iterator(), 0), false)
                        .onClose(() -> {
                            warcReader.close();
                            response.close();
                        });
            } else {
                throw new IOException("Unexpected code " + response);
            }
        } catch (Exception e) {
            System.out.println("---------------");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString() {
        return "WarcFile{" + "name=" + name + ", uri=" + uri + ", size=" + size + '}';
    }

}
