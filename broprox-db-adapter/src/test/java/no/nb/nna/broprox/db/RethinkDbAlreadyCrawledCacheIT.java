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
package no.nb.nna.broprox.db;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class RethinkDbAlreadyCrawledCacheIT {

    public static RethinkDbAlreadyCrawledCache db;

    @BeforeClass
    public static void init() {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        db = new RethinkDbAlreadyCrawledCache(dbHost, dbPort, "broprox");
    }

    @AfterClass
    public static void shutdown() {
        if (db != null) {
            db.close();
        }
    }

    @Before
    public void cleanDb() {
        RethinkDB r = RethinkDB.r;
        db.executeRequest(r.table(RethinkDbAdapter.TABLES.ALREADY_CRAWLED_CACHE.name).delete());
    }

    /**
     * Test of put and get method, of class RethinkDbAlreadyCrawledCache.
     */
    @Test
    public void testPutAndGetAndClean() {
        HttpVersion httpVersion = HttpVersion.HTTP_1_1;
        HttpResponseStatus status = HttpResponseStatus.ACCEPTED;
        String uri = "http://www.example.com";
        String executionId1 = "123";
        String executionId2 = "456";
        ByteString cacheValue1 = ByteString.copyFromUtf8("data1");
        ByteString cacheValue2 = ByteString.copyFromUtf8("data2");

        FullHttpResponse result;
        result = db.get(uri, executionId1);
        assertThat(result).isNull();

        db.put(httpVersion, status, uri, executionId1, cacheValue1);
        db.put(httpVersion, status, uri, executionId2, cacheValue2);

        result = db.get(uri, executionId1);
        assertThat(result).isNotNull();
        assertThat(result.status().equals(status));
        assertThat(result.protocolVersion().equals(httpVersion));
        assertThat(result.content().nioBuffer()).isEqualTo(cacheValue1.asReadOnlyByteBuffer());

        db.cleanExecution(executionId1);

        result = db.get(uri, executionId1);
        assertThat(result).isNull();

        result = db.get(uri, executionId2);
        assertThat(result).isNotNull();
        assertThat(result.status().equals(status));
        assertThat(result.protocolVersion().equals(httpVersion));
        assertThat(result.content().nioBuffer()).isEqualTo(cacheValue2.asReadOnlyByteBuffer());
    }

}
