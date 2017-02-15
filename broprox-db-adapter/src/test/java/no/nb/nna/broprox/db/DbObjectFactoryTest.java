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


import no.nb.nna.broprox.db.model.CrawlLog;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.gson.reflect.TypeToken;
import no.nb.nna.broprox.db.model.CrawlExecutionStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class DbObjectFactoryTest {

    public DbObjectFactoryTest() {
    }

    /**
     * Test of create method, of class DbObjectFactory.
     */
    @Test
    public void testCreate() {
        CrawlLog result = DbObjectFactory.create(CrawlLog.class)
                .withContentType("foo")
                .withSize(123)
                .withStatusCode(200)
                .withFetchTimeStamp(OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));

        assertThat(result.getContentType()).isEqualTo("foo");
        assertThat(result).isInstanceOf(DbObject.class);
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("contentType", "foo");
        expected.put("size", 123L);
        expected.put("statusCode", 200);
        expected.put("fetchTimeStamp", OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));
        assertThat(((DbObject) result).getMap())
                .isEqualTo(expected);
    }

    /**
     * Test of of method, of class DbObjectFactory.
     */
    @Test
    public void testOf_Class_Map() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("contentType", "foo");
        map.put("size", 123L);
        map.put("statusCode", 200L);
        map.put("fetchTimeStamp", OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));

        Optional<CrawlLog> result = DbObjectFactory.of(CrawlLog.class, map);

        assertThat(result.get().getContentType()).isEqualTo("foo");
        assertThat(result.get().getSize()).isEqualTo(123);
        assertThat(result.get().getStatusCode()).isEqualTo(200);

        assertThat(result.get()).isInstanceOf(DbObject.class);
        assertThat(((DbObject) result.get()).getMap())
                .isEqualTo(map);
    }

    /**
     * Test of of method, of class DbObjectFactory.
     */
    @Test
    public void testOf_Class_String() {
        Optional<CrawlLog> result = DbObjectFactory
                .of(CrawlLog.class, "{\"contentType\": \"foo\", \"size\": 123, \"statusCode\": 200, "
                        + "\"fetchTimeStamp\":{\"dateTime\":{\"date\":{\"year\":2017,\"month\":2,\"day\":2},"
                        + "\"time\":{\"hour\":9,\"minute\":32,\"second\":57,\"nano\":515000000}},"
                        + "\"offset\":{\"totalSeconds\":3600}}}");

        assertThat(result.get()).isInstanceOf(DbObject.class);

        assertThat(result.get().getContentType()).isEqualTo("foo");
        assertThat(result.get().getSize()).isEqualTo(123);
        assertThat(result.get().getStatusCode()).isEqualTo(200);
        assertThat(result.get().getFetchTimeMillis()).isEqualTo(0);
        assertThat(result.get().getBlockDigest()).isEqualTo(null);

        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("contentType", "foo");
        expected.put("size", 123L);
        expected.put("statusCode", 200);
        expected.put("fetchTimeStamp", OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));
        assertThat(((DbObject) result.get()).getMap())
                .isEqualTo(expected);
    }

    /**
     * Test of of method, of class DbObjectFactory.
     */
    @Test
    public void testOf_TypeToken_String() {
        TypeToken type = new TypeToken<List<CrawlLog>>(){};
        Optional<List<CrawlLog>> result = DbObjectFactory
                .of(type, "[{\"contentType\": \"foo\", \"size\": 123, \"statusCode\": 200, "
                        + "\"fetchTimeStamp\":{\"dateTime\":{\"date\":{\"year\":2017,\"month\":2,\"day\":2},"
                        + "\"time\":{\"hour\":9,\"minute\":32,\"second\":57,\"nano\":515000000}},"
                        + "\"offset\":{\"totalSeconds\":3600}}}]");

        assertThat(result.get()).isInstanceOf(List.class);

        assertThat(result.get().get(0).getContentType()).isEqualTo("foo");
        assertThat(result.get().get(0).getSize()).isEqualTo(123);
        assertThat(result.get().get(0).getStatusCode()).isEqualTo(200);
        assertThat(result.get().get(0).getFetchTimeMillis()).isEqualTo(0);
        assertThat(result.get().get(0).getBlockDigest()).isEqualTo(null);

        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("contentType", "foo");
        expected.put("size", 123L);
        expected.put("statusCode", 200);
        expected.put("fetchTimeStamp", OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));
        assertThat(((DbObject) result.get().get(0)).getMap())
                .isEqualTo(expected);
    }

    /**
     * Test of of method, of class DbObjectFactory.
     */
    @Test
    public void testOf_Type_String() {
        TypeToken type = new TypeToken<CrawlLog[]>(){};
        Optional<CrawlLog[]> result = DbObjectFactory
                .of(type, "[{\"contentType\": \"foo\", \"size\": 123, \"statusCode\": 200, "
                        + "\"fetchTimeStamp\":{\"dateTime\":{\"date\":{\"year\":2017,\"month\":2,\"day\":2},"
                        + "\"time\":{\"hour\":9,\"minute\":32,\"second\":57,\"nano\":515000000}},"
                        + "\"offset\":{\"totalSeconds\":3600}}}]");

        assertThat(result.get().getClass().isArray()).isTrue();

        assertThat(result.get()[0].getContentType()).isEqualTo("foo");
        assertThat(result.get()[0].getSize()).isEqualTo(123);
        assertThat(result.get()[0].getStatusCode()).isEqualTo(200);
        assertThat(result.get()[0].getFetchTimeMillis()).isEqualTo(0);
        assertThat(result.get()[0].getBlockDigest()).isEqualTo(null);

        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("contentType", "foo");
        expected.put("size", 123L);
        expected.put("statusCode", 200);
        expected.put("fetchTimeStamp", OffsetDateTime.parse("2017-02-02T09:32:57.515+01:00"));
        assertThat(((DbObject) result.get()[0]).getMap())
                .isEqualTo(expected);
    }

    @Test
    public void testEnum() {
        CrawlExecutionStatus s = DbObjectFactory.create(CrawlExecutionStatus.class);
        s.withState(CrawlExecutionStatus.State.DIED);
        assertThat(s.getMap().get("state")).isInstanceOf(String.class).isEqualTo("DIED");
        assertThat(s.getState()).isSameAs(CrawlExecutionStatus.State.DIED);
    }
}
