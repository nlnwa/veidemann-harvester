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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import org.junit.Test;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;
import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class ProtoUtilsTest {

    /**
     * Test of protoToRethink method, of class ProtobufUtils.
     */
    @Test
    public void testProtoToRethink() {
        CrawlEntityListReply msg = CrawlEntityListReply.newBuilder()
                .addEntity(CrawlEntity.newBuilder()
                        .setId("UUID")
                        .setMeta(ConfigProto.Meta.newBuilder()
                                .setName("Nasjonalbiblioteket")
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("frequency")
                                        .setValue("Daily"))
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("orgType")
                                        .setValue("Government"))
                                .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                ).build();

        Map crawlEntity = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                );
        Map crawlEntityList = r.hashMap("entity", r.array(crawlEntity));

        Map<String, Object> result = ProtoUtils.protoToRethink(msg);

        assertThat(result).isEqualTo(crawlEntityList);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_Class() {
        CrawlEntityListReply expResult = CrawlEntityListReply.newBuilder()
                .addEntity(CrawlEntity.newBuilder()
                        .setId("UUID")
                        .setMeta(ConfigProto.Meta.newBuilder()
                                .setName("Nasjonalbiblioteket")
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("frequency")
                                        .setValue("Daily"))
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("orgType")
                                        .setValue("Government"))
                                .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                ).build();

        Map crawlEntity = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                );
        Map crawlEntityList = r.hashMap("entity", r.array(crawlEntity));

        CrawlEntityListReply result = ProtoUtils.rethinkToProto(crawlEntityList, CrawlEntityListReply.class);

        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_MessageBuilder() {
        CrawlEntityListReply expResult = CrawlEntityListReply.newBuilder()
                .addEntity(CrawlEntity.newBuilder()
                        .setId("UUID")
                        .setMeta(ConfigProto.Meta.newBuilder()
                                .setName("Nasjonalbiblioteket")
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("frequency")
                                        .setValue("Daily"))
                                .addLabel(ConfigProto.Label.newBuilder()
                                        .setKey("orgType")
                                        .setValue("Government"))
                                .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                ).build();

        Map crawlEntity = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                );
        Map crawlEntityList = r.hashMap("entity", r.array(crawlEntity));

        Message result = ProtoUtils.rethinkToProto(crawlEntityList, CrawlEntityListReply.newBuilder());

        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of timeStampToOffsetDateTime method, of class ProtobufUtils.
     */
    @Test
    public void testTsToOdt() {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamps.fromMillis(now.toEpochMilli());
        OffsetDateTime expResult = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);

        OffsetDateTime result = ProtoUtils.tsToOdt(timestamp);
        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of offsetDateTimeToTimeStamp method, of class ProtobufUtils.
     */
    @Test
    public void testOdtToTs() {
        Instant now = Instant.now();
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        Timestamp expResult = Timestamps.fromMillis(now.toEpochMilli());

        Timestamp result = ProtoUtils.odtToTs(timestamp);
        assertThat(result).isEqualTo(expResult);
    }

}
