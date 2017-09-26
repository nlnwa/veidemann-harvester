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
import no.nb.nna.broprox.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.PolitenessConfig;
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
        PolitenessConfigListReply msg = PolitenessConfigListReply.newBuilder()
                .addValue(PolitenessConfig.newBuilder()
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
                        .setDelayFactor(.1f)
                ).build();

        Map politenessConfig = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("delayFactor", .1f);
        Map politenessConfigList = r.hashMap("value", r.array(politenessConfig));

        Map<String, Object> result = ProtoUtils.protoToRethink(msg);

        assertThat(result).isEqualTo(politenessConfigList);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_Class() {
        PolitenessConfigListReply expResult = PolitenessConfigListReply.newBuilder()
                .addValue(PolitenessConfig.newBuilder()
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
                        .setDelayFactor(.1f)
                ).build();

        Map politenessConfig = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("delayFactor", .1f);
        Map politenessConfigList = r.hashMap("value", r.array(politenessConfig));

        PolitenessConfigListReply result = ProtoUtils
                .rethinkToProto(politenessConfigList, PolitenessConfigListReply.class);

        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_MessageBuilder() {
        PolitenessConfigListReply expResult = PolitenessConfigListReply.newBuilder()
                .addValue(PolitenessConfig.newBuilder()
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
                        .setDelayFactor(.1f)
                ).build();

        Map politenessConfig = r.hashMap("id", "UUID")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("delayFactor", .1f);
        Map politenessConfigList = r.hashMap("value", r.array(politenessConfig));

        Message result = ProtoUtils.rethinkToProto(politenessConfigList, PolitenessConfigListReply.newBuilder());

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
