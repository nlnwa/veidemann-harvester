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

import java.time.OffsetDateTime;

import com.google.protobuf.InvalidProtocolBufferException;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for RethinkDbAdapter.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbAdapterIT {

    public static RethinkDbAdapter db;

    public RethinkDbAdapterIT() {
    }

    @BeforeClass
    public static void init() {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        db = new RethinkDbAdapter(dbHost, dbPort, "broprox");
    }

    @AfterClass
    public static void shutdown() {
        if (db != null) {
            db.close();
        }
    }

    /**
     * Test of saveCrawlEntity method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveCrawlEntity() throws InvalidProtocolBufferException {
        System.out.println("saveCrawlEntity");
        CrawlEntity entity = CrawlEntity.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();

        CrawlEntity result = db.saveCrawlEntity(entity);

        assertThat(result.getId()).isNotEmpty();
        assertThat(result).isEqualTo(entity.toBuilder().setId(result.getId()).build());
    }

    /**
     * Test of listCrawlEntities method, of class RethinkDbAdapter.
     */
    @Test
    public void testListCrawlEntities() throws InvalidProtocolBufferException {
        CrawlEntity entity1 = CrawlEntity.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity1 = db.saveCrawlEntity(entity1);

        CrawlEntity entity2 = CrawlEntity.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder()
                        .setName("VG")
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Hourly"))
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("orgType")
                                .setValue("News"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity2 = db.saveCrawlEntity(entity2);

        ControllerProto.CrawlEntityListRequest request = ControllerProto.CrawlEntityListRequest.getDefaultInstance();
        ControllerProto.CrawlEntityListReply result = db.listCrawlEntities(request);
        assertThat(result.getEntityCount()).isGreaterThanOrEqualTo(2);
        assertThat(result.getEntityList()).contains(entity1, entity2);

        request = ControllerProto.CrawlEntityListRequest.newBuilder().setId(entity1.getId()).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getEntityCount()).isEqualTo(1);
        assertThat(result.getEntityList()).contains(entity1);
    }

}
