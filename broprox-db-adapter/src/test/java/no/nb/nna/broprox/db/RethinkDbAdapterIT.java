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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.model.MessagesProto.CrawlEntity;
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
                .setName("Nasjonalbiblioteket")
                .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                .addLabel("Daily")
                .addLabel("Government")
                .build();

        CrawlEntity expResult = null;

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
                .setName("Nasjonalbiblioteket")
                .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                .addLabel("Daily")
                .addLabel("Government")
                .build();
        entity1 = db.saveCrawlEntity(entity1);

        CrawlEntity entity2 = CrawlEntity.newBuilder()
                .setName("VG")
                .setCreated(Timestamps.fromMillis(System.currentTimeMillis()))
                .addLabel("Hourly")
                .addLabel("News")
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
