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

import java.util.Map;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.broprox.api.ControllerGrpc;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class ProxyMockIT implements BroproxHeaderConstants {

    private long timeout = 1000;

    static ManagedChannel channel;

    static ControllerGrpc.ControllerBlockingStub controllerClient;

    static RethinkDbAdapter db;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() {
        String controllerHost = System.getProperty("controller.host");
        int controllerPort = Integer.parseInt(System.getProperty("controller.port"));
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        channel = ManagedChannelBuilder.forAddress(controllerHost, controllerPort).usePlaintext(true).build();
        controllerClient = ControllerGrpc.newBlockingStub(channel);

        db = new RethinkDbAdapter(dbHost, dbPort, "broprox");
    }

    @AfterClass
    public static void shutdown() {
        if (channel != null) {
            channel.shutdown();
        }
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testHarvest() {
        String jobId = controllerClient.listCrawlJobs(ControllerProto.CrawlJobListRequest.newBuilder()
                .setNamePrefix("default").build())
                .getValue(0).getId();

        ConfigProto.CrawlEntity entity = ConfigProto.CrawlEntity.newBuilder().setMeta(ConfigProto.Meta.newBuilder()
                .setName("Test entity 1")).build();
        entity = controllerClient.saveEntity(entity);
        ConfigProto.Seed seed = ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName("http://a1.dev"))
                .setEntityId(entity.getId())
                .addJobId(jobId)
                .build();
        seed = controllerClient.saveSeed(seed);

        ControllerProto.RunCrawlRequest request = ControllerProto.RunCrawlRequest.newBuilder()
                .setJobId(jobId)
                .setSeedId(seed.getId())
                .build();
        controllerClient.runCrawl(request);

        Cursor<Map<String, Object>> cursor = db.executeRequest(r.table(RethinkDbAdapter.TABLES.EXECUTIONS.name)
                .changes());
        for (Map<String, Object> exec : cursor) {
            System.out.println(">>> " + exec);
            if (exec.containsKey("new_val") && "FINISHED".equals(((Map) exec.get("new_val")).get("state"))) {
                System.out.println("HEPP");
                break;
            }
        }

        fail("This is a prototype");
    }

}
