/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.integrationtests;

import com.google.protobuf.Empty;
import com.rethinkdb.RethinkDB;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels.Level;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ReportGrpc;
import no.nb.nna.veidemann.api.StatusGrpc;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.db.Tables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class CrawlTestBase {
    static ManagedChannel contentWriterChannel;

    static ManagedChannel controllerChannel;

    static ControllerGrpc.ControllerBlockingStub controllerClient;

    static StatusGrpc.StatusBlockingStub statusClient;

    static ReportGrpc.ReportStub reportClient;

    static ContentWriterGrpc.ContentWriterBlockingStub contentWriterClient;

    static RethinkDbAdapter db;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() throws DbConnectionException {
        String controllerHost = System.getProperty("controller.host");
        int controllerPort = Integer.parseInt(System.getProperty("controller.port"));
        String contentWriterHost = System.getProperty("contentwriter.host");
        int contentWriterPort = Integer.parseInt(System.getProperty("contentwriter.port"));
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        System.out.println("Database address: " + dbHost + ":" + dbPort);

        controllerChannel = ManagedChannelBuilder.forAddress(controllerHost, controllerPort).usePlaintext().build();
        controllerClient = ControllerGrpc.newBlockingStub(controllerChannel).withWaitForReady();

        statusClient = StatusGrpc.newBlockingStub(controllerChannel).withWaitForReady();

        reportClient = ReportGrpc.newStub(controllerChannel).withWaitForReady();

        contentWriterChannel = ManagedChannelBuilder.forAddress(contentWriterHost, contentWriterPort).usePlaintext()
                .build();
        contentWriterClient = ContentWriterGrpc.newBlockingStub(contentWriterChannel).withWaitForReady();

        if (!DbService.isConfigured()) {
            CommonSettings dbSettings = new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword("");
            DbService.configure(dbSettings);
        }
        db = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();

        LogLevels.Builder logLevels = controllerClient.getLogConfig(Empty.getDefaultInstance()).toBuilder();
        logLevels.addLogLevelBuilder().setLogger("no.nb.nna.veidemann.frontier").setLevel(Level.INFO);
        controllerClient.saveLogConfig(logLevels.build());
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @After
    public void cleanup() throws DbException {
        contentWriterClient.delete(Empty.getDefaultInstance());
        db.executeRequest("delete", r.table(Tables.CRAWLED_CONTENT.name).delete());
        db.executeRequest("delete", r.table(Tables.CRAWL_LOG.name).delete());
        db.executeRequest("delete", r.table(Tables.PAGE_LOG.name).delete());
        db.executeRequest("delete", r.table(Tables.EXECUTIONS.name).delete());
        db.executeRequest("delete", r.table(Tables.JOB_EXECUTIONS.name).delete());
        db.executeRequest("delete", r.table(Tables.EXTRACTED_TEXT.name).delete());
        db.executeRequest("delete", r.table(Tables.SCREENSHOT.name).delete());
        db.executeRequest("delete", r.table(Tables.URI_QUEUE.name).delete());
        db.executeRequest("delete", r.table(Tables.CRAWL_HOST_GROUP.name).delete());
        db.executeRequest("delete", r.table(Tables.CRAWL_ENTITIES.name).delete());
        db.executeRequest("delete", r.table(Tables.SEEDS.name).delete());
    }

    CrawlEntity createEntity(String name) {
        return controllerClient.saveEntity(
                ConfigProto.CrawlEntity.newBuilder()
                        .setMeta(ConfigProto.Meta.newBuilder().setName(name))
                        .build());
    }

    Seed createSeed(String uri, CrawlEntity entity, String jobId) {
        return controllerClient.saveSeed(ConfigProto.Seed.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder().setName(uri))
                .setEntityId(entity.getId())
                .addJobId(jobId)
                .build());
    }
}
