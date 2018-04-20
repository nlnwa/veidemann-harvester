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
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels.Level;
import no.nb.nna.veidemann.api.ContentWriterGrpc;
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.StatusGrpc;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.junit.After;
import org.junit.BeforeClass;

public abstract class CrawlTestBase {
    static ManagedChannel contentWriterChannel;

    static ManagedChannel controllerChannel;

    static ControllerGrpc.ControllerBlockingStub controllerClient;

    static StatusGrpc.StatusBlockingStub statusClient;

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

        controllerChannel = ManagedChannelBuilder.forAddress(controllerHost, controllerPort).usePlaintext(true).build();
        controllerClient = ControllerGrpc.newBlockingStub(controllerChannel).withWaitForReady();

        statusClient = StatusGrpc.newBlockingStub(controllerChannel).withWaitForReady();

        contentWriterChannel = ManagedChannelBuilder.forAddress(contentWriterHost, contentWriterPort).usePlaintext(true)
                .build();
        contentWriterClient = ContentWriterGrpc.newBlockingStub(contentWriterChannel).withWaitForReady();

        if (!RethinkDbConnection.isConfigured()) {
            RethinkDbConnection.configure(dbHost, dbPort, "veidemann", "admin", "");
        }
        db = new RethinkDbAdapter();

        LogLevels.Builder logLevels = controllerClient.getLogConfig(Empty.getDefaultInstance()).toBuilder();
        logLevels.addLogLevelBuilder().setLogger("no.nb.nna.veidemann.frontier").setLevel(Level.INFO);
        controllerClient.saveLogConfig(logLevels.build());
    }

    @After
    public void cleanup() throws DbException {
        contentWriterClient.delete(Empty.getDefaultInstance());
        db.executeRequest("delete", r.table(TABLES.CRAWLED_CONTENT.name).delete());
        db.executeRequest("delete", r.table(TABLES.CRAWL_LOG.name).delete());
        db.executeRequest("delete", r.table(TABLES.PAGE_LOG.name).delete());
        db.executeRequest("delete", r.table(TABLES.EXECUTIONS.name).delete());
        db.executeRequest("delete", r.table(TABLES.JOB_EXECUTIONS.name).delete());
        db.executeRequest("delete", r.table(TABLES.EXTRACTED_TEXT.name).delete());
        db.executeRequest("delete", r.table(TABLES.SCREENSHOT.name).delete());
        db.executeRequest("delete", r.table(TABLES.URI_QUEUE.name).delete());
        db.executeRequest("delete", r.table(TABLES.CRAWL_HOST_GROUP.name).delete());
        db.executeRequest("delete", r.table(TABLES.ALREADY_CRAWLED_CACHE.name).delete());
    }
}
