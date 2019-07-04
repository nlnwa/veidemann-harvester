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
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ReportGrpc;
import no.nb.nna.veidemann.api.StatusGrpc;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.Meta;
import no.nb.nna.veidemann.api.config.v1.Seed;
import no.nb.nna.veidemann.api.contentwriter.v1.ContentWriterGrpc;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.RethinkDbAdapter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

public abstract class CrawlTestBase {
    static ManagedChannel contentWriterChannel;

    static ManagedChannel controllerChannel;

    static ConfigGrpc.ConfigBlockingStub configClient;

    static ControllerGrpc.ControllerBlockingStub controllerClient;

    static StatusGrpc.StatusBlockingStub statusClient;

    static ReportGrpc.ReportStub reportClient;

    static ContentWriterGrpc.ContentWriterBlockingStub contentWriterClient;

    static RethinkDbAdapter db;

    static RethinkDB r = RethinkDB.r;

    ConfigObject browserConfig;
    ConfigObject politeness;
    ConfigObject collection;
    ConfigObject crawlConfig;
    ConfigObject job;

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

        configClient = ConfigGrpc.newBlockingStub(controllerChannel).withWaitForReady();

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
    public static void shutdown() throws InterruptedException {
        System.out.println("Shutting down...");
        controllerChannel.shutdown();
        contentWriterChannel.shutdown();
        System.out.println("Avaiting termination of controller channel");
        controllerChannel.awaitTermination(1, TimeUnit.MINUTES);
        System.out.println("Avaiting termination of content writer channel");
        contentWriterChannel.awaitTermination(1, TimeUnit.MINUTES);
        DbService.getInstance().close();
        System.out.println("Shut down");
    }

    @After
    public void cleanup() throws DbException {
    }

    public ConfigObject createJob(String name, int depth, long maxDurationS, long maxBytes) {
        // Update browser config with shorter inactivity time
        browserConfig = configClient.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.browserConfig)
                .build())
                .next();
        ConfigObject.Builder browserConfigBuilder = browserConfig.toBuilder();
        browserConfigBuilder.getBrowserConfigBuilder().setMaxInactivityTimeMs(500);
        browserConfig = configClient.saveConfigObject(browserConfigBuilder.build());

        // Create politeness which is not very polite
        ConfigObject.Builder politenessBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.politenessConfig);
        politenessBuilder.getMetaBuilder().setName(name);
        politenessBuilder.getPolitenessConfigBuilder()
                .setMaxTimeBetweenPageLoadMs(100)
                .setMinTimeBetweenPageLoadMs(1)
                .setDelayFactor(.01f)
                .setRetryDelaySeconds(1)
                .setUseHostname(true);
        politeness = configClient.saveConfigObject(politenessBuilder.build());

        // Create a collection for this test
        ConfigObject.Builder collectionBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.collection);
        collectionBuilder.getMetaBuilder().setName(name).setDescription("Collection for " + name);
        collectionBuilder.getCollectionBuilder()
                .setCompress(true)
                .setFileSize(1024 * 1024 * 100);
        collectionBuilder.getCollectionBuilder().addSubCollectionsBuilder().setName("screenshot").setType(SubCollectionType.SCREENSHOT);
        collectionBuilder.getCollectionBuilder().addSubCollectionsBuilder().setName("dns").setType(SubCollectionType.DNS);
        collection = configClient.saveConfigObject(collectionBuilder.build());

        // Create a crawl config which references the above configs
        ConfigObject.Builder crawlConfigBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlConfig);
        crawlConfigBuilder.getMetaBuilder().setName(name);
        crawlConfigBuilder.getCrawlConfigBuilder()
                .setCollectionRef(ApiTools.refForConfig(collection))
                .setBrowserConfigRef(ApiTools.refForConfig(browserConfig))
                .setPolitenessRef(ApiTools.refForConfig(politeness))
                .getExtraBuilder().setCreateScreenshot(true).setExtractText(true);
        crawlConfig = configClient.saveConfigObject(crawlConfigBuilder.build());

        // Create a job with the specified limits
        ConfigObject.Builder jobBuilder = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlJob);
        jobBuilder.getMetaBuilder().setName(name);
        jobBuilder.getCrawlJobBuilder()
                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig))
                .getLimitsBuilder()
                .setDepth(depth)
                .setMaxDurationS(maxDurationS)
                .setMaxBytes(maxBytes);
        job = configClient.saveConfigObject(jobBuilder.build());

        return job;
    }

    ConfigObject createEntity(String name) {
        return configClient.saveConfigObject(
                ConfigObject.newBuilder()
                        .setApiVersion("v1")
                        .setKind(Kind.crawlEntity)
                        .setMeta(Meta.newBuilder().setName(name))
                        .build());
    }

    ConfigObject createSeed(String uri, ConfigObject entity, String jobId) {
        return configClient.saveConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.seed)
                .setMeta(Meta.newBuilder().setName(uri))
                .setSeed(Seed.newBuilder()
                        .setEntityRef(ApiTools.refForConfig(entity))
                        .addJobRef(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId(jobId)))
                .build());
    }

    void listSeeds() {
        configClient.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .build()).forEachRemaining(s -> System.out.println("Seed: " + s.getId() + " :: " + s));
    }
}
