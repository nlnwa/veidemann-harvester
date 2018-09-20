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
package no.nb.nna.veidemann.db;

import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.Label;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels.LogLevel;
import no.nb.nna.veidemann.api.ConfigProto.Meta;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for RethinkDbAdapter.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbAdapterIT {

    public static RethinkDbConfigAdapter configAdapter;
    public static RethinkDbAdapter dbAdapter;

    static RethinkDB r = RethinkDB.r;

    public RethinkDbAdapterIT() {
    }

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();

        dbAdapter = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();
        configAdapter = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @Before
    public void cleanDb() throws DbException {
        for (Tables table : Tables.values()) {
            if (table != Tables.SYSTEM) {
                dbAdapter.executeRequest("delete", r.table(table.name).delete());
            }
        }
    }

    /**
     * Test of saveCrawlEntity method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveCrawlEntity() throws InvalidProtocolBufferException, DbException {
        CrawlEntity entity = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")))
                        .setCreatedBy("anonymous"))
                .build();

        OffsetDateTime start = OffsetDateTime.now();
        CrawlEntity result = configAdapter.saveCrawlEntity(entity);

        assertThat(result.getId()).isNotEmpty();
        assertThat(result.getMeta().getName()).isEqualTo("Nasjonalbiblioteket");
        assertThat(result.getMeta().getCreated()).isEqualTo(
                ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")));
        assertThat(result.getMeta().getCreatedBy()).isEqualTo("anonymous");
        assertThat(result.getMeta().getLastModified()).isNotNull();
        assertThat(ProtoUtils.tsToOdt(result.getMeta().getLastModified()))
                .isAfterOrEqualTo(start)
                .isBeforeOrEqualTo(OffsetDateTime.now());
        assertThat(result.getMeta().getLastModifiedBy()).isEqualTo("anonymous");
        assertThat(result.getMeta().getLabelList()).containsOnly(
                Label.newBuilder()
                        .setKey("frequency")
                        .setValue("Daily").build(),
                Label.newBuilder()
                        .setKey("orgType")
                        .setValue("Government").build());

        // Override
        CrawlEntity override = CrawlEntity.newBuilder()
                .setId(result.getId())
                .build();

        CrawlEntity overrideResult = configAdapter.saveCrawlEntity(override);

        assertThat(overrideResult.getId()).isEqualTo(result.getId());
        assertThat(overrideResult.getMeta().getName()).isEqualTo("Nasjonalbiblioteket");
        assertThat(overrideResult.getMeta().getCreated()).isEqualTo(
                ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")));
        assertThat(overrideResult.getMeta().getCreatedBy()).isEqualTo("anonymous");
        assertThat(overrideResult.getMeta().getLastModified()).isNotNull();
        assertThat(overrideResult.getMeta().getLastModified()).isNotEqualTo(result.getMeta().getLastModified());
        assertThat(ProtoUtils.tsToOdt(overrideResult.getMeta().getLastModified()))
                .isAfterOrEqualTo(ProtoUtils.tsToOdt(result.getMeta().getLastModified()))
                .isBeforeOrEqualTo(OffsetDateTime.now());
        assertThat(overrideResult.getMeta().getLastModifiedBy()).isEqualTo("anonymous");
        assertThat(overrideResult.getMeta().getLabelList()).isEmpty();

        // override
        override = CrawlEntity.newBuilder()
                .setId(result.getId())
                .setMeta(Meta.newBuilder()
                        .setName("Foo")
                        .setCreated(ProtoUtils.getNowTs())
                        .setCreatedBy("Bar")
                        .setLastModified(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")))
                        .setLastModifiedBy("person")
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Media")))
                .build();

        start = OffsetDateTime.now();
        overrideResult = configAdapter.saveCrawlEntity(override);

        assertThat(overrideResult.getId()).isEqualTo(result.getId());
        assertThat(overrideResult.getMeta().getName()).isEqualTo("Foo");
        assertThat(overrideResult.getMeta().getCreated()).isEqualTo(
                ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")));
        assertThat(overrideResult.getMeta().getCreatedBy()).isEqualTo("anonymous");
        assertThat(overrideResult.getMeta().getLastModified()).isNotNull();
        assertThat(overrideResult.getMeta().getLastModified()).isNotEqualTo(result.getMeta().getLastModified());
        assertThat(ProtoUtils.tsToOdt(overrideResult.getMeta().getLastModified()))
                .isAfterOrEqualTo(start)
                .isBeforeOrEqualTo(OffsetDateTime.now());
        assertThat(overrideResult.getMeta().getLastModifiedBy()).isEqualTo("person");
        assertThat(overrideResult.getMeta().getLabelList()).containsOnly(
                Label.newBuilder()
                        .setKey("orgType")
                        .setValue("Media").build());

    }

    /**
     * Test of listCrawlEntities method, of class RethinkDbAdapter.
     */
    @Test
    public void testListCrawlEntities() throws InvalidProtocolBufferException, DbException {
        Label freqDaily = Label.newBuilder().setKey("frequency").setValue("Daily").build();
        Label freqHourly = Label.newBuilder().setKey("frequency").setValue("Hourly").build();
        Label orgCulture = Label.newBuilder().setKey("orgType").setValue("Culture").build();
        Label orgNews = Label.newBuilder().setKey("orgType").setValue("News").build();
        Label orgGovernment = Label.newBuilder().setKey("orgType").setValue("Government").build();
        Label priHigh = Label.newBuilder().setKey("priority").setValue("high").build();
        Label priLow = Label.newBuilder().setKey("priority").setValue("low").build();
        Label fooHigh = Label.newBuilder().setKey("foo").setValue("high").build();
        Label fooLow = Label.newBuilder().setKey("foo").setValue("low").build();
        Label fooLower = Label.newBuilder().setKey("foo").setValue("lower").build();
        Label fooLowest = Label.newBuilder().setKey("foo").setValue("lowest").build();

        String freqHourlySelector = "frequency:Hourly";
        String orgNewsSelector = "orgType:News";
        String fooLowTruncSelector = "foo:low*";
        String anyHighSelector = ":high";
        String anyLowTruncSelector = ":low*";

        CrawlEntity entity1 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(freqDaily)
                        .addLabel(orgGovernment)
                        .addLabel(orgCulture)
                        .addLabel(fooHigh)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity1 = configAdapter.saveCrawlEntity(entity1);

        CrawlEntity entity2 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("VG")
                        .addLabel(freqHourly)
                        .addLabel(orgNews)
                        .addLabel(fooLower)
                        .addLabel(priHigh)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity2 = configAdapter.saveCrawlEntity(entity2);

        CrawlEntity entity3 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalballetten")
                        .addLabel(freqHourly)
                        .addLabel(orgCulture)
                        .addLabel(fooLowest)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity3 = configAdapter.saveCrawlEntity(entity3);

        ListRequest request = ListRequest.getDefaultInstance();
        CrawlEntityListReply result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(3);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity1, entity2, entity3);

        GetRequest getId = GetRequest.newBuilder().setId(entity1.getId()).build();
        CrawlEntity resultEntity = configAdapter.getCrawlEntity(getId);
        assertThat(resultEntity).isEqualTo(entity1);

        request = ListRequest.newBuilder().setName("^nasj.*").build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity3);

        request = ListRequest.newBuilder().setName(".*biblioteket$").build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity1);

        request = ListRequest.newBuilder().setName(".*ball.*").build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity3);

        request = ListRequest.newBuilder().setPageSize(2).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity3, entity1);

        request = ListRequest.newBuilder().setPageSize(2).setPage(1).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity2);

        // Select on label
        request = ListRequest.newBuilder().addLabelSelector(freqHourlySelector).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);

        request = ListRequest.newBuilder().addLabelSelector(freqHourlySelector).addLabelSelector(orgNewsSelector).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity2);

        request = ListRequest.newBuilder().addLabelSelector(fooLowTruncSelector).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);

        request = ListRequest.newBuilder().addLabelSelector(anyHighSelector).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity2);

        request = ListRequest.newBuilder().addLabelSelector(anyLowTruncSelector).build();
        result = configAdapter.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);
    }

    /**
     * Test of deleteCrawlEntity method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlEntity() throws DbException {
        CrawlEntity entity1 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Culture"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity1 = configAdapter.saveCrawlEntity(entity1);

        CrawlEntity entity2 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("VG")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Hourly"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("News"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity2 = configAdapter.saveCrawlEntity(entity2);

        CrawlEntityListReply result = configAdapter.listCrawlEntities(ListRequest.getDefaultInstance());
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity2);

        configAdapter.deleteCrawlEntity(entity2);

        result = configAdapter.listCrawlEntities(ListRequest.getDefaultInstance());
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity1);
        assertThat(result.getValueList()).doesNotContain(entity2);
    }

    /**
     * Test of isDuplicateContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testhasCrawledContent() throws DbException {
        CrawledContent cc1 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id1")
                .build();
        CrawledContent cc2 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id2")
                .build();
        CrawledContent cc3 = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id3")
                .build();

        assertThat(dbAdapter.hasCrawledContent(cc1).isPresent()).isFalse();
        dbAdapter.saveCrawlLog(CrawlLog.newBuilder()
                .setWarcId(cc1.getWarcId())
                .setJobExecutionId("jeid")
                .setExecutionId("ceid")
                .build());

        Optional<CrawledContent> r2 = dbAdapter.hasCrawledContent(cc2);
        assertThat(r2.isPresent()).isTrue();
        assertThat(r2.get()).isEqualTo(cc1);

        Optional<CrawledContent> r3 = dbAdapter.hasCrawledContent(cc3);
        assertThat(r3.isPresent()).isTrue();
        assertThat(r3.get()).isEqualTo(cc1);

        CrawledContent cc4 = CrawledContent.newBuilder()
                .setWarcId("warc-id4")
                .build();

        assertThatThrownBy(() -> dbAdapter.hasCrawledContent(cc4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'digest' is missing from: 'CrawledContent");
    }

    /**
     * Test of deleteCrawledContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawledContent() throws DbException {
        CrawledContent cc = CrawledContent.newBuilder()
                .setDigest("testDeleteCrawledContent")
                .setWarcId("warc-id")
                .build();

        dbAdapter.hasCrawledContent(cc);
        dbAdapter.deleteCrawledContent(cc.getDigest());
        dbAdapter.deleteCrawledContent(cc.getDigest());
    }

    /**
     * Test of addExtractedText method, of class RethinkDbAdapter.
     */
    @Test
    public void testAddExtractedText() throws DbException {
        ExtractedText et1 = ExtractedText.newBuilder()
                .setWarcId("testAddExtractedText")
                .setText("text")
                .build();

        ExtractedText result1 = dbAdapter.addExtractedText(et1);
        assertThat(result1).isEqualTo(et1);

        assertThatThrownBy(() -> dbAdapter.addExtractedText(et1))
                .isInstanceOf(DbException.class)
                .hasMessageContaining("Duplicate primary key");

        ExtractedText et2 = ExtractedText.newBuilder()
                .setText("text")
                .build();

        assertThatThrownBy(() -> dbAdapter.addExtractedText(et2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'warc_id' is missing from: 'ExtractedText");
    }

    /**
     * Test of addCrawlLog method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveCrawlLog() throws DbException {
        CrawlLog cl = CrawlLog.newBuilder()
                .setContentType("text/plain")
                .setJobExecutionId("jeid")
                .setExecutionId("eid")
                .build();
        CrawlLog result = dbAdapter.saveCrawlLog(cl);
        assertThat(result.getContentType()).isEqualTo("text/plain");
        assertThat(result.getWarcId()).isNotEmpty();
    }

    /**
     * Test of saveBrowserScript method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveBrowserScript() throws DbException {
        BrowserScript script = BrowserScript.newBuilder()
                .setMeta(ApiTools.buildMeta("test.js", "description", ApiTools.buildLabel("type", "login")))
                .setScript("code")
                .build();

        BrowserScript result = configAdapter.saveBrowserScript(script);
        assertThat(result.getId()).isNotEmpty();
        assertThat(result.getScript()).isEqualTo("code");
        assertThat(ApiTools.hasLabel(result.getMeta(), ApiTools.buildLabel("type", "login"))).isTrue();
    }

    @Test
    public void testSaveListAndDeleteRoleMapping() throws DbException {
        RoleMapping rm = RoleMapping.newBuilder()
                .setEmail("test")
                .addRole(Role.ADMIN).build();

        RoleMapping result = configAdapter.saveRoleMapping(rm);
        assertThat(result.getId()).isNotEmpty();
        assertThat(result.getEmail()).isEqualTo("test");
        assertThat(result.getRoleList()).containsExactly(Role.ADMIN);

        RoleMappingsListReply resultList = configAdapter.listRoleMappings(RoleMappingsListRequest.getDefaultInstance());
        assertThat(resultList.getValueCount()).isEqualTo((int) resultList.getCount()).isEqualTo(1);
        assertThat(resultList.getValue(0)).isEqualTo(result);

        resultList = configAdapter.listRoleMappings(RoleMappingsListRequest.newBuilder().setId(result.getId()).build());
        assertThat(resultList.getValueCount()).isEqualTo((int) resultList.getCount()).isEqualTo(1);
        assertThat(resultList.getValue(0)).isEqualTo(result);

        configAdapter.deleteRoleMapping(result);
        resultList = configAdapter.listRoleMappings(RoleMappingsListRequest.getDefaultInstance());
        assertThat(resultList.getValueCount()).isEqualTo((int) resultList.getCount()).isEqualTo(0);
    }

    /**
     * Test of getBrowserScripts method, of class RethinkDbAdapter.
     */
    @Test
    public void testListBrowserScripts() throws DbException {
        BrowserScript script1 = BrowserScript.newBuilder()
                .setMeta(ApiTools.buildMeta("test.js", "description", ApiTools.buildLabel("type", "login")))
                .setScript("code")
                .build();
        BrowserScript script2 = BrowserScript.newBuilder()
                .setMeta(ApiTools.buildMeta("extract-outlinks.js", "description",
                        ApiTools.buildLabel("type", "extract_outlinks")))
                .setScript("code")
                .build();
        script1 = configAdapter.saveBrowserScript(script1);
        script2 = configAdapter.saveBrowserScript(script2);

        ListRequest request = ListRequest.getDefaultInstance();
        BrowserScriptListReply result = configAdapter.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).containsExactly(script2, script1);

        request = ListRequest.newBuilder().addLabelSelector("type:extract_outlinks").build();
        result = configAdapter.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).containsExactly(script2);

        request = ListRequest.newBuilder().addLabelSelector("type:behavior").build();
        result = configAdapter.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(0);
        assertThat(result.getCount()).isEqualTo(0);

        request = ListRequest.newBuilder().setName("extr.*").build();
        result = configAdapter.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).containsExactly(script2);
    }

    /**
     * Test of addScreenshot method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testAddScreenshot() throws DbException {
        System.out.println("addScreenshot");
        Screenshot s = null;
        RethinkDbAdapter instance = null;
        Screenshot expResult = null;
        Screenshot result = instance.saveScreenshot(s);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of listSeeds method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveAndListSeeds() throws DbException {
        CrawlEntity entity1 = configAdapter.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build());

        CrawlEntity entity2 = configAdapter.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Foo")
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build());

        Seed seed1 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed1.foo"))
                .addJobId("job1")
                .setEntityId(entity1.getId())
                .build();
        Seed savedSeed1 = configAdapter.saveSeed(seed1);

        Seed seed2 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed2.foo"))
                .addJobId("job1")
                .addJobId("job2")
                .setEntityId(entity2.getId())
                .build();
        Seed savedSeed2 = configAdapter.saveSeed(seed2);

        Seed seed3 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed3.foo"))
                .addJobId("job2")
                .setEntityId(entity1.getId())
                .build();
        Seed savedSeed3 = configAdapter.saveSeed(seed3);

        SeedListRequest request = SeedListRequest.newBuilder().setCrawlJobId("job1").build();
        SeedListReply result = configAdapter.listSeeds(request);
        assertThat(result.getValueList()).containsOnly(savedSeed1, savedSeed2);

        request = SeedListRequest.newBuilder().setEntityId(entity1.getId()).build();
        result = configAdapter.listSeeds(request);
        assertThat(result.getValueList()).containsOnly(savedSeed1, savedSeed3);

        request = SeedListRequest.newBuilder().setEntityId(entity2.getId()).build();
        result = configAdapter.listSeeds(request);
        assertThat(result.getValueList()).containsOnly(savedSeed2);
    }

    /**
     * Test of deleteSeed method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testDeleteSeed() throws DbException {
        System.out.println("deleteSeed");
        ConfigProto.Seed seed = null;
        RethinkDbConfigAdapter instance = null;
        Empty expResult = null;
        Empty result = instance.deleteSeed(seed);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of saveCrawlJob method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveAndListCrawlJob() throws DbException {
        CrawlScheduleConfig schedule = configAdapter.saveCrawlScheduleConfig(ConfigProto.CrawlScheduleConfig.newBuilder()
                .setCronExpression("* * * * *")
                .setMeta(Meta.newBuilder().setName("Every minute")).build());

        BrowserConfig browserConfig = configAdapter.saveBrowserConfig(ConfigProto.BrowserConfig.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test browser config"))
                .setWindowWidth(100).build());

        PolitenessConfig politenessConfig = configAdapter.savePolitenessConfig(ConfigProto.PolitenessConfig.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test politeness config"))
                .setMinTimeBetweenPageLoadMs(500).build());

        CrawlConfig crawlConfig = configAdapter.saveCrawlConfig(ConfigProto.CrawlConfig.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test crawl config"))
                .setBrowserConfigId(browserConfig.getId())
                .setPolitenessId(politenessConfig.getId()).build());

        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setScheduleId(schedule.getId())
                .setCrawlConfigId(crawlConfig.getId())
                .build();

        CrawlJob result = configAdapter.saveCrawlJob(crawlJob);
        assertThat(result.getId()).isNotEmpty();

        assertThat(configAdapter.listCrawlJobs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test job");
                    assertThat(r.getValue(0).getScheduleId())
                            .isEqualTo(schedule.getId());
                });

        assertThat(configAdapter.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(result.getScheduleId());
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Every minute");
                    assertThat(r.getValue(0).getCronExpression()).isEqualTo("* * * * *");
                });

        Map<String, String> id = new HashMap<>();
        assertThat(configAdapter.listCrawlConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(result.getCrawlConfigId());
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test crawl config");
                    id.put("browserConfig", r.getValue(0).getBrowserConfigId());
                    id.put("politenessConfig", r.getValue(0).getPolitenessId());
                });

        assertThat(configAdapter.listBrowserConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(id.get("browserConfig"));
                    assertThat(r.getValue(0).getWindowWidth()).isEqualTo(100);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test browser config");
                });

        assertThat(configAdapter.listPolitenessConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(id.get("politenessConfig"));
                    assertThat(r.getValue(0).getMinTimeBetweenPageLoadMs()).isEqualTo(500);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test politeness config");
                });

        CrawlJob badCrawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .build();

        assertThatThrownBy(() -> configAdapter.saveCrawlJob(badCrawlJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A crawl config is required for crawl jobs");

    }

    /**
     * Test of deleteCrawlJob method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlJob() throws DbException {
        CrawlConfig crawlConfig = configAdapter.saveCrawlConfig(CrawlConfig.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test crawl config")).build());

        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setCrawlConfigId(crawlConfig.getId())
                .build();

        CrawlJob result = configAdapter.saveCrawlJob(crawlJob);
        assertThat(configAdapter.listCrawlJobs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        configAdapter.deleteCrawlJob(crawlJob);
        assertThat(configAdapter.listCrawlJobs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        configAdapter.deleteCrawlJob(result);
        assertThat(configAdapter.listCrawlJobs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(0);

        CrawlJob toBeDeleted = configAdapter.saveCrawlJob(crawlJob);
        Seed seed = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test seed"))
                .addJobId(toBeDeleted.getId())
                .build();

        seed = configAdapter.saveSeed(seed);

        assertThatThrownBy(() -> configAdapter.deleteCrawlJob(toBeDeleted))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Can't delete CrawlJob, there are 1 Seed(s) referring it");

        configAdapter.deleteSeed(seed);
        configAdapter.deleteCrawlJob(toBeDeleted);
    }

    /**
     * Test of listCrawlConfigs method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testListCrawlConfigs() throws DbException {
        System.out.println("listCrawlConfigs");
        ListRequest request = null;
        RethinkDbConfigAdapter instance = null;
        ControllerProto.CrawlConfigListReply expResult = null;
        ControllerProto.CrawlConfigListReply result = instance.listCrawlConfigs(request);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of saveCrawlConfig method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testSaveCrawlConfig() throws DbException {
        System.out.println("saveCrawlConfig");
        ConfigProto.CrawlConfig crawlConfig = null;
        RethinkDbConfigAdapter instance = null;
        ConfigProto.CrawlConfig expResult = null;
        ConfigProto.CrawlConfig result = instance.saveCrawlConfig(crawlConfig);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteCrawlConfig method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testDeleteCrawlConfig() throws DbException {
        System.out.println("deleteCrawlConfig");
        ConfigProto.CrawlConfig crawlConfig = null;
        RethinkDbConfigAdapter instance = null;
        Empty expResult = null;
        Empty result = instance.deleteCrawlConfig(crawlConfig);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of listCrawlScheduleConfigs method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testListCrawlScheduleConfigs() throws DbException {
        System.out.println("listCrawlScheduleConfigs");
        ListRequest request = null;
        RethinkDbConfigAdapter instance = null;
        ControllerProto.CrawlScheduleConfigListReply expResult = null;
        ControllerProto.CrawlScheduleConfigListReply result = instance.listCrawlScheduleConfigs(request);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of saveCrawlScheduleConfig method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testSaveCrawlScheduleConfig() throws DbException {
        System.out.println("saveCrawlScheduleConfig");
        ConfigProto.CrawlScheduleConfig crawlScheduleConfig = null;
        RethinkDbConfigAdapter instance = null;
        CrawlScheduleConfig expResult = null;
        ConfigProto.CrawlScheduleConfig result = instance.saveCrawlScheduleConfig(crawlScheduleConfig);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteCrawlScheduleConfig method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlScheduleConfig() throws DbException {
        CrawlScheduleConfig scheduleConfig = CrawlScheduleConfig.newBuilder()
                .setCronExpression("* * * * *")
                .setMeta(Meta.newBuilder().setName("Every minute"))
                .build();

        CrawlScheduleConfig result = configAdapter.saveCrawlScheduleConfig(scheduleConfig);
        assertThat(configAdapter.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        configAdapter.deleteCrawlScheduleConfig(scheduleConfig);
        assertThat(configAdapter.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        configAdapter.deleteCrawlScheduleConfig(result);
        assertThat(configAdapter.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(0);

        CrawlScheduleConfig toBeDeleted = configAdapter.saveCrawlScheduleConfig(scheduleConfig);

        CrawlConfig crawlConfig = configAdapter.saveCrawlConfig(CrawlConfig.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test crawl config")).build());

        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setScheduleId(toBeDeleted.getId())
                .setCrawlConfigId(crawlConfig.getId())
                .build();

        crawlJob = configAdapter.saveCrawlJob(crawlJob);
        assertThat(configAdapter.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);

        assertThatThrownBy(() -> configAdapter.deleteCrawlScheduleConfig(toBeDeleted))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Can't delete CrawlScheduleConfig, there are 1 CrawlJob(s) referring it");

        configAdapter.deleteCrawlJob(crawlJob);
        configAdapter.deleteCrawlScheduleConfig(toBeDeleted);
    }

    @Test
    public void testSaveAndGetLogConfig() throws DbException {
        LogLevel l1 = LogLevel.newBuilder().setLogger("no.nb.nna").setLevel(LogLevels.Level.INFO).build();
        LogLevel l2 = LogLevel.newBuilder().setLogger("org.apache").setLevel(LogLevels.Level.FATAL).build();
        LogLevels logLevels = LogLevels.newBuilder().addLogLevel(l1).addLogLevel(l2).build();
        LogLevels response;

        response = configAdapter.saveLogConfig(logLevels);
        assertThat(response).isEqualTo(logLevels);

        response = configAdapter.getLogConfig();
        assertThat(response).isEqualTo(logLevels);
    }

    @Test
    public void testPaused() throws DbException {
        assertThat(dbAdapter.getDesiredPausedState()).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        assertThat(dbAdapter.setDesiredPausedState(true)).isFalse();

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();

        assertThat(dbAdapter.setDesiredPausedState(true)).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();

        assertThat(dbAdapter.setDesiredPausedState(false)).isTrue();
        assertThat(dbAdapter.isPaused()).isFalse();

        CrawlHostGroup chg = CrawlHostGroup.newBuilder().setId("chg").setBusy(true).build();
        configAdapter.saveMessage(chg, Tables.CRAWL_HOST_GROUP);

        assertThat(dbAdapter.getDesiredPausedState()).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        assertThat(dbAdapter.setDesiredPausedState(true)).isFalse();
        assertThat(dbAdapter.isPaused()).isFalse();

        chg = chg.toBuilder().setBusy(false).build();
        configAdapter.saveMessage(chg, Tables.CRAWL_HOST_GROUP);

        assertThat(dbAdapter.getDesiredPausedState()).isTrue();
        assertThat(dbAdapter.isPaused()).isTrue();
    }
}
