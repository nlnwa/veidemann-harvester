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
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rethinkdb.RethinkDB;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.SeedListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.Label;
import no.nb.nna.broprox.model.ConfigProto.Meta;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.ConfigProto.Selector;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

    @Before
    public void cleanDb() {
        RethinkDB r = RethinkDB.r;
        for (RethinkDbAdapter.TABLES table : RethinkDbAdapter.TABLES.values()) {
            if (table != RethinkDbAdapter.TABLES.SYSTEM) {
                db.executeRequest(r.table(table.name).delete());
            }
        }
    }

    /**
     * Test of saveCrawlEntity method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveCrawlEntity() throws InvalidProtocolBufferException {
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
        CrawlEntity result = db.saveCrawlEntity(entity);

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

        start = OffsetDateTime.now();
        CrawlEntity overrideResult = db.saveCrawlEntity(override);

        assertThat(overrideResult.getId()).isEqualTo(result.getId());
        assertThat(overrideResult.getMeta().getName()).isEqualTo("Nasjonalbiblioteket");
        assertThat(overrideResult.getMeta().getCreated()).isEqualTo(
                ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")));
        assertThat(overrideResult.getMeta().getCreatedBy()).isEqualTo("anonymous");
        assertThat(overrideResult.getMeta().getLastModified()).isNotNull();
        assertThat(overrideResult.getMeta().getLastModified()).isNotEqualTo(result.getMeta().getLastModified());
        assertThat(ProtoUtils.tsToOdt(overrideResult.getMeta().getLastModified()))
                .isAfterOrEqualTo(start)
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
        overrideResult = db.saveCrawlEntity(override);

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
    public void testListCrawlEntities() throws InvalidProtocolBufferException {
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

        CrawlEntity entity1 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(freqDaily)
                        .addLabel(orgGovernment)
                        .addLabel(orgCulture)
                        .addLabel(fooHigh)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity1 = db.saveCrawlEntity(entity1);

        CrawlEntity entity2 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("VG")
                        .addLabel(freqHourly)
                        .addLabel(orgNews)
                        .addLabel(fooLower)
                        .addLabel(priHigh)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity2 = db.saveCrawlEntity(entity2);

        CrawlEntity entity3 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalballetten")
                        .addLabel(freqHourly)
                        .addLabel(orgCulture)
                        .addLabel(fooLowest)
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity3 = db.saveCrawlEntity(entity3);

        ListRequest request = ListRequest.getDefaultInstance();
        CrawlEntityListReply result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(3);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity1, entity2, entity3);

        request = ListRequest.newBuilder().setId(entity1.getId()).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity1);

        request = ListRequest.newBuilder().setNamePrefix("nasj").build();
        result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity3);

        request = ListRequest.newBuilder().setPageSize(2).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity3, entity1);

        request = ListRequest.newBuilder().setPageSize(2).setPage(1).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(3);
        assertThat(result.getValueList()).contains(entity2);

        // Select on label
        request = ListRequest.newBuilder().setSelector(Selector.newBuilder().addLabel(freqHourly)).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);

        request = ListRequest.newBuilder().setSelector(
                Selector.newBuilder().addLabel(freqHourly).addLabel(orgNews)).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity2);

        Label fooLowTrunc = Label.newBuilder().setKey("foo").setValue("low*").build();
        request = ListRequest.newBuilder().setSelector(Selector.newBuilder().addLabel(fooLowTrunc)).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);

        Label anyHigh = Label.newBuilder().setValue("high").build();
        request = ListRequest.newBuilder().setSelector(Selector.newBuilder().addLabel(anyHigh)).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity2);

        Label anyLowTrunc = Label.newBuilder().setValue("low*").build();
        request = ListRequest.newBuilder().setSelector(Selector.newBuilder().addLabel(anyLowTrunc)).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity2, entity3);
    }

    /**
     * Test of deleteCrawlEntity method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlEntity() {
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
        entity1 = db.saveCrawlEntity(entity1);

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
        entity2 = db.saveCrawlEntity(entity2);

        CrawlEntityListReply result = db.listCrawlEntities(ListRequest.getDefaultInstance());
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity2);

        db.deleteCrawlEntity(entity2);

        result = db.listCrawlEntities(ListRequest.getDefaultInstance());
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getValueList()).contains(entity1);
        assertThat(result.getValueList()).doesNotContain(entity2);
    }

    /**
     * Test of isDuplicateContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testIsDuplicateContent() {
        CrawledContent cc = CrawledContent.newBuilder()
                .setDigest("testIsDuplicateContent")
                .setWarcId("warc-id")
                .build();

        assertThat(db.isDuplicateContent(cc.getDigest()).isPresent()).isFalse();
        db.addCrawledContent(cc);
        assertThat(db.isDuplicateContent(cc.getDigest()).isPresent()).isTrue();
    }

    /**
     * Test of deleteCrawledContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawledContent() {
        CrawledContent cc = CrawledContent.newBuilder()
                .setDigest("testDeleteCrawledContent")
                .setWarcId("warc-id")
                .build();

        db.addCrawledContent(cc);
        db.deleteCrawledContent(cc.getDigest());
        db.deleteCrawledContent(cc.getDigest());
    }

    /**
     * Test of addCrawledContent method, of class RethinkDbAdapter.
     */
    @Test
    public void testAddCrawledContent() {
        CrawledContent cc1 = CrawledContent.newBuilder()
                .setDigest("testAddCrawledContent")
                .setWarcId("warc-id")
                .build();

        CrawledContent result1 = db.addCrawledContent(cc1);
        assertThat(result1).isEqualTo(cc1);

        assertThatThrownBy(() -> db.addCrawledContent(cc1))
                .isInstanceOf(DbException.class)
                .hasMessageContaining("Duplicate primary key");

        CrawledContent cc2 = CrawledContent.newBuilder()
                .setWarcId("warc-id")
                .build();

        assertThatThrownBy(() -> db.addCrawledContent(cc2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'digest' is missing from: 'CrawledContent");
    }

    /**
     * Test of addExtractedText method, of class RethinkDbAdapter.
     */
    @Test
    public void testAddExtractedText() {
        ExtractedText et1 = ExtractedText.newBuilder()
                .setWarcId("testAddExtractedText")
                .setText("text")
                .build();

        ExtractedText result1 = db.addExtractedText(et1);
        assertThat(result1).isEqualTo(et1);

        assertThatThrownBy(() -> db.addExtractedText(et1))
                .isInstanceOf(DbException.class)
                .hasMessageContaining("Duplicate primary key");

        ExtractedText et2 = ExtractedText.newBuilder()
                .setText("text")
                .build();

        assertThatThrownBy(() -> db.addExtractedText(et2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The required field 'warc_id' is missing from: 'ExtractedText");
    }

    /**
     * Test of addCrawlLog method, of class RethinkDbAdapter.
     */
    @Test
    public void testAddCrawlLog() {
        CrawlLog cl = CrawlLog.newBuilder()
                .setContentType("text/plain")
                .build();
        CrawlLog result = db.addCrawlLog(cl);
        assertThat(result.getContentType()).isEqualTo("text/plain");
        assertThat(result.getWarcId()).isNotEmpty();
    }

    /**
     * Test of updateCrawlLog method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testUpdateCrawlLog() {
        System.out.println("updateCrawlLog");
        CrawlLog cl = null;
        RethinkDbAdapter instance = null;
        CrawlLog expResult = null;
        CrawlLog result = instance.updateCrawlLog(cl);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of saveBrowserScript method, of class RethinkDbAdapter.
     */
    @Test
    public void testSaveBrowserScript() {
        BrowserScript script = BrowserScript.newBuilder()
                .setMeta(Meta.newBuilder().setName("test.js"))
                .setScript("code")
                .setType(BrowserScript.Type.LOGIN)
                .build();

        BrowserScript result = db.saveBrowserScript(script);
        assertThat(result.getId()).isNotEmpty();
        assertThat(result.getScript()).isEqualTo("code");
        assertThat(result.getType()).isSameAs(BrowserScript.Type.LOGIN);
    }

    /**
     * Test of getBrowserScripts method, of class RethinkDbAdapter.
     */
    @Test
    public void testListBrowserScripts() {
        BrowserScript script1 = BrowserScript.newBuilder()
                .setMeta(Meta.newBuilder().setName("test.js"))
                .setScript("code")
                .setType(BrowserScript.Type.LOGIN)
                .build();
        BrowserScript script2 = BrowserScript.newBuilder()
                .setMeta(Meta.newBuilder().setName("extract-outlinks.js"))
                .setScript("code")
                .setType(BrowserScript.Type.EXTRACT_OUTLINKS)
                .build();
        script1 = db.saveBrowserScript(script1);
        script2 = db.saveBrowserScript(script2);

        BrowserScriptListRequest request = BrowserScriptListRequest.getDefaultInstance();
        BrowserScriptListReply result = db.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(2);
        assertThat(result.getCount()).isEqualTo(2);
        assertThat(result.getValueList()).containsExactly(script2, script1);

        request = BrowserScriptListRequest.newBuilder().setType(BrowserScript.Type.EXTRACT_OUTLINKS).build();
        result = db.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).containsExactly(script2);

        request = BrowserScriptListRequest.newBuilder().setType(BrowserScript.Type.BEHAVIOR).build();
        result = db.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(0);
        assertThat(result.getCount()).isEqualTo(0);

        request = BrowserScriptListRequest.newBuilder().setNamePrefix("extr").build();
        result = db.listBrowserScripts(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isEqualTo(1);
        assertThat(result.getValueList()).containsExactly(script2);
    }

    /**
     * Test of addExecutionStatus method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testAddExecutionStatus() {
        System.out.println("addExecutionStatus");
        CrawlExecutionStatus status = null;
        RethinkDbAdapter instance = null;
        CrawlExecutionStatus expResult = null;
        CrawlExecutionStatus result = instance.addExecutionStatus(status);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of updateExecutionStatus method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testUpdateExecutionStatus() {
        System.out.println("updateExecutionStatus");
        CrawlExecutionStatus status = null;
        RethinkDbAdapter instance = null;
        CrawlExecutionStatus expResult = null;
        CrawlExecutionStatus result = instance.updateExecutionStatus(status);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of addQueuedUri method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testAddQueuedUri() {
        System.out.println("addQueuedUri");
        QueuedUri qu = null;
        RethinkDbAdapter instance = null;
        QueuedUri expResult = null;
        QueuedUri result = instance.addQueuedUri(qu);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of updateQueuedUri method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testUpdateQueuedUri() {
        System.out.println("updateQueuedUri");
        QueuedUri qu = null;
        RethinkDbAdapter instance = null;
        QueuedUri expResult = null;
        QueuedUri result = instance.updateQueuedUri(qu);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of addScreenshot method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testAddScreenshot() {
        System.out.println("addScreenshot");
        Screenshot s = null;
        RethinkDbAdapter instance = null;
        Screenshot expResult = null;
        Screenshot result = instance.addScreenshot(s);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of listSeeds method, of class RethinkDbAdapter.
     */
    @Test
    public void testListSeeds() {
        Seed seed1 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed1.foo"))
                .addJobId("job1")
                .build();
        Seed savedSeed1 = db.saveSeed(seed1);

        Seed seed2 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed2.foo"))
                .addJobId("job1")
                .addJobId("job2")
                .build();
        Seed savedSeed2 = db.saveSeed(seed2);

        Seed seed3 = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("http://seed3.foo"))
                .addJobId("job2")
                .build();
        Seed savedSeed3 = db.saveSeed(seed3);

        SeedListRequest request = SeedListRequest.newBuilder().setCrawlJobId("job1").build();
        SeedListReply result = db.listSeeds(request);
        assertThat(result.getValueList()).containsOnly(savedSeed1, savedSeed2);
    }

    /**
     * Test of saveSeed method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testSaveSeed() {
        System.out.println("saveSeed");
        ConfigProto.Seed seed = null;
        RethinkDbAdapter instance = null;
        ConfigProto.Seed expResult = null;
        ConfigProto.Seed result = instance.saveSeed(seed);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteSeed method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testDeleteSeed() {
        System.out.println("deleteSeed");
        ConfigProto.Seed seed = null;
        RethinkDbAdapter instance = null;
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
    public void testSaveAndListCrawlJob() {
        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setSchedule(ConfigProto.CrawlScheduleConfig.newBuilder()
                        .setCronExpression("* * * * *")
                        .setMeta(Meta.newBuilder().setName("Every minute")))
                .setCrawlConfig(ConfigProto.CrawlConfig.newBuilder()
                        .setMeta(Meta.newBuilder().setName("Test crawl config"))
                        .setBrowserConfig(ConfigProto.BrowserConfig.newBuilder()
                                .setMeta(Meta.newBuilder().setName("Test browser config"))
                                .setWindowWidth(100))
                        .setPoliteness(ConfigProto.PolitenessConfig.newBuilder()
                                .setMeta(Meta.newBuilder().setName("Test politeness config"))
                                .setMinTimeBetweenPageLoadMs(500)))
                .build();

        CrawlJob result = db.saveCrawlJob(crawlJob);
        assertThat(result.getId()).isNotEmpty();

        assertThat(db.listCrawlJobs(CrawlJobListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test job");
                    assertThat(r.getValue(0).getSceduleConfigOrIdCase())
                            .isSameAs(CrawlJob.SceduleConfigOrIdCase.SCHEDULE_ID);
                });

        assertThat(db.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(result.getScheduleId());
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Every minute");
                    assertThat(r.getValue(0).getCronExpression()).isEqualTo("* * * * *");
                });

        Map<String, String> id = new HashMap<>();
        assertThat(db.listCrawlConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(result.getCrawlConfigId());
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test crawl config");
                    id.put("browserConfig", r.getValue(0).getBrowserConfigId());
                    id.put("politenessConfig", r.getValue(0).getPolitenessId());
                });

        assertThat(db.listBrowserConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(id.get("browserConfig"));
                    assertThat(r.getValue(0).getWindowWidth()).isEqualTo(100);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test browser config");
                });

        assertThat(db.listPolitenessConfigs(ListRequest.getDefaultInstance()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getId()).isEqualTo(id.get("politenessConfig"));
                    assertThat(r.getValue(0).getMinTimeBetweenPageLoadMs()).isEqualTo(500);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test politeness config");
                });

        // Test with resolve parameter
        assertThat(db.listCrawlJobs(CrawlJobListRequest.newBuilder().setExpand(true).build()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test job");
                    assertThat(r.getValue(0).getSceduleConfigOrIdCase())
                            .isSameAs(CrawlJob.SceduleConfigOrIdCase.SCHEDULE);
                });

        Label label = Label.newBuilder().setKey("type").setValue("default").build();
        crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setCrawlConfig(ConfigProto.CrawlConfig.newBuilder()
                        .setMeta(Meta.newBuilder().setName("Test crawl config with label").addLabel(label))
                        .setPoliteness(ConfigProto.PolitenessConfig.newBuilder()
                                .setMeta(Meta.newBuilder().setName("Test politeness config"))
                                .setMinTimeBetweenPageLoadMs(500)))
                .build();

        CrawlJob result2 = db.saveCrawlJob(crawlJob);

        assertThat(db.listCrawlJobs(CrawlJobListRequest.newBuilder().setId(result2.getId()).setExpand(true).build()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test job");
                    assertThat(r.getValue(0).getSceduleConfigOrIdCase())
                            .isSameAs(CrawlJob.SceduleConfigOrIdCase.SCEDULECONFIGORID_NOT_SET);
                    assertThat(r.getValue(0).getCrawlConfigOrIdCase())
                            .isSameAs(CrawlJob.CrawlConfigOrIdCase.CRAWL_CONFIG);
                });

        CrawlJob badCrawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setSchedule(ConfigProto.CrawlScheduleConfig.newBuilder()
                        .setCronExpression("* * * * *")
                        .setMeta(Meta.newBuilder().setName("Every minute")))
                .build();

        assertThatThrownBy(() -> db.saveCrawlJob(badCrawlJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A crawl config is required for crawl jobs");

        CrawlJob crawlJobWithSelector = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setCrawlConfigSelector(Selector.newBuilder().addLabel(label))
                .build();

        CrawlJob result3 = db.saveCrawlJob(crawlJobWithSelector);

        assertThat(db.listCrawlJobs(CrawlJobListRequest.newBuilder().setId(result3.getId()).setExpand(true).build()))
                .satisfies(r -> {
                    assertThat(r.getCount()).isEqualTo(1);
                    assertThat(r.getValue(0).getMeta().getName()).isEqualTo("Test job");
                    assertThat(r.getValue(0).getSceduleConfigOrIdCase())
                            .isSameAs(CrawlJob.SceduleConfigOrIdCase.SCEDULECONFIGORID_NOT_SET);
                    assertThat(r.getValue(0).getCrawlConfigOrIdCase())
                            .isSameAs(CrawlJob.CrawlConfigOrIdCase.CRAWL_CONFIG);
                    assertThat(r.getValue(0).getCrawlConfig().getId())
                            .isEqualTo(result3.getCrawlConfigId());
                    assertThat(r.getValue(0).getCrawlConfig().getMeta().getName())
                            .isEqualTo("Test crawl config with label");
                });
    }

    /**
     * Test of deleteCrawlJob method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlJob() {
        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setCrawlConfig(ConfigProto.CrawlConfig.newBuilder()
                        .setMeta(Meta.newBuilder().setName("Test crawl config")))
                .build();

        CrawlJob result = db.saveCrawlJob(crawlJob);
        assertThat(db.listCrawlJobs(CrawlJobListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        db.deleteCrawlJob(crawlJob);
        assertThat(db.listCrawlJobs(CrawlJobListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        db.deleteCrawlJob(result);
        assertThat(db.listCrawlJobs(CrawlJobListRequest.getDefaultInstance()).getCount()).isEqualTo(0);

        CrawlJob toBeDeleted = db.saveCrawlJob(crawlJob);
        Seed seed = Seed.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test seed"))
                .addJobId(toBeDeleted.getId())
                .build();

        seed = db.saveSeed(seed);

        assertThatThrownBy(() -> db.deleteCrawlJob(toBeDeleted))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Can't delete CrawlJob, there are 1 Seed(s) referring it");

        db.deleteSeed(seed);
        db.deleteCrawlJob(toBeDeleted);
    }

    /**
     * Test of listCrawlConfigs method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testListCrawlConfigs() {
        System.out.println("listCrawlConfigs");
        ListRequest request = null;
        RethinkDbAdapter instance = null;
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
    public void testSaveCrawlConfig() {
        System.out.println("saveCrawlConfig");
        ConfigProto.CrawlConfig crawlConfig = null;
        RethinkDbAdapter instance = null;
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
    public void testDeleteCrawlConfig() {
        System.out.println("deleteCrawlConfig");
        ConfigProto.CrawlConfig crawlConfig = null;
        RethinkDbAdapter instance = null;
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
    public void testListCrawlScheduleConfigs() {
        System.out.println("listCrawlScheduleConfigs");
        ListRequest request = null;
        RethinkDbAdapter instance = null;
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
    public void testSaveCrawlScheduleConfig() {
        System.out.println("saveCrawlScheduleConfig");
        ConfigProto.CrawlScheduleConfig crawlScheduleConfig = null;
        RethinkDbAdapter instance = null;
        ConfigProto.CrawlScheduleConfig expResult = null;
        ConfigProto.CrawlScheduleConfig result = instance.saveCrawlScheduleConfig(crawlScheduleConfig);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteCrawlScheduleConfig method, of class RethinkDbAdapter.
     */
    @Test
    public void testDeleteCrawlScheduleConfig() {
        CrawlScheduleConfig scheduleConfig = CrawlScheduleConfig.newBuilder()
                .setCronExpression("* * * * *")
                .setMeta(Meta.newBuilder().setName("Every minute"))
                .build();

        CrawlScheduleConfig result = db.saveCrawlScheduleConfig(scheduleConfig);
        assertThat(db.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        db.deleteCrawlScheduleConfig(scheduleConfig);
        assertThat(db.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);
        db.deleteCrawlScheduleConfig(result);
        assertThat(db.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(0);

        CrawlJob crawlJob = CrawlJob.newBuilder()
                .setMeta(Meta.newBuilder().setName("Test job"))
                .setSchedule(ConfigProto.CrawlScheduleConfig.newBuilder()
                        .setCronExpression("* * * * *")
                        .setMeta(Meta.newBuilder().setName("Every minute")))
                .setCrawlConfig(ConfigProto.CrawlConfig.newBuilder()
                        .setMeta(Meta.newBuilder().setName("Test crawl config")))
                .build();

        crawlJob = db.saveCrawlJob(crawlJob);
        assertThat(db.listCrawlScheduleConfigs(ListRequest.getDefaultInstance()).getCount()).isEqualTo(1);

        CrawlScheduleConfig toBeDeleted = CrawlScheduleConfig.newBuilder().setId(crawlJob.getScheduleId()).build();
        assertThatThrownBy(() -> db.deleteCrawlScheduleConfig(toBeDeleted))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Can't delete CrawlScheduleConfig, there are 1 CrawlJob(s) referring it");

        db.deleteCrawlJob(crawlJob);
        db.deleteCrawlScheduleConfig(toBeDeleted);
    }

}
