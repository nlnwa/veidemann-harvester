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
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.Label;
import no.nb.nna.broprox.model.ConfigProto.Meta;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import no.nb.nna.broprox.model.MessagesProto.CrawledContent;
import no.nb.nna.broprox.model.MessagesProto.ExtractedText;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.model.MessagesProto.Screenshot;
import org.junit.AfterClass;
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

        CrawlEntity result = db.saveCrawlEntity(entity);

        assertThat(result.getId()).isNotEmpty();
        assertThat(result.getMeta().getName()).isEqualTo("Nasjonalbiblioteket");
        assertThat(result.getMeta().getCreated()).isEqualTo(
                ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z")));
        assertThat(result.getMeta().getCreatedBy()).isEqualTo("anonymous");
        assertThat(result.getMeta().getLastModified()).isNotNull();
        assertThat(result.getMeta().getLastModifiedBy()).isEqualTo("anonymous");
        assertThat(result.getMeta().getLabelList()).containsOnly(
                Label.newBuilder()
                        .setKey("frequency")
                        .setValue("Daily").build(),
                Label.newBuilder()
                        .setKey("orgType")
                        .setValue("Government").build());
    }

    /**
     * Test of listCrawlEntities method, of class RethinkDbAdapter.
     */
    @Test
    public void testListCrawlEntities() throws InvalidProtocolBufferException {
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

        CrawlEntity entity3 = CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalballetten")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Hourly"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Culture"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();
        entity3 = db.saveCrawlEntity(entity3);

        ListRequest request = ListRequest.getDefaultInstance();
        CrawlEntityListReply result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isGreaterThanOrEqualTo(3);
        assertThat(result.getCount()).isGreaterThanOrEqualTo(3);
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
        assertThat(result.getCount()).isGreaterThanOrEqualTo(3);
        assertThat(result.getValueList()).contains(entity3, entity1);

        request = ListRequest.newBuilder().setPageSize(2).setPage(1).build();
        result = db.listCrawlEntities(request);
        assertThat(result.getValueCount()).isEqualTo(1);
        assertThat(result.getCount()).isGreaterThanOrEqualTo(3);
        assertThat(result.getValueList()).contains(entity2);
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
        assertThat(result.getValueCount()).isGreaterThanOrEqualTo(2);
        assertThat(result.getValueList()).contains(entity1, entity2);

        db.deleteCrawlEntity(entity2);

        result = db.listCrawlEntities(ListRequest.getDefaultInstance());
        assertThat(result.getValueCount()).isGreaterThanOrEqualTo(1);
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
    @Ignore
    public void testAddCrawlLog() {
        System.out.println("addCrawlLog");
        CrawlLog cl = null;
        RethinkDbAdapter instance = null;
        CrawlLog expResult = null;
        CrawlLog result = instance.addCrawlLog(cl);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
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
    @Ignore
    public void testSaveBrowserScript() {
        System.out.println("saveBrowserScript");
        ConfigProto.BrowserScript script = null;
        RethinkDbAdapter instance = null;
        ConfigProto.BrowserScript expResult = null;
        ConfigProto.BrowserScript result = instance.saveBrowserScript(script);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getBrowserScripts method, of class RethinkDbAdapter.
     */
    @Test
    @Ignore
    public void testGetBrowserScripts() {
        System.out.println("getBrowserScripts");
        ConfigProto.BrowserScript.Type type = null;
        RethinkDbAdapter instance = null;
        List<ConfigProto.BrowserScript> expResult = null;
        List<ConfigProto.BrowserScript> result = instance.getBrowserScripts(type);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
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

}
