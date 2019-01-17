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
package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import io.grpc.Context;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.Map;

import static no.nb.nna.veidemann.api.config.v1.Kind.browserConfig;
import static no.nb.nna.veidemann.api.config.v1.Kind.browserScript;
import static no.nb.nna.veidemann.api.config.v1.Kind.crawlEntity;
import static no.nb.nna.veidemann.api.config.v1.Kind.crawlJob;
import static no.nb.nna.veidemann.api.config.v1.Kind.crawlScheduleConfig;
import static no.nb.nna.veidemann.api.config.v1.Kind.seed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbConfigAdapterIT {
    public static RethinkDbConfigAdapter configAdapter;
    public static RethinkDbAdapter dbAdapter;
    static final RethinkDB r = RethinkDB.r;

    ConfigObject saved1;
    ConfigObject saved2;
    ConfigObject saved3;
    ConfigObject saved4;
    ConfigObject saved5;

    @Before
    public void init() throws DbException {
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
        try {
            DbService.getInstance().getDbInitializer().initialize();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        configAdapter = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();
        dbAdapter = (RethinkDbAdapter) DbService.getInstance().getDbAdapter();

        ConfigObject.Builder co1 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlScheduleConfig);
        co1.getMetaBuilder()
                .setName("csc1")
                .setDescription("desc1")
                .addLabelBuilder().setKey("foo").setValue("bar");
        co1.getCrawlScheduleConfigBuilder()
                .setCronExpression("cron1");

        ConfigObject.Builder co2 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlScheduleConfig);
        co2.getMetaBuilder()
                .setName("csc2")
                .setDescription("desc2");
        co2.getCrawlScheduleConfigBuilder()
                .setCronExpression("cron2");

        ConfigObject.Builder co3 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlScheduleConfig);
        co3.getMetaBuilder()
                .setName("csc3")
                .setDescription("desc3")
                .addLabelBuilder().setKey("foo").setValue("bar");
        co3.getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        co3.getCrawlScheduleConfigBuilder()
                .setCronExpression("cron3");

        saved1 = configAdapter.saveConfigObject(co1.build());
        saved2 = configAdapter.saveConfigObject(co2.build());
        saved3 = configAdapter.saveConfigObject(co3.build());

        ConfigObject.Builder co4 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlJob);
        co4.getMetaBuilder()
                .setName("cj1")
                .setDescription("desc4")
                .addLabelBuilder().setKey("foo").setValue("bar");
        co4.getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        co4.getCrawlJobBuilder().getScheduleRefBuilder()
                .setKind(crawlScheduleConfig)
                .setId(saved1.getId());

        ConfigObject.Builder co5 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlJob);
        co5.getMetaBuilder()
                .setName("cj2")
                .setDescription("desc5")
                .addLabelBuilder().setKey("foo").setValue("bar");
        co5.getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        co5.getCrawlJobBuilder().getScheduleRefBuilder()
                .setKind(crawlScheduleConfig)
                .setId(saved1.getId());

        saved4 = configAdapter.saveConfigObject(co4.build());
        saved5 = configAdapter.saveConfigObject(co5.build());
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void testSaveAndGetConfigObject() throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(browserConfig);

        co.getMetaBuilder()
                .setName("Default")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");

        co.getBrowserConfigBuilder()
                .setUserAgent("agent")
                .addScriptRef(ConfigRef.newBuilder().setKind(browserScript).setId("script"));

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(browserConfig)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();
    }

    @Test
    public void testDelete() throws DbException {
        assertThatExceptionOfType(DbQueryException.class)
                .isThrownBy(() -> configAdapter.deleteConfigObject(saved1))
                .withMessage("Can't delete crawlScheduleConfig, there are 2 crawlJob(s) referring it");
        assertThat(configAdapter.deleteConfigObject(saved4).getDeleted()).isTrue();
        assertThatExceptionOfType(DbQueryException.class)
                .isThrownBy(() -> configAdapter.deleteConfigObject(saved1))
                .withMessage("Can't delete crawlScheduleConfig, there are 1 crawlJob(s) referring it");
        assertThat(configAdapter.deleteConfigObject(saved5).getDeleted()).isTrue();
        assertThat(configAdapter.deleteConfigObject(saved1).getDeleted()).isTrue();
    }

    @Test
    public void testListConfigObjects() throws DbException {
        // Test list by kind
        ListRequest.Builder req1 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);

        assertThat(configAdapter.listConfigObjects(req1.build()).stream())
                .contains(saved1, saved2, saved3).doesNotContain(saved4);

        // Test list by id
        ListRequest.Builder req2 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .addId(saved1.getId())
                .addId(saved3.getId());

        assertThat(configAdapter.listConfigObjects(req2.build()).stream())
                .contains(saved1, saved3).doesNotContain(saved2, saved4);

        // Test list by name regexp
        ListRequest.Builder req3 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex(".*3");

        assertThat(configAdapter.listConfigObjects(req3.build()).stream())
                .contains(saved3).doesNotContain(saved1, saved2, saved4);

        // Test select returned fields
        ListRequest.Builder req4 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req4.getReturnedFieldsMaskBuilder()
                .addPaths("meta.name")
                .addPaths("apiVersion");

        assertThat(configAdapter.listConfigObjects(req4.build()).stream())
                .isNotEmpty().allMatch(c ->
                c.getMeta().getLabelCount() == 0
                        && c.getApiVersion().equals("v1")
                        && !c.getMeta().getName().isEmpty()
                        && c.getMeta().getDescription().isEmpty());

        // Test list by template filter
        ListRequest.Builder req5 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req5.getQueryTemplateBuilder().getMetaBuilder()
                .setName("csc3")
                .setDescription("desc2")
                .addLabelBuilder().setKey("foo").setValue("bar");

        ListRequest.Builder lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.name");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(saved3).doesNotContain(saved1, saved2, saved4);


        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.description");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(saved2).doesNotContain(saved3, saved1);

        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.label");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(saved1, saved3).doesNotContain(saved2, saved4);

        lr.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(saved3).doesNotContain(saved1, saved2, saved4);


        // Test order
        ListRequest.Builder req6 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req6.setOrderByPath("meta.name");

        assertThat(configAdapter.listConfigObjects(req6.build()).stream())
                .isSortedAccordingTo((r1, r2) -> r1.getMeta().getName().compareToIgnoreCase(r2.getMeta().getName()));

        req6.setOrderDescending(true);
        assertThat(configAdapter.listConfigObjects(req6.build()).stream())
                .isSortedAccordingTo((r1, r2) -> r2.getMeta().getName().compareToIgnoreCase(r1.getMeta().getName()));

        req6.setOrderDescending(false);
        req6.setOrderByPath("meta.lastModified");
        assertThat(configAdapter.listConfigObjects(req6.build()).stream()).isSortedAccordingTo(
                Comparator.comparing(r -> ProtoUtils.tsToOdt(r.getMeta().getLastModified())));

        req6.setOrderDescending(false);
        req6.setOrderByPath("meta.label");
        req6.setNameRegex("c[s|j]");
        assertThat(configAdapter.listConfigObjects(req6.build()).stream())
                .startsWith(saved3)
                .containsSubsequence(saved3, saved1)
                .containsOnlyOnce(saved1)
                .doesNotContain(saved2, saved4);

        // Test all options at once
        ListRequest.Builder req7 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .addId(saved1.getId())
                .addLabelSelector("foo:")
                .setOrderByPath("meta.label")
                .setNameRegex("csc");
        req7.getQueryTemplateBuilder()
                .getMetaBuilder().setDescription("desc1");
        req7.getQueryMaskBuilder().addPaths("meta.description");
        assertThat(configAdapter.listConfigObjects(req7.build()).stream())
                .contains(saved1).doesNotContain(saved3, saved2, saved4);
    }

    @Test
    public void testCountConfigObjects() throws Exception {
        assertThat(configAdapter.countConfigObjects(ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]").build()).getCount()).isEqualTo(3);
    }

    @Test
    public void testGetLabelKeys() throws Exception {
        assertThat(configAdapter.getLabelKeys(GetLabelKeysRequest.newBuilder()
                .setKind(crawlScheduleConfig).build()).getKeyList())
                .containsExactlyInAnyOrder("foo", "aaa");
    }

    @Test
    public void testUpdateConfigObjects() throws Exception {
        // Test add label to objects which already has 'foo:bar' label
        UpdateRequest.Builder ur1 = UpdateRequest.newBuilder();
        ur1.getListRequestBuilder().setKind(crawlScheduleConfig)
                .getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("foo").setValue("bar");
        ur1.getListRequestBuilder().getQueryMaskBuilder().addPaths("meta.label");
        ur1.getUpdateTemplateBuilder().getMetaBuilder().setDescription("jalla").addLabelBuilder().setKey("big").setValue("bang");
        ur1.getUpdateMaskBuilder().addPaths("meta.label+");
        assertThat(configAdapter.updateConfigObjects(ur1.build()).getUpdated()).isEqualTo(2);

        // Check result
        ListRequest.Builder test1 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]");

        Label fooBarLabel = Label.newBuilder().setKey("foo").setValue("bar").build();
        Label bigBangLabel = Label.newBuilder().setKey("big").setValue("bang").build();
        Label aaaBBBLabel = Label.newBuilder().setKey("aaa").setValue("bbb").build();
        Label cccDDDLabel = Label.newBuilder().setKey("ccc").setValue("ddd").build();
        assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                .allMatch(r -> {
                    if (r.getMeta().getLabelList().contains(fooBarLabel)) {
                        return r.getMeta().getLabelList().contains(bigBangLabel);
                    } else {
                        return !r.getMeta().getLabelList().contains(bigBangLabel);
                    }
                });

        // Test remove aaa:bbb label and meta.description from all objects
        UpdateRequest.Builder ur2 = UpdateRequest.newBuilder();
        ur2.getListRequestBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]")
                .getReturnedFieldsMaskBuilder().addPaths("meta.description");
        ur2.getUpdateTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        ur2.getUpdateTemplateBuilder().getCrawlScheduleConfigBuilder().setCronExpression("newCron");
        ur2.getUpdateMaskBuilder()
                .addPaths("meta.label-")
                .addPaths("meta.description")
                .addPaths("crawlScheduleConfig.cronExpression");

        // Set user to user1
        Context.current().withValues(EmailContextKey.getKey(), "user1", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur2.build()).getUpdated()).isEqualTo(3));

        // Check result
        assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                .allSatisfy(r -> {
                    assertThat(r.getMeta().getLabelList()).doesNotContain(aaaBBBLabel);
                    assertThat(r.getMeta().getDescription()).isEmpty();
                    assertThat(r.getCrawlScheduleConfig().getCronExpression()).isEqualTo("newCron");
                });

        // Repeat last update. No objects should actually change
        ConfigObject[] before = configAdapter.listConfigObjects(ListRequest.newBuilder().setKind(crawlScheduleConfig).build())
                .stream().toArray(ConfigObject[]::new);

        // Set user to user2
        Context.current().withValues(EmailContextKey.getKey(), "user2", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur2.build()).getUpdated()).isEqualTo(0));

        ConfigObject[] after = configAdapter.listConfigObjects(ListRequest.newBuilder().setKind(crawlScheduleConfig).build())
                .stream().toArray(ConfigObject[]::new);

        assertThat(before).contains(after);

        // Update whole subobject
        UpdateRequest.Builder ur3 = UpdateRequest.newBuilder();
        ur3.getListRequestBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]")
                .getReturnedFieldsMaskBuilder().addPaths("meta.description");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("ccc").setValue("ddd");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().setDescription("desc");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().setName("cs");
        ur3.getUpdateTemplateBuilder().getCrawlScheduleConfigBuilder().setCronExpression("newestCron");
        ur3.getUpdateMaskBuilder()
                .addPaths("meta")
                .addPaths("crawlScheduleConfig.validFrom")
                .addPaths("crawlScheduleConfig");

        // Set user to user3
        Context.current().withValues(EmailContextKey.getKey(), "user3", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur3.build()).getUpdated()).isEqualTo(3));

        try {
            // Check result
            assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                    .allSatisfy(r -> {
                        assertThat(r.getMeta().getLabelList()).containsOnly(cccDDDLabel);
                        assertThat(r.getMeta().getName()).isEqualTo("cs");
                        assertThat(r.getMeta().getDescription()).isEqualTo("desc");
                        assertThat(r.getCrawlScheduleConfig().getCronExpression()).isEqualTo("newestCron");
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSeed() throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(seed);

        co.getMetaBuilder()
                .setName("http://example.com")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");

        co.getSeedBuilder()
                .setEntityRef(ConfigRef.newBuilder().setKind(crawlEntity).setId("entity1"))
                .addJobRef(ConfigRef.newBuilder().setKind(crawlJob).setId("job1"));

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test List
        ListRequest lr = ListRequest.newBuilder().setKind(seed).build();
        assertThat(configAdapter.listConfigObjects(lr).stream()).containsExactly(saved);

        // Test update
        UpdateRequest ur = UpdateRequest.newBuilder().setListRequest(lr)
                .setUpdateTemplate(ConfigObject.getDefaultInstance()).build();
        assertThat(configAdapter.updateConfigObjects(ur).getUpdated()).isEqualTo(1);
        assertThat(configAdapter.listConfigObjects(lr).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                    assertThat(s.getMeta().getName()).isEmpty();
                    assertThat(s.getSeed().hasEntityRef()).isFalse();
                    assertThat(s.getSeed().getJobRefList()).isEmpty();
                });

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();
    }

    @Test
    public void testCrawlEntity() throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlEntity);

        co.getMetaBuilder()
                .setName("Example.com")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(crawlEntity)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test List
        ListRequest lr = ListRequest.newBuilder().setKind(crawlEntity).build();
        assertThat(configAdapter.listConfigObjects(lr).stream()).containsExactly(saved);

        // Test update
        UpdateRequest ur = UpdateRequest.newBuilder().setListRequest(lr)
                .setUpdateTemplate(ConfigObject.getDefaultInstance()).build();
        assertThat(configAdapter.updateConfigObjects(ur).getUpdated()).isEqualTo(1);
        assertThat(configAdapter.listConfigObjects(lr).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(crawlEntity);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                    assertThat(s.getMeta().getName()).isEmpty();
                });

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(crawlEntity)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();
    }
}