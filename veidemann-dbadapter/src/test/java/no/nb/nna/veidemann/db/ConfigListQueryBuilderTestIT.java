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

import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.Meta;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class ConfigListQueryBuilderTestIT {
    public static ConfigAdapter db;

    static CrawlEntity e1, e2, e3, e4, e5;

    @Before
    public void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
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

        db = DbService.getInstance().getConfigAdapter();

        e1 = db.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Entity One")
                        .addLabel(ApiTools.buildLabel("foo", "abc"))
                        .addLabel(ApiTools.buildLabel("bar", "def")))
                .build());
        e2 = db.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Entity Two")
                        .addLabel(ApiTools.buildLabel("foo", "abcde"))
                        .addLabel(ApiTools.buildLabel("bar", "xyz")))
                .build());
        e3 = db.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Entity Three")
                        .addLabel(ApiTools.buildLabel("foo", "bcde"))
                        .addLabel(ApiTools.buildLabel("bar", "xyz")))
                .build());
        e4 = db.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Entity Four")
                        .addLabel(ApiTools.buildLabel("foo", "xyz"))
                        .addLabel(ApiTools.buildLabel("bar", "abc")))
                .build());
        e5 = db.saveCrawlEntity(CrawlEntity.newBuilder()
                .setMeta(Meta.newBuilder()
                        .setName("Entity Five")
                        .addLabel(ApiTools.buildLabel("bar", "abc")))
                .build());
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void testBuildNameQuery() throws DbException {
        CrawlEntityListReply reply;

        // Match part of name
        reply = db.listCrawlEntities(ListRequest.newBuilder().setName("tity").build());
        assertThat(reply.getCount()).isEqualTo(5);
        assertThat(reply.getValueList()).containsExactly(e5, e4, e1, e3, e2);

        reply = db.listCrawlEntities(ListRequest.newBuilder().setName("^tity").build());
        assertThat(reply.getCount()).isEqualTo(0);
        assertThat(reply.getValueList()).containsExactly();

        reply = db.listCrawlEntities(ListRequest.newBuilder().setName("En.*e").build());
        assertThat(reply.getCount()).isEqualTo(3);
        assertThat(reply.getValueList()).containsExactly(e5, e1, e3);

        // Exact match
        reply = db.listCrawlEntities(ListRequest.newBuilder().setName("^Entity Three$").build());
        assertThat(reply.getCount()).isEqualTo(1);
        assertThat(reply.getValueList()).containsExactly(e3);
    }

    @Test
    public void testBuildSelectorQuery() throws DbException {
        CrawlEntityListReply reply;

        // No filter
        reply = db.listCrawlEntities(ListRequest.newBuilder().build());
        assertThat(reply.getCount()).isEqualTo(5);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2, e3, e4, e5);

        // No match
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("tomt").build());
        assertThat(reply.getCount()).isEqualTo(0);
        assertThat(reply.getValueList()).isEmpty();

        // Exact match
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:abc").build());
        assertThat(reply.getCount()).isEqualTo(1);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1);

        // key and wildcard
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:abc*").build());
        assertThat(reply.getCount()).isEqualTo(2);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2);

        // No key, exact value
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("abc").build());
        assertThat(reply.getCount()).isEqualTo(3);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e4, e5);

        // No key, value wildcard
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("abc*").build());
        assertThat(reply.getCount()).isEqualTo(4);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2, e4, e5);

        // Key, but no value
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:").build());
        assertThat(reply.getCount()).isEqualTo(4);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2, e3, e4);

        // Two filters
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:").addLabelSelector("bar:xyz").build());
        assertThat(reply.getCount()).isEqualTo(2);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e2, e3);

        // Three filters
        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:").addLabelSelector("bar:xyz").addLabelSelector("bcde").build());
        assertThat(reply.getCount()).isEqualTo(1);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e3);
    }

    @Test
    public void testMix() throws DbException {
        CrawlEntityListReply reply;

        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:abc*").setName("Entity").build());
        assertThat(reply.getCount()).isEqualTo(2);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2);

        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:abc*").setName("Two").build());
        assertThat(reply.getCount()).isEqualTo(1);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e2);

        reply = db.listCrawlEntities(ListRequest.newBuilder().addLabelSelector("foo:").setName("Entity F").build());
        assertThat(reply.getCount()).isEqualTo(1);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e4);
    }
}