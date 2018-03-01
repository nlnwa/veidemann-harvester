package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.Meta;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class ConfigListQueryBuilderTestIT {
    public static RethinkDbAdapter db;

    static CrawlEntity e1, e2, e3, e4, e5;

    @BeforeClass
    public static void init() {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));
        db = new RethinkDbAdapter(dbHost, dbPort, "veidemann");

        RethinkDB r = RethinkDB.r;
        for (RethinkDbAdapter.TABLES table : RethinkDbAdapter.TABLES.values()) {
            if (table != RethinkDbAdapter.TABLES.SYSTEM) {
                db.executeRequest("delete", r.table(table.name).delete());
            }
        }

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

    @AfterClass
    public static void shutdown() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testBuildNameQuery() {
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
    public void testBuildSelectorQuery() {
        CrawlEntityListReply reply;

        // No filter
        reply = db.listCrawlEntities(ListRequest.newBuilder().build());
        assertThat(reply.getCount()).isEqualTo(5);
        assertThat(reply.getValueList()).containsExactlyInAnyOrder(e1, e2, e3, e4, e5);

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
    public void testMix() {
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