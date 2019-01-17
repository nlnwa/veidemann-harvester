/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db.initializer;

import com.google.protobuf.Message;
import com.rethinkdb.gen.ast.GetField;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.model.MapObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class Upgrade1_0To1_1 extends UpgradeDbBase {
    public Upgrade1_0To1_1(String dbName, RethinkDbConnection conn) {
        super(dbName, conn);
    }

    final void upgrade() throws DbQueryException, DbConnectionException {
        // Drop old cache table
        conn.exec(r.tableDrop("already_crawled_cache"));

        // Drop old indexes
        conn.exec(r.table(Tables.SEEDS.name).indexDrop("jobId"));
        conn.exec(r.table(Tables.SEEDS.name).indexDrop("entityId"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexDrop("crawlHostGroupKey_sequence_earliestFetch"));

        renameCrawlConfigField();

        updateRefFields(Tables.SEEDS, Kind.seed,
                RefField.create("entityId", "entityRef", Kind.crawlEntity),
                RefField.create("jobId", "jobRef", Kind.crawlJob)
        );

        updateRefFields(Tables.CONFIG, Kind.crawlJob,
                RefField.create("scheduleId", "scheduleRef", Kind.crawlScheduleConfig),
                RefField.create("crawlConfigId", "crawlConfigRef", Kind.crawlConfig)
        );

        updateRefFields(Tables.CONFIG, Kind.crawlConfig,
                RefField.create("collectionId", "collectionRef", Kind.collection),
                RefField.create("browserConfigId", "browserConfigRef", Kind.browserConfig),
                RefField.create("politenessId", "politenessRef", Kind.politenessConfig)
        );

        updateRefFields(Tables.CONFIG, Kind.browserConfig,
                RefField.create("scriptId", "scriptRef", Kind.browserScript)
        );

        // Insert default config for the new collection type
        insertDefaultCollection();

        // Create new indexes
        conn.exec(r.table(Tables.CONFIG.name).indexCreate("configRefs", row -> row
                .g(Kind.browserConfig.name()).g("scriptRef").map(d -> r.array(d.g("kind"), d.g("id")))
                .add(r.array(row.g(Kind.crawlJob.name()).g("scheduleRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                .add(r.array(row.g(Kind.crawlJob.name()).g("crawlConfigRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                .add(r.array(row.g(Kind.crawlConfig.name()).g("collectionRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                .add(r.array(row.g(Kind.crawlConfig.name()).g("browserConfigRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
                .add(r.array(row.g(Kind.crawlConfig.name()).g("politenessRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
        ).optArg("multi", true));

        conn.exec(r.table(Tables.SEEDS.name).indexCreate("configRefs", row -> row
                .g(Kind.seed.name()).g("jobRef").map(d -> r.array(d.g("kind"), d.g("id")))
                .add(r.array(row.g(Kind.seed.name()).g("entityRef").do_(d -> r.array(d.g("kind"), d.g("id")))))
        ).optArg("multi", true));

        conn.exec(r.table(Tables.URI_QUEUE.name).indexCreate("crawlHostGroupKey_sequence_earliestFetch",
                uri -> r.array(uri.g("crawlHostGroupId"),
                        uri.g("politenessRef").g("id"),
                        uri.g("sequence"),
                        uri.g("earliestFetchTimeStamp"))));

        conn.exec(r.table(Tables.CONFIG.name).indexWait("configRefs"));
        conn.exec(r.table(Tables.SEEDS.name).indexWait("configRefs"));
        conn.exec(r.table(Tables.URI_QUEUE.name).indexWait("crawlHostGroupKey_sequence_earliestFetch"));
    }

    @Override
    String fromVersion() {
        return "1.0";
    }

    @Override
    String toVersion() {
        return "1.1";
    }

    private void renameCrawlConfigField() throws DbQueryException, DbConnectionException {
        String kind = "crawlConfig";
        conn.exec(r.table(Tables.CONFIG.name).filter(r.hashMap("kind", kind)).replace(o -> o
                .merge(r.hashMap(kind, r.hashMap("extra",
                        r.hashMap("createScreenshot", o.g(kind).g("extra").g("createSnapshot")))))
                .without(r.hashMap(kind, r.hashMap("extra", "createSnapshot")))
        ));
    }

    private static class RefField {
        String oldName;
        String newName;
        Kind referredKind;

        static RefField create(String oldName, String newName, Kind referredKind) {
            RefField ref = new RefField();
            ref.oldName = oldName;
            ref.newName = newName;
            ref.referredKind = referredKind;
            return ref;
        }

        ReqlFunction1 createMergeDoc(Kind kind) {
            ReqlFunction1 func = o -> r.branch(o.g(kind.name()).hasFields(oldName),
                    r.hashMap(kind.name(), r.hashMap(newName,
                            r.branch(o.g(kind.name()).g(oldName).typeOf().eq("STRING"),
                                    r.hashMap("kind", referredKind.name()).with("id", o.g(kind.name()).g(oldName)),
                                    o.g(kind.name()).g(oldName).map(id -> r.hashMap("kind", referredKind.name()).with("id", id))))),
                    r.hashMap());
            return func;
        }
    }

    private void updateRefFields(Tables table, Kind kind, RefField... refFields) throws DbQueryException, DbConnectionException {
        final List<String> oldNames = r.array();
        for (RefField ref : refFields) {
            oldNames.add(ref.oldName);
        }

        conn.exec(r.table(table.name).filter(r.hashMap("kind", kind.name())).replace(o -> {
                    for (RefField ref : refFields) {
                        o = o.merge(ref.createMergeDoc(kind));
                    }
                    o = o.without(r.hashMap(kind.name(), oldNames));
                    return o;
                }
        ));
    }

    private void insertDefaultCollection() throws DbQueryException, DbConnectionException {
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("default_objects/collection-configs.yaml")) {
            readYamlFile(in, ConfigObject.class)
                    .forEach(o -> {
                        try {
                            db.saveConfigObject(o);
                        } catch (DbException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (ChangeFeed<ConfigObject> collection = db.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.collection).addLabelSelector("type:default").build())) {

            String collectionId = collection.stream().findFirst().get().getId();

            conn.exec(r.table(Tables.CONFIG.name).filter(r.hashMap("kind", Kind.crawlConfig.name())).update(o ->
                    r.hashMap(Kind.crawlConfig.name(),
                            r.hashMap("collectionRef",
                                    r.hashMap("kind", Kind.collection.name()).with("id", collectionId)
                            )
                    )
            ));
        }
    }

    <T extends Message> Stream<T> readYamlFile(InputStream in, Class<T> type) {
        Yaml yaml = new Yaml();
        return StreamSupport.stream(yaml.loadAll(in).spliterator(), false)
                .map(o -> ProtoUtils.rethinkToProto((Map) o, type));
    }
}
