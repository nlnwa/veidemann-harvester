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
package no.nb.nna.broprox.frontier.evaluation;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.db.model.QueuedUri;

import static no.nb.nna.broprox.db.RethinkDbAdapter.*;

/**
 *
 */
public class QueueProcessor {

    private final RethinkDbAdapter db;

    static final RethinkDB r = RethinkDB.r;

    public QueueProcessor(RethinkDbAdapter db) {
        this.db = db;

        System.out.println("Starting Queue Processor");
        ForkJoinPool.commonPool().submit(new Runnable() {
            @Override
            public void run() {
                try (Cursor<Map<String, Map<String, Object>>> cursor = db.executeRequest(r.table(TABLE_URI_QUEUE).changes());) {
                    for (Map<String, Map<String, Object>> doc : cursor) {

                        DbObjectFactory.of(QueuedUri.class, doc.get("new_val"))
                                .ifPresent(q -> {
                                    System.out.println("Deleting: " + q.getUri());
                                    db.executeRequest(r.table(TABLE_URI_QUEUE).get(q.getId()).delete());
                                });

                        DbObjectFactory.of(QueuedUri.class, doc.get("old_val"))
                                .ifPresent(q -> System.out.println("Old: " + q.getUri()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });
    }

}
