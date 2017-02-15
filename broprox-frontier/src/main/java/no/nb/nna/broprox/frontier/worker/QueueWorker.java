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
package no.nb.nna.broprox.frontier.worker;

import java.util.Map;
import java.util.concurrent.RecursiveAction;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.QueuedUri;

import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_URI_QUEUE;

/**
 *
 */
public class QueueWorker extends RecursiveAction {

    private final Frontier queueProcessor;

    static final RethinkDB r = RethinkDB.r;

    public QueueWorker(Frontier queueProcessor) {
        this.queueProcessor = queueProcessor;
    }

    @Override
    protected void compute() {
        while (true) {
            QueuedUri qUri = getNextToFetch();
            queueProcessor.getDb().executeRequest(r.table(TABLE_URI_QUEUE).get(qUri.getId()).delete());
            CrawlExecution exe = queueProcessor.runningExecutions.get(qUri.getExecutionId());
            if (exe != null) {
                exe.setCurrentUri(qUri.getUri());
                try {
                    getPool().managedBlock(exe);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    QueuedUri getNextToFetch() {
        while (true) {
            try (Cursor<Map<String, Object>> cursor = queueProcessor.getDb().executeRequest(
                    r.table(TABLE_URI_QUEUE)
                    .limit(1));) {
                if (cursor.hasNext()) {
                    return DbObjectFactory.of(QueuedUri.class, cursor.next()).get();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
