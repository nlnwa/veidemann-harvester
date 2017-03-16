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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.RecursiveAction;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.CrawlExecutionStatus;
import no.nb.nna.broprox.db.model.QueuedUri;

import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLE_URI_QUEUE;

/**
 *
 */
public class QueueWorker extends RecursiveAction {

    private final Frontier frontier;

    static final RethinkDB r = RethinkDB.r;

    public QueueWorker(Frontier frontier) {
        this.frontier = frontier;
    }

    @Override
    protected void compute() {
        while (true) {
            CrawlExecution exe;
            try {
                System.out.println("Waiting for next execution to be ready");
                exe = frontier.executionsQueue.take();
                System.out.println("Running next fetch of exexcution: " + exe.getId());
            } catch (InterruptedException ex) {
                // We are interrupted, stop the crawl.
                System.out.println("Crawler thread stopped");
                return;
            }
            QueuedUri qUri = getNextToFetch(exe.getId());
            if (qUri == null) {
                // No more uris, we are done.
                System.out.println("Reached end of crawl");
                exe.getStatus().withState(CrawlExecutionStatus.State.FINISHED)
                        .withEndTime(OffsetDateTime.now(ZoneOffset.UTC));
                frontier.getDb().updateExecutionStatus(exe.getStatus());
                frontier.runningExecutions.remove(exe.getId());
                return;
            }
            frontier.getDb().executeRequest(r.table(TABLE_URI_QUEUE).get(qUri.getId()).delete());
            exe.setCurrentUri(qUri);
            try {
                getPool().managedBlock(exe);
                exe.calculateDelay();
                frontier.executionsQueue.add(exe);
                System.out.println("End of Link crawl");
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    QueuedUri getNextToFetch(String executionId) {
        try (Cursor<Map<String, Object>> cursor = frontier.getDb().executeRequest(
                r.table(TABLE_URI_QUEUE).between(r.array(executionId, r.minval()), r.array(executionId, r.maxval()))
                .optArg("index", "executionIds").orderBy().optArg("index", "executionIds")
                .limit(1));) {
            if (cursor.hasNext()) {
                return DbObjectFactory.of(QueuedUri.class, cursor.next()).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
