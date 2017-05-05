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
import io.opentracing.References;
import no.nb.nna.broprox.commons.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;

import static no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;

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

            new OpenTracingWrapper("QueueWorker")
                    .setParentSpan(exe.getParentSpan())
                    .setParentReferenceType(References.FOLLOWS_FROM)
                    .run("runNextFetch", this::processExecution, exe);
        }
    }

    private void processExecution(CrawlExecution exe) {
        if (!exe.isSeedResolved()) {
            try {
                getPool().managedBlock(exe);
                exe.calculateDelay();
                frontier.executionsQueue.add(exe);
                System.out.println("End of Seed crawl");
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            QueuedUri qUri = getNextToFetch(exe.getId());
            if (qUri == null) {
                // No more uris, we are done.
                System.out.println("Reached end of crawl");
                CrawlExecutionStatus.State state = exe.getStatus().getState();
                if (state == CrawlExecutionStatus.State.RUNNING) {
                    state = CrawlExecutionStatus.State.FINISHED;
                }
                exe.setStatus(exe.getStatus().toBuilder()
                        .setState(state)
                        .setEndTime(ProtoUtils.getNowTs())
                        .build());
                frontier.getDb().updateExecutionStatus(exe.getStatus());
                frontier.getHarvesterClient().cleanupExecution(exe.getId());
                frontier.runningExecutions.remove(exe.getId());
            } else {
                frontier.getDb().executeRequest(r.table(TABLES.URI_QUEUE.name).get(qUri.getId()).delete());
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
    }

    QueuedUri getNextToFetch(String executionId) {
        try (Cursor<Map<String, Object>> cursor = frontier.getDb().executeRequest(
                r.table(TABLES.URI_QUEUE.name)
                        .between(r.array(executionId, r.minval()), r.array(executionId, r.maxval()))
                        .optArg("index", "executionIds").orderBy().optArg("index", "executionIds")
                        .limit(1));) {
            if (cursor.hasNext()) {
                return ProtoUtils.rethinkToProto(cursor.next(), QueuedUri.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
