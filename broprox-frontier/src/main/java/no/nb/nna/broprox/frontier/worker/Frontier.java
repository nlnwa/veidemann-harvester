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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnels;
import com.rethinkdb.RethinkDB;
import no.nb.nna.broprox.commons.opentracing.OpenTracingParentContextKey;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.netpreserve.commons.uri.UriConfigs;

/**
 *
 */
public class Frontier implements AutoCloseable {

    private static final int EXPECTED_MAX_URIS = 1000000;

    private final RethinkDbAdapter db;

    private final HarvesterClient harvesterClient;

    static final RethinkDB r = RethinkDB.r;

    final Map<String, CrawlExecution> runningExecutions = new ConcurrentHashMap<>();

    final DelayQueue<CrawlExecution> executionsQueue = new DelayQueue<>();

    static final ForkJoinPool EXECUTOR_SERVICE = new ForkJoinPool(32);

    CuckooFilter<CharSequence> alreadyIncluded;

    public Frontier(RethinkDbAdapter db, HarvesterClient harvesterClient) {
        this.db = db;
        this.harvesterClient = harvesterClient;
        this.alreadyIncluded = new CuckooFilter.Builder<>(Funnels.unencodedCharsFunnel(), EXPECTED_MAX_URIS).build();

        System.out.println("Starting Queue Processor");
//        ForkJoinTask proc = EXECUTOR_SERVICE.submit(new Runnable() {
//            @Override
//            public void map() {
//                try (Cursor<Map<String, Map<String, Object>>> cursor = db.executeRequest(r.table(TABLE_URI_QUEUE)
//                        .changes());) {
//                    for (Map<String, Map<String, Object>> doc : cursor) {
//                        // Remove from already included filter when deleted from queue
//                        DbObjectFactory.of(QueuedUri.class, doc.get("old_val"))
//                                .ifPresent(q -> alreadyIncluded.delete(q.getSurt()));
//
//                        // Add to already included filter when added to queue
//                        DbObjectFactory.of(QueuedUri.class, doc.get("new_val"))
//                                .ifPresent(q -> alreadyIncluded.put(q.getSurt()));
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//        });

        for (int i = 0; i < 30; i++) {
            EXECUTOR_SERVICE.submit(new QueueWorker(this));
        }
    }

    public CrawlExecutionStatus newExecution(final CrawlJob job, final Seed seed) {
        OpenTracingWrapper otw = new OpenTracingWrapper("Frontier");
        return otw.map("scheduleSeed", this::scheduleSeed, job, seed);
    }

    public CrawlExecutionStatus scheduleSeed(final CrawlJob job, final Seed seed) {
        CrawlExecutionStatus status = CrawlExecutionStatus.newBuilder()
                .setJobId(job.getId())
                .setSeedId(seed.getId())
                .setState(CrawlExecutionStatus.State.CREATED)
                .build();

        status = db.addExecutionStatus(status);
        CrawlExecution exe = new CrawlExecution(
                OpenTracingParentContextKey.parentSpan(), this, status, job, seed.getScope());
        runningExecutions.put(status.getId(), exe);

        status = status.toBuilder().setState(CrawlExecutionStatus.State.RUNNING)
                .setStartTime(ProtoUtils.getNowTs())
                .build();
        db.updateExecutionStatus(status);

        String uri = seed.getMeta().getName();
        QueuedUri qUri = QueuedUri.newBuilder()
                .setUri(uri)
                .setExecutionId(status.getId())
                .setSequence(exe.getNextSequenceNum())
                .setSurt(UriConfigs.SURT_KEY.buildUri(uri).toString())
                .setScope(seed.getScope())
                .build();
        exe.setCurrentUri(qUri);
        executionsQueue.add(exe);

        return status;
    }

    boolean alreadeyIncluded(QueuedUri qUri) {
        boolean included = alreadyIncluded.mightContain(qUri.getSurt());
        if (included) {
            // TODO: extra check
        }
        return included;
    }

    public RethinkDbAdapter getDb() {
        return db;
    }

    public HarvesterClient getHarvesterClient() {
        return harvesterClient;
    }

    @Override
    public void close() {
        try {
            EXECUTOR_SERVICE.shutdown();
            EXECUTOR_SERVICE.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

}
