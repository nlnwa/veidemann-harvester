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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.rethinkdb.RethinkDB;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlScope;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.netpreserve.commons.uri.UriConfigs;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CrawlExecution implements ForkJoinPool.ManagedBlocker, Delayed {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlExecution.class);

    private CrawlExecutionStatus status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private final CrawlScope scope;

    private QueuedUri currentUri;

    private volatile List<QueuedUri> outlinks;

    private long nextExecTimestamp = 0L;

    private final AtomicLong nextSeqNum = new AtomicLong(1L);

    private boolean seedResolved = false;

    private final Span parentSpan;

    public CrawlExecution(Span span, Frontier frontier, CrawlExecutionStatus status, CrawlConfig config, CrawlScope scope) {
        System.out.println("NEW CRAWL EXECUTION: " + status.getId());
        this.frontier = frontier;
        this.status = status;
        this.config = config;
        this.parentSpan = span;
        this.scope = scope;
    }

    public String getId() {
        return status.getId();
    }

    public CrawlConfig getConfig() {
        return config;
    }

    public CrawlExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(CrawlExecutionStatus status) {
        this.status = status;
    }

    public QueuedUri getCurrentUri() {
        return currentUri;
    }

    public void setCurrentUri(QueuedUri currentUri) {
        this.currentUri = currentUri;
        this.outlinks = Collections.EMPTY_LIST;
    }

    public void fetch() {
        System.out.println("Fetching " + currentUri.getUri());
        currentUri = currentUri.toBuilder()
                .setSurt(UriConfigs.SURT_KEY.buildUri(currentUri.getUri()).toString())
                .setScope(scope)
                .build();

        try {
            try {
                outlinks = frontier.getHarvesterClient().fetchPage(status.getId(), currentUri, config);
                seedResolved = true;

                status = status.toBuilder()
                        .setState(CrawlExecutionStatus.State.RUNNING)
                        .setDocumentsCrawled(status.getDocumentsCrawled() + 1)
                        .build();
                frontier.getDb().updateExecutionStatus(status);

            } catch (Exception e) {
                System.out.println("Error fetching page (" + currentUri + "): " + e);
                // should do some logging and updating here
                status = frontier.getDb().updateExecutionStatus(status.toBuilder()
                        .setState(CrawlExecutionStatus.State.FAILED)
                        .build());
                seedResolved = true;
                System.out.println("Nothing more to do since Harvester is dead.");
                return;
            }

            if (outlinks.isEmpty()) {
                LOG.debug("No outlinks from {}", currentUri.getSurt());
                return;
            }

            queueOutlinks();
        } catch (Exception e) {
            System.out.println("Strange error fetching page (" + currentUri + "): " + e);
            e.printStackTrace();
            // TODO: should do some logging and updating here
        }
        return;
    }

    void queueOutlinks() {
        QueuedUri.IdSeq currentId = QueuedUri.IdSeq.newBuilder()
                .setId(getId())
                .setSeq(1L)
                .build();

        long nextSequenceNum = 1;
        if (!config.getDepthFirst()) {
            nextSequenceNum = getNextSequenceNum();
        }

//        outlinks.stream().map(uri -> {
//            return uri.toBuilder();
//        }).forEach(outUriBuilder -> {
//            try {
//                outUriBuilder.setSurt(UriConfigs.SURT_KEY.buildUri(outUriBuilder.getUri()).toString());
//            } catch (Exception e) {
//                System.out.println("Strange error with outlink (" + outUriBuilder + "): " + e);
//                e.printStackTrace();
//                return;
//            }
//
//            if (shouldInclude(outUriBuilder)) {
//                LOG.debug("Found new URI: {}, queueing.", outUriBuilder.getSurt());
//                List<QueuedUri.IdSeq> eIds = outUriBuilder.getExecutionIdsList();
//
//                if (!eIds.contains(currentId)) {
//                    if (config.getDepthFirst()) {
//                        nextSequenceNum = getNextSequenceNum();
//                    }
//
//                    outUriBuilder.addExecutionIds(currentId.toBuilder().setSeq(nextSequenceNum).build());
//                }
//                try {
//                    frontier.getDb().addQueuedUri(outUriBuilder.build());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            } else {
//                LOG.debug("Found already included URI: {}, skipping.", outUriBuilder.getSurt());
//            }
//        });
        for (QueuedUri outUri : outlinks) {
            QueuedUri.Builder outUriBuilder = outUri.toBuilder();
            try {
                outUriBuilder.setSurt(UriConfigs.SURT_KEY.buildUri(outUriBuilder.getUri()).toString());
            } catch (Exception e) {
                System.out.println("Strange error with outlink (" + outUriBuilder + "): " + e);
                e.printStackTrace();
                return;
            }

            if (shouldInclude(outUriBuilder)) {
                LOG.debug("Found new URI: {}, queueing.", outUriBuilder.getSurt());
                List<QueuedUri.IdSeq> eIds = outUriBuilder.getExecutionIdsList();

                if (!eIds.contains(currentId)) {
                    if (config.getDepthFirst()) {
                        nextSequenceNum = getNextSequenceNum();
                    }

                    outUriBuilder.addExecutionIds(currentId.toBuilder().setSeq(nextSequenceNum).build());
                }
                try {
                    frontier.getDb().addQueuedUri(outUriBuilder.build());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                LOG.debug("Found already included URI: {}, skipping.", outUriBuilder.getSurt());
            }
        }
    }

    boolean shouldInclude(MessagesProto.QueuedUriOrBuilder outlink) {
        RethinkDB r = RethinkDB.r;
        boolean notSeen = frontier.getDb().executeRequest(
                r.table(RethinkDbAdapter.TABLES.CRAWL_LOG.name)
                        .between(
                                r.array(outlink.getSurt(), ProtoUtils.tsToOdt(status.getStartTime())),
                                r.array(outlink.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(RethinkDbAdapter.TABLES.URI_QUEUE.name).getAll(outlink.getSurt())
                                        .optArg("index", "surt")
                                        .limit(1)
                        ).isEmpty());

        if (notSeen && outlink.getSurt().startsWith(scope.getSurtPrefix())) {
            return true;
        }
        return false;
    }

    public long objectSize() {
        long size = 0L;
        size += GraphLayout.parseInstance(getId()).totalSize();
        size += GraphLayout.parseInstance(config).totalSize();
        size += GraphLayout.parseInstance(currentUri).totalSize();
        return size;
    }

    @Override
    public boolean block() throws InterruptedException {
        if (outlinks.isEmpty()) {
            new OpenTracingWrapper("CrawlExecution")
                    .addTag(Tags.HTTP_URL.getKey(), currentUri.getUri())
                    .run("Fetch", this::fetch);
        }
        return true;
    }

    @Override
    public boolean isReleasable() {
        return !outlinks.isEmpty();
    }

    public boolean isDone() {
        return outlinks.isEmpty();
    }

    public boolean isSeedResolved() {
        return seedResolved;
    }

    public long getNextSequenceNum() {
        return nextSeqNum.getAndIncrement();
    }

    public void calculateDelay() {
        nextExecTimestamp = System.currentTimeMillis() + getConfig().getPoliteness().getMinTimeBetweenPageLoadMs();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long remainingDelayMillis = nextExecTimestamp - System.currentTimeMillis();
        return unit.convert(remainingDelayMillis, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o instanceof CrawlExecution) {
            return (int) (((CrawlExecution) o).nextExecTimestamp - nextExecTimestamp);
        }
        throw new IllegalArgumentException("Can only compare CrawlExecution");
    }

    public Span getParentSpan() {
        return parentSpan;
    }

}
