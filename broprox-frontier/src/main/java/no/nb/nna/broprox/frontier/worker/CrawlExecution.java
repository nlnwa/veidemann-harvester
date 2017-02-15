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

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

import no.nb.nna.broprox.db.DbObjectFactory;
import no.nb.nna.broprox.db.model.CrawlConfig;
import no.nb.nna.broprox.db.model.CrawlExecutionStatus;
import no.nb.nna.broprox.db.model.QueuedUri;
import org.netpreserve.commons.uri.UriConfigs;
import org.openjdk.jol.info.GraphLayout;

/**
 *
 */
public class CrawlExecution implements Callable<Void>, ForkJoinPool.ManagedBlocker {

    private final CrawlExecutionStatus status;

    private final Frontier frontier;

    private final CrawlConfig config;

    private String currentUri;

    private volatile QueuedUri[] outlinks;

    public CrawlExecution(Frontier frontier, CrawlExecutionStatus status, CrawlConfig config) {
        System.out.println("NEW CRAWL EXECUTION: " + status.getId());
        this.frontier = frontier;
        this.status = status;
        this.config = config;
    }

    public String getId() {
        return status.getId();
    }

    public CrawlConfig getConfig() {
        return config;
    }

    public String getCurrentUri() {
        return currentUri;
    }

    public void setCurrentUri(String currentUri) {
        this.currentUri = currentUri;
        this.outlinks = null;
    }

    @Override
    public Void call() throws Exception {
        return null;
    }

    public void fetch() {
        System.out.println("Fetching " + currentUri);
        QueuedUri qUri = DbObjectFactory.create(QueuedUri.class)
                .withExecutionId(getId())
                .withCrawlConfig(config)
                .withUri(currentUri)
                .withSurt(UriConfigs.SURT_KEY.buildUri(currentUri).toString())
                .withDiscoveryPath("");

        try {
            try {
                outlinks = frontier.getHarvesterClient().fetchPage(qUri);

                status.withState(CrawlExecutionStatus.State.RUNNING)
                        .withDocumentsCrawled(status.getDocumentsCrawled() + 1);
                frontier.getDb().updateExecutionStatus(status);

            } catch (Exception e) {
                System.out.println("Error fetching page (" + currentUri + "): " + e);
                // should do some logging and updating here
            }

            if (outlinks == null || outlinks.length == 0) {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!! BOTTOM REACHED: " + outlinks);
                outlinks = new QueuedUri[0];
                return;
            }

            for (QueuedUri outUri : outlinks) {
                try {
                    outUri.withSurt(UriConfigs.SURT_KEY.buildUri(outUri.getUri()).toString());
                } catch (Exception e) {
                    System.out.println("Strange error with outlink (" + outUri + "): " + e);
                    e.printStackTrace();
                    return;
                }
//                System.out.println(" >> " + outUri.toJson() + " ::: " + queueProcessor.alreadeyIncluded(outUri));
                if (shouldInclude(outUri)) {
                    try {
                        frontier.getDb().addQueuedUri(outUri);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Strange error fetching page (" + currentUri + "): " + e);
            e.printStackTrace();
            // should do some logging and updating here
        }
        return;
    }

    boolean shouldInclude(QueuedUri outlink) {
        if (!frontier.alreadeyIncluded(outlink)) {
            if (outlink.getSurt().startsWith(config.getScope())) {
                return true;
            }
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
        if (outlinks == null) {
            fetch();
        }
        return true;
    }

    @Override
    public boolean isReleasable() {
        return outlinks != null;
    }

    public boolean isDone() {
        return outlinks.length == 0;
    }

}
