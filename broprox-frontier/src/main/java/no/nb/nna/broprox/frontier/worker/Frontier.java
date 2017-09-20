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

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnels;
import com.rethinkdb.RethinkDB;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.commons.FailedFetchCodes;
import no.nb.nna.broprox.commons.client.DnsServiceClient;
import no.nb.nna.broprox.commons.client.RobotsServiceClient;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import no.nb.nna.broprox.model.MessagesProto;
import no.nb.nna.broprox.model.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Frontier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Frontier.class);

    private static final int EXPECTED_MAX_URIS = 1000000;

    private final RethinkDbAdapter db;

    private final HarvesterClient harvesterClient;

    private final RobotsServiceClient robotsServiceClient;

    private final DnsServiceClient dnsServiceClient;

    static final RethinkDB r = RethinkDB.r;

    static final ForkJoinPool EXECUTOR_SERVICE = new ForkJoinPool(32);

    CuckooFilter<CharSequence> alreadyIncluded;

    public Frontier(RethinkDbAdapter db, HarvesterClient harvesterClient, RobotsServiceClient robotsServiceClient, DnsServiceClient dnsServiceClient) {
        this.db = db;
        this.harvesterClient = harvesterClient;
        this.robotsServiceClient = robotsServiceClient;
        this.dnsServiceClient = dnsServiceClient;
        this.alreadyIncluded = new CuckooFilter.Builder<>(Funnels.unencodedCharsFunnel(), EXPECTED_MAX_URIS).build();

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
        // Create execution
        CrawlExecutionStatus status = CrawlExecutionStatus.newBuilder()
                .setJobId(job.getId())
                .setSeedId(seed.getId())
                .setState(CrawlExecutionStatus.State.CREATED)
                .setScope(seed.getScope())
                .build();
        status = getDb().addExecutionStatus(status);
        LOG.debug("New crawl execution: " + status.getId());

        String uri = seed.getMeta().getName();
        MessagesProto.QueuedUriOrBuilder qUri = QueuedUri.newBuilder()
                .setUri(uri);

        qUri = enrichUri(qUri, status.getId(), 1L, job.getCrawlConfig());
        qUri = addUriToQueue(qUri);

        return status;
    }

    public QueuedUri enrichUri(MessagesProto.QueuedUriOrBuilder qUri, String executionId, long sequence,
            ConfigProto.CrawlConfig config) {

        QueuedUri.Builder qUriBuilder;
        if (qUri instanceof QueuedUri.Builder) {
            qUriBuilder = (QueuedUri.Builder) qUri;
        } else {
            qUriBuilder = ((QueuedUri) qUri).toBuilder();
        }

        Uri surt = null;
        try {
            surt = UriConfigs.SURT_KEY.buildUri(qUri.getUri());
            LOG.debug("Resolve ip for URI '{}'", qUri.getUri());

            String ip = getDnsServiceClient()
                    .resolve(surt.getHost(), surt.getDecodedPort())
                    .getAddress()
                    .getHostAddress();

            List<ConfigProto.CrawlHostGroupConfig> groupConfigs = getDb()
                    .listCrawlHostGroupConfigs(ControllerProto.ListRequest.newBuilder()
                            .setSelector(config.getPoliteness().getCrawlHostGroupSelector()).build()).getValueList();
            String crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroup(ip, groupConfigs);
            getDb().getOrCreateCrawlHostGroup(crawlHostGroupId, config.getPoliteness().getId());

            qUriBuilder.setIp(ip)
                    .setSurt(surt.toString())
                    .setCrawlHostGroupId(crawlHostGroupId)
                    .setPolitenessId(config.getPoliteness().getId())
                    .setExecutionId(executionId)
                    .setSequence(sequence);


        } catch (UnknownHostException ex) {
            LOG.error("Failed ip resolution for URI '{}' by extracting host '{}' and port '{}'",
                    qUri.getUri(),
                    surt.getHost(),
                    surt.getDecodedPort());

            qUriBuilder.setError(FailedFetchCodes.FAILED_DNS.toFetchError(ex.toString()));
        }
        return qUriBuilder.build();
    }

    public QueuedUri addUriToQueue(MessagesProto.QueuedUriOrBuilder qUri) {
        if (qUri instanceof QueuedUri.Builder) {
            return getDb().addQueuedUri(((QueuedUri.Builder) qUri).build());
        } else {
            return getDb().addQueuedUri((QueuedUri) qUri);
        }
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

    public RobotsServiceClient getRobotsServiceClient() {
        return robotsServiceClient;
    }

    public DnsServiceClient getDnsServiceClient() {
        return dnsServiceClient;
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
