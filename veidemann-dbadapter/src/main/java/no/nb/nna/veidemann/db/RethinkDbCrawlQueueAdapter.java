package no.nb.nna.veidemann.db;

import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.db.CrawlQueueAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RethinkDbCrawlQueueAdapter implements CrawlQueueAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbCrawlQueueAdapter.class);
    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    private final long expirationSeconds = 3600;
    private final int lockExpirationSeconds = 3600;

    public RethinkDbCrawlQueueAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    private Key createKey(String crawlHostGroupId, String politenessId) {
        return new Key("chg", crawlHostGroupId + ":" + politenessId);
    }

    private Key createKey(List<String> crawlHostGroupId) {
        return createKey(crawlHostGroupId.get(0), crawlHostGroupId.get(1));
    }

    @Override
    public QueuedUri addToCrawlHostGroup(QueuedUri qUri) throws DbException {
        String crawlHostGroupId = qUri.getCrawlHostGroupId();
        String politenessId = qUri.getPolitenessId();
        Objects.requireNonNull(crawlHostGroupId, "CrawlHostGroupId cannot be null");
        Objects.requireNonNull(politenessId, "PolitenessId cannot be null");
        if (qUri.getSequence() <= 0L) {
            throw new IllegalArgumentException("Sequence must be a positive number");
        }

        DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroupId, politenessId), lockExpirationSeconds);
        lock.lock();
        try {
            if (!qUri.hasEarliestFetchTimeStamp()) {
                qUri = qUri.toBuilder().setEarliestFetchTimeStamp(ProtoUtils.getNowTs()).build();
            }
            OffsetDateTime earliestFetchTimeStamp = ProtoUtils.tsToOdt(qUri.getEarliestFetchTimeStamp());

            List key = r.array(crawlHostGroupId, politenessId);
            conn.exec("db-addToCrawlHostGroup",
                    r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .insert(r.hashMap("id", key)
                                    .with("nextFetchTime", earliestFetchTimeStamp)
                                    .with("busy", false))
                            .optArg("return_changes", false)
                            .optArg("conflict", (id, old_doc, new_doc) -> old_doc)
                            .optArg("durability", "hard")
            );

            Map rMap = ProtoUtils.protoToRethink(qUri);
            return conn.executeInsert("db-saveQueuedUri",
                    r.table(Tables.URI_QUEUE.name)
                            .insert(rMap)
                            .optArg("conflict", "replace"),
                    QueuedUri.class
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() throws DbException {
        try (Cursor<Map<String, Object>> response = conn.exec("db-borrowFirstReadyCrawlHostGroup",
                r.table(Tables.CRAWL_HOST_GROUP.name)
                        .orderBy().optArg("index", "nextFetchTime")
                        .between(r.minval(), r.now()).optArg("right_bound", "closed")
        )) {

            for (Map<String, Object> chgDoc : response) {
                List chgId = (List) chgDoc.get("id");

                DistributedLock lock = conn.createDistributedLock(createKey(chgId), lockExpirationSeconds);
                lock.lock();
                try {
                    Map<String, Object> borrowResponse = conn.exec("db-borrowFirstReadyCrawlHostGroup",
                            r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                    .get(chgId)
                                    .replace(d ->
                                            r.branch(
                                                    // CrawlHostGroup doesn't exist, return null
                                                    d.eq(null),
                                                    null,

                                                    // Busy is false or expired. This is the one we want, ensure busy is false and return it
                                                    d.g("busy").eq(false).or(d.g("expires").lt(r.now())),
                                                    d.merge(r.hashMap("busy", false).with("expires", r.now())),

                                                    // The CrawlHostGroup is busy, return it unchanged
                                                    d
                                            ))
                                    .optArg("return_changes", true)
                                    .optArg("durability", "hard")
                    );

                    if (borrowResponse == null) {
                        return FutureOptional.empty();
                    } else {
                        long replaced = (long) borrowResponse.get("replaced");
                        if (replaced == 1L) {
                            Map<String, Object> newDoc = ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val");
                            long queueCount = crawlHostGroupQueueCount(chgId);

                            if (queueCount == 0L) {
                                // No more URIs in queue for this CrawlHostGroup, delete it
                                conn.exec("db-deleteCrawlHostGroup",
                                        r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                                .get(chgId)
                                                .delete()
                                                .optArg("return_changes", false)
                                                .optArg("durability", "hard")
                                );
                            } else {
                                newDoc.put("busy", true);
                                newDoc.put("expires", r.now().add(expirationSeconds));
                                CrawlHostGroup chg = buildCrawlHostGroup(newDoc, queueCount);

                                conn.exec("db-saveCrawlHostGroup",
                                        r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                                                .get(chgId)
                                                .replace(newDoc)
                                                .optArg("return_changes", false)
                                                .optArg("durability", "hard")
                                );
                                return FutureOptional.of(chg);
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        return FutureOptional.empty();
    }

    @Override
    public void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) throws DbException {
        List key = r.array(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId());
        double nextFetchDelayS = nextFetchDelayMs / 1000.0;

        DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId()), lockExpirationSeconds);
        lock.lock();
        try {
            Map<String, Object> response = conn.exec("db-releaseCrawlHostGroup",
                    r.table(Tables.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .replace(d ->
                                    r.branch(
                                            // CrawlHostGroup doesn't exist, return null
                                            d.eq(null),
                                            null,

                                            // CrawlHostGroup is busy, release it
                                            d.g("busy").eq(true),
                                            d.merge(r.hashMap("busy", false)
                                                    .with("expires", null)
                                                    .with("nextFetchTime", r.now().add(nextFetchDelayS))),

                                            // The CrawlHostGroup is already released probably because it was expired.
                                            d
                                    ))
                            .optArg("return_changes", false)
                            .optArg("durability", "hard")
            );

            long replaced = (long) response.get("replaced");
            if (replaced != 1L) {
                LOG.warn("Could not release crawl host group");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long deleteQueuedUrisForExecution(String executionId) throws DbException {
        long deleted = 0;

        List<Map<String, String>> chgKeys = conn.exec(
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .pluck("crawlHostGroupId", "politenessId")
                        .distinct()
        );


        for (Map<String, String> group : chgKeys) {
            List<String> startKey = r.array(group.get("crawlHostGroupId"), group.get("politenessId"), r.minval(), r.minval());
            List<String> endKey = r.array(group.get("crawlHostGroupId"), group.get("politenessId"), r.maxval(), r.maxval());

            DistributedLock lock = conn.createDistributedLock(createKey(group.get("crawlHostGroupId"), group.get("politenessId")), lockExpirationSeconds);
            lock.lock();
            try {
                long deleteResponse = conn.exec("db-deleteQueuedUrisForExecution",
                        r.table(Tables.URI_QUEUE.name)
                                .between(startKey, endKey)
                                .optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                                .filter(row -> row.g("executionId").eq(executionId))
                                .delete()
                                .g("deleted")
                );
                deleted += deleteResponse;
            } finally {
                lock.unlock();
            }
        }

        return deleted;
    }

    @Override
    public long queuedUriCount(String executionId) throws DbException {
        return conn.exec("db-queuedUriCount",
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .count()
        );
    }

    @Override
    public boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException {
        return conn.exec("db-uriNotIncludedInQueue",
                r.table(Tables.CRAWL_LOG.name)
                        .between(
                                r.array(qu.getSurt(), ProtoUtils.tsToOdt(since)),
                                r.array(qu.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(Tables.URI_QUEUE.name).getAll(qu.getSurt())
                                        .optArg("index", "surt")
                                        .limit(1)
                        ).isEmpty());
    }

    @Override
    public FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup) throws DbException {
        List fromKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.minval(),
                r.minval()
        );

        List toKey = r.array(
                crawlHostGroup.getId(),
                crawlHostGroup.getPolitenessId(),
                r.maxval(),
                r.now()
        );

        List<Map<String, String>> eids = conn.exec("db-getNextQueuedUriToFetch",
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                        .between(fromKey, toKey)
                        .pluck("executionId")
                        .distinct());

        // Compute the total weight of all items together
        // TODO: For now all executions have same weight. That should be configurable on a per job basis
        double totalWeight = 0.0d;
        for (Map<String, String> i : eids) {
//            totalWeight += i.getWeight();
            totalWeight += 1;
        }
        // Now choose a random item
        int randomIndex = -1;
        double random = Math.random() * totalWeight;
        for (int i = 0; i < eids.size(); ++i) {
//            random -= items[i].getWeight();
            random -= 1;
            if (random <= 0.0d) {
                randomIndex = i;
                break;
            }
        }
        String randomExecutionId = eids.get(randomIndex).get("executionId");


        try (Cursor<Map<String, Object>> cursor = conn.exec("db-getNextQueuedUriToFetch",
                r.table(Tables.URI_QUEUE.name).optArg("read_mode", "majority")
                        .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                        .between(fromKey, toKey)
                        .filter(r.hashMap("executionId", randomExecutionId))
                        .limit(1))) {

            if (cursor.hasNext()) {
                QueuedUri qUri = ProtoUtils.rethinkToProto(cursor.next(), QueuedUri.class);
                // URI is ready to be processed, remove it from queue
                DistributedLock lock = conn.createDistributedLock(createKey(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId()), lockExpirationSeconds);
                lock.lock();
                try {
                    conn.exec("db-setTimeoutForQueuedUri",
                            r.table(Tables.URI_QUEUE.name)
                                    .get(qUri.getId())
                                    .update(r.hashMap("earliestFetchTimeStamp", r.now().add(1800)))
                    );
                } finally {
                    lock.unlock();
                }

                return FutureOptional.of(qUri);
            }
        }
        return FutureOptional.empty();
    }

    private CrawlHostGroup buildCrawlHostGroup(Map<String, Object> resultDoc, long queueCount) throws DbException {
        CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder()
                .setId(((List<String>) resultDoc.get("id")).get(0))
                .setPolitenessId(((List<String>) resultDoc.get("id")).get(1))
                .setNextFetchTime(ProtoUtils.odtToTs((OffsetDateTime) resultDoc.get("nextFetchTime")))
                .setBusy((boolean) resultDoc.get("busy"))
                .setQueuedUriCount(queueCount);

        return chg.build();
    }

    public long crawlHostGroupQueueCount(List<String> crawlHostGroupId) throws DbQueryException, DbConnectionException {
        String chgId = crawlHostGroupId.get(0);
        String politenessId = crawlHostGroupId.get(1);

        List fromKey = r.array(
                chgId,
                politenessId,
                r.minval(),
                r.minval()
        );

        List toKey = r.array(
                chgId,
                politenessId,
                r.maxval(),
                r.maxval()
        );

        return conn.exec("", r.table(Tables.URI_QUEUE.name)
                .optArg("read_mode", "majority")
                .between(fromKey, toKey).optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                .count());
    }

}
