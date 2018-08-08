package no.nb.nna.veidemann.db;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.db.CrawlQueueAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
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

    public RethinkDbCrawlQueueAdapter(RethinkDbConnection conn) {
        this.conn = conn;
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

        try (Lock lock = aquireLock(crawlHostGroupId, politenessId)) {
            if (!qUri.hasEarliestFetchTimeStamp()) {
                qUri = qUri.toBuilder().setEarliestFetchTimeStamp(ProtoUtils.getNowTs()).build();
            }
            return ((RethinkDbAdapter) conn.getDbAdapter()).saveMessage(qUri, TABLES.URI_QUEUE);
        } catch (InterruptedException e) {
            LOG.info("addToCrawlHostGroup was interrupted");
            throw new RuntimeException(e);
        }
    }

    @Override
    public FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() throws DbException {
        OffsetDateTime nextReadyTime = null;

        try (Cursor<Map<String, Object>> response = conn.exec("db-borrowFirstReadyCrawlHostGroup",
                r.table(TABLES.CRAWL_HOST_GROUP.name)
                        .orderBy().optArg("index", "nextFetchTime")
                        .between(r.minval(), r.now()).optArg("right_bound", "closed")
                        .filter(r.hashMap("busy", false))
        )) {

            for (Map<String, Object> chgDoc : response) {
                nextReadyTime = null;
                List key = (List) chgDoc.get("id");

                try (Lock lock = aquireLockIfexists(key)) {
                    if (lock.key == null) {
                        return FutureOptional.empty();
                    }

                    long queueCount = lock.queueCount();

                    if (!lock.isBusy() && queueCount == 0L) {
                        lock.delete();
                    } else if (!lock.isBusy()) {
                        lock.setBusy(true);
                        CrawlHostGroup chg = buildCrawlHostGroup(lock.chgDoc, queueCount);
                        return FutureOptional.of(chg);
                    } else {
                        if (nextReadyTime == null) {
                            nextReadyTime = (OffsetDateTime) chgDoc.get("nextFetchTime");
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.info("borrowFirstReadyCrawlHostGroup was interrupted");
                    return FutureOptional.empty();
                }
            }
        }

        if (nextReadyTime == null) {
            return FutureOptional.empty();
        } else {
            return FutureOptional.emptyUntil(nextReadyTime);
        }
    }

    @Override
    public CrawlHostGroup releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) throws DbException {
        List key = r.array(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId());
        double nextFetchDelayS = nextFetchDelayMs / 1000.0;

        Map<String, Object> response = null;

        try (Lock lock = aquireLock(key)) {
            lock.setBusy(false);
            lock.setFetchDelay(nextFetchDelayS);
            response = lock.release(true);
            return buildCrawlHostGroup(response, 0L);
        } catch (InterruptedException e) {
            LOG.info("releaseCrawlHostGroup was interrupted");
            throw new RuntimeException(e);
        }
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

    @Override
    public long deleteQueuedUrisForExecution(String executionId) throws DbException {
        long deleted = 0;

        List<Map<String, List>> response = conn.exec("db-deleteQueuedUrisForExecution",
                r.table(TABLES.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .group("crawlHostGroupId", "politenessId")
                        .pluck("id"));

        for (Map<String, List> group : response) {
            List<String> key = group.get("group");
            List<Map> values = group.get("values");

            try (Lock lock = aquireLock(key)) {
                for (Map qUri : values) {
                    long deleteResponse = conn.exec("db-deleteQueuedUrisForExecution",
                            r.table(TABLES.URI_QUEUE.name)
                                    .get(qUri.get("id"))
                                    .delete()
                                    .g("deleted")
                    );
                    deleted += deleteResponse;
                    System.out.println("DELETED: " + deleted + ", RESP: " + deleteResponse);
                }
            } catch (InterruptedException e) {
                LOG.info("getNextQueuedUriToFetch was interrupted");
                throw new RuntimeException(e);
            }
        }

        return deleted;
    }

    @Override
    public long queuedUriCount(String executionId) throws DbException {
        return conn.exec("db-queuedUriCount",
                r.table(TABLES.URI_QUEUE.name).optArg("read_mode", "majority")
                        .getAll(executionId).optArg("index", "executionId")
                        .count()
        );
    }

    @Override
    public boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException {
        return conn.exec("db-uriNotIncludedInQueue",
                r.table(RethinkDbAdapter.TABLES.CRAWL_LOG.name)
                        .between(
                                r.array(qu.getSurt(), ProtoUtils.tsToOdt(since)),
                                r.array(qu.getSurt(), r.maxval()))
                        .optArg("index", "surt_time").filter(row -> row.g("statusCode").lt(500)).limit(1)
                        .union(
                                r.table(RethinkDbAdapter.TABLES.URI_QUEUE.name).getAll(qu.getSurt())
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
                r.maxval()
        );

        try (Cursor<Map<String, Object>> cursor = conn.exec("db-getNextQueuedUriToFetch",
                r.table(TABLES.URI_QUEUE.name).optArg("read_mode", "majority")
                        .orderBy().optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                        .between(fromKey, toKey)
                        .limit(1))) {

            if (cursor.hasNext()) {
                QueuedUri qUri = ProtoUtils.rethinkToProto(cursor.next(), QueuedUri.class);
                if (Timestamps.comparator().compare(qUri.getEarliestFetchTimeStamp(), ProtoUtils.getNowTs()) <= 0) {
                    // URI is ready to be processed, remove it from queue
                    try (Lock lock = aquireLock(crawlHostGroup.getId(), crawlHostGroup.getPolitenessId())) {
                        ((RethinkDbAdapter) conn.getDbAdapter()).deleteConfigMessage(qUri, TABLES.URI_QUEUE);
                    } catch (InterruptedException e) {
                        LOG.info("getNextQueuedUriToFetch was interrupted");
                        throw new RuntimeException(e);
                    }

                    return FutureOptional.of(qUri);
                } else {
                    return FutureOptional.emptyUntil(ProtoUtils.tsToOdt(qUri.getEarliestFetchTimeStamp()));
                }
            }
        }
        return FutureOptional.empty();
    }

    private Lock aquireLock(String crawlHostGroupId, String politenessId) throws DbQueryException, DbConnectionException, InterruptedException {
        Objects.requireNonNull(crawlHostGroupId, "CrawlHostGroupId cannot be null");
        Objects.requireNonNull(politenessId, "PolitenessId cannot be null");

        List key = r.array(crawlHostGroupId, politenessId);
        return aquireLock(key);
    }

    private Lock aquireLock(List key) throws DbQueryException, DbConnectionException, InterruptedException {
        while (true) {
            Map<String, Object> borrowResponse = conn.exec("db-aquireLock",
                    r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .replace(d ->
                                    r.branch(
                                            // CrawlHostGroup doesn't exist, create new
                                            r.not(d), r.hashMap("id", key)
                                                    .with("nextFetchTime", r.now())
                                                    .with("busy", false)
                                                    .with("lock", true),

                                            // This is the one we want, set lock to true and return it
                                            d.g("lock").eq(false), d.merge(r.hashMap("lock", true)),
                                            // The CrawlHostGroup is locked, return it unchanged
                                            d
                                    ))
                            .optArg("return_changes", true)
                            .optArg("durability", "hard")
            );

            long replaced = (long) borrowResponse.get("replaced");
            long created = (long) borrowResponse.get("inserted");
            if (replaced == 1L || created == 1L) {
                return new Lock(key, ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val"));
            }
            Thread.sleep(100);
        }
    }

    private Lock aquireLockIfexists(List key) throws DbQueryException, DbConnectionException, InterruptedException {
        while (true) {
            Map<String, Object> borrowResponse = conn.exec("db-aquireLock",
                    r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .replace(d ->
                                    r.branch(
                                            // CrawlHostGroup doesn't exist, return null
                                            r.not(d), null,

                                            // This is the one we want, set lock to true and return it
                                            d.g("lock").eq(false), d.merge(r.hashMap("lock", true)),
                                            // The CrawlHostGroup is locked, return it unchanged
                                            d
                                    ))
                            .optArg("return_changes", true)
                            .optArg("durability", "hard")
            );

            if (borrowResponse != null) {
                long replaced = (long) borrowResponse.get("replaced");
                long created = (long) borrowResponse.get("inserted");
                if (replaced == 1L || created == 1L) {
                    return new Lock(key, ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val"));
                }
            } else {
                return new Lock(null, null);
            }
            Thread.sleep(100);
        }
    }

    private class Lock implements AutoCloseable {
        final List key;
        final Map<String, Object> chgDoc;
        boolean processed = false;

        public Lock(List key, Map<String, Object> chgDoc) {
            this.key = key;
            this.chgDoc = chgDoc;
        }

        public boolean isBusy() {
            return (boolean) chgDoc.get("busy");
        }

        public void setBusy(boolean busy) {
            chgDoc.put("busy", busy);
        }

        public void setFetchDelay(double nextFetchDelayS) {
            chgDoc.put("nextFetchTime", r.now().add(nextFetchDelayS));
        }

        public long queueCount() throws DbQueryException, DbConnectionException {
            String chgId = ((List<String>) chgDoc.get("id")).get(0);
            String politenessId = ((List<String>) chgDoc.get("id")).get(1);

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

            long queueCount = conn.exec("", r.table(TABLES.URI_QUEUE.name)
                    .optArg("read_mode", "majority")
                    .between(fromKey, toKey).optArg("index", "crawlHostGroupKey_sequence_earliestFetch")
                    .count());

            return queueCount;
        }

        @Override
        public void close() throws DbQueryException, DbConnectionException {
            release(false);
        }

        public Map<String, Object> release(boolean returnChanges) throws DbQueryException, DbConnectionException {
            if (key == null || processed) {
                return null;
            }

            chgDoc.put("lock", false);

            Map<String, Object> borrowResponse = conn.exec("db-releaseLock",
                    r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .replace(d ->
                                    r.branch(
                                            // This is the one we want, set lock to true and return it
                                            d.g("lock").eq(true), d.merge(chgDoc),
                                            // The CrawlHostGroup is locked, return it unchanged
                                            d
                                    ))
                            .optArg("return_changes", returnChanges ? "always" : false)
                            .optArg("durability", "hard")
            );

            long replaced = (long) borrowResponse.get("replaced");
            if (replaced != 1L) {
                throw new DbQueryException("Failed releasing lock for " + key.toString());
            }

            processed = true;

            if (returnChanges) {
                return ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0).get("new_val");
            } else {
                return null;
            }
        }

        public void delete() throws DbQueryException, DbConnectionException {
            conn.exec("db-deleteLock",
                    r.table(TABLES.CRAWL_HOST_GROUP.name).optArg("read_mode", "majority")
                            .get(key)
                            .delete()
                            .optArg("return_changes", false)
                            .optArg("durability", "hard")
            );
            processed = true;
        }
    }
}
