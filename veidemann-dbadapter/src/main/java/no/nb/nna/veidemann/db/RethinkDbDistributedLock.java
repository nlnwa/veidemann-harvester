package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DistributedLock;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RethinkDbDistributedLock implements DistributedLock {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbDistributedLock.class);
    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;
    private final Key key;
    private final List<String> dbKey;
    private final int expireSeconds;
    private final String instanceId;


    public RethinkDbDistributedLock(RethinkDbConnection conn, Key key, int expireSeconds) {
        Objects.requireNonNull(conn, "Database connection cannot be null");
        Objects.requireNonNull(key, "Lock key cannot be null");
        this.conn = conn;
        this.key = key;
        this.dbKey = r.array(key.getDomain());
        if ((key.getDomain().getBytes().length + key.getKey().getBytes().length) > 120) {
            this.dbKey.add(ApiTools.createSha1Digest(key.getKey()));
        } else {
            this.dbKey.add(key.getKey());
        }
        this.expireSeconds = expireSeconds;
        this.instanceId = UUID.randomUUID().toString();
    }

    public void lock() throws DbQueryException, DbConnectionException {
        long start = System.currentTimeMillis();
        while (true) {
            if (tryLock()) {
                return;
            }
            try {
                if ((System.currentTimeMillis() - start) > 15000) {
                    LOG.warn("Object has been locked for more than 15s. Lock: {}:{}", key.getDomain(), key.getKey(), new RuntimeException());
                    start = System.currentTimeMillis();
                }
                Thread.sleep(200);
            } catch (InterruptedException e) {
            }
        }
    }

    public void lockInterruptibly() throws DbQueryException, DbConnectionException, InterruptedException {
        while (true) {
            if (tryLock()) {
                return;
            }
            Thread.sleep(200);
        }
    }

    public boolean tryLock() throws DbQueryException, DbConnectionException {
        Map<String, Object> borrowResponse = conn.exec("db-aquireLock",
                r.table(Tables.LOCKS.name).optArg("read_mode", "majority")
                        .get(dbKey)
                        .replace(d ->
                                r.branch(
                                        // Lock doesn't exist, create new
                                        d.eq(null),
                                        r.hashMap("id", dbKey)
                                                .with("key", key)
                                                .with("expires", r.now().add(expireSeconds))
                                                .with("instanceId", instanceId),

                                        // Lock is owned by this instance, update expires
                                        d.g("instanceId").eq(instanceId),
                                        d.merge(r.hashMap("expires", r.now().add(expireSeconds))),

                                        // Lock is false or expired, set this instance as owner
                                        d.g("expires").lt(r.now()),
                                        d.merge(r.hashMap("expires", r.now().add(expireSeconds))
                                                .with("instanceId", instanceId)),

                                        // The CrawlHostGroup is locked by another instance, return it unchanged
                                        d
                                ))
                        .optArg("return_changes", true)
                        .optArg("durability", "hard")
        );

        long replaced = (long) borrowResponse.get("replaced");
        long created = (long) borrowResponse.get("inserted");

        if (replaced == 1L) {
            Map<String, Map> changes = ((List<Map<String, Map>>) borrowResponse.get("changes")).get(0);
            Map oldVal = changes.get("old_val");
            Map newVal = changes.get("new_val");
            if (oldVal.get("instanceId").equals(newVal.get("instanceId"))) {
                LOG.debug("Lock is already owned by this instance");
            } else {
                LOG.debug("Lock is expired");
            }

            return true;
        }

        if (created == 1L) {
            return true;
        }
        return false;
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws DbQueryException, DbConnectionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (tryLock()) {
                return true;
            }
            if (System.currentTimeMillis() > (startTime + unit.toMillis(timeout))) {
                return false;
            }
            Thread.sleep(200);
        }
    }

    public void unlock() throws DbQueryException, DbConnectionException {
        Map<String, Object> borrowResponse = conn.exec("db-releaseLock",
                r.table(Tables.LOCKS.name).optArg("read_mode", "majority")
                        .get(dbKey)
                        .replace(d ->
                                r.branch(
                                        // Lock is already released, return null
                                        d.eq(null),
                                        null,

                                        // Lock is owned by this instance, delete it by returning null
                                        d.g("instanceId").eq(instanceId),
                                        null,

                                        // The CrawlHostGroup is locked, return it unchanged
                                        d
                                ))
                        .optArg("return_changes", "always")
                        .optArg("durability", "hard")
        );

        long deleted = (long) borrowResponse.get("deleted");
        long unchanged = (long) borrowResponse.get("unchanged");
        long skipped = (long) borrowResponse.get("skipped");

        if (skipped == 1L) {
            LOG.debug("Lock is already released");
        } else if (unchanged == 1L) {
            String otherInstanceId = ((List<Map<String, Map<String, String>>>) borrowResponse.get("changes"))
                    .get(0).get("new_val").get("instanceId");
            throw new NotOwnerException("Lock for '" + key + "' owned by instance '" + otherInstanceId + "'");
        } else if (deleted != 1L) {
            throw new DbQueryException("Failed releasing lock for " + key);
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Key getKey() {
        return key;
    }

    public static List<Key> listExpiredDistributedLocks(RethinkDbConnection conn) throws DbQueryException, DbConnectionException {
        try (Cursor<List<String>> listResponse = conn.exec("db-listExpiredLocks",
                r.table(Tables.LOCKS.name).optArg("read_mode", "majority")
                        .optArg("durability", "hard")
                        .filter(l -> l.g("expires").ge(r.now()))
                        .pluck("id")
        )) {

            return cursorToList(listResponse);
        }
    }

    public static List<Key> listExpiredDistributedLocks(RethinkDbConnection conn, String domain) throws DbQueryException, DbConnectionException {
        try (Cursor<List<String>> listResponse = conn.exec("db-listExpiredLocks",
                r.table(Tables.LOCKS.name).optArg("read_mode", "majority")
                        .between(r.array(domain, r.minval()), r.array(domain, r.maxval()))
                        .optArg("durability", "hard")
                        .filter(l -> l.g("expires").ge(r.now()))
                        .pluck("id")
        )) {

            return cursorToList(listResponse);
        }
    }

    private static List<Key> cursorToList(Cursor<List<String>> listCursor) {
        List<Key> result = new ArrayList<>();

        for (List<String> l : listCursor) {
            result.add(new Key(l.get(0), l.get(1)));
        }
        return result;
    }
}
