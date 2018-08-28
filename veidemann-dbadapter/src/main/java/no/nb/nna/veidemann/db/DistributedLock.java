package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DistributedLock {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);
    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;
    private final String key;
    private final String hashedKey;
    private final int expireSeconds;
    private final String instanceId;


    public DistributedLock(RethinkDbConnection conn, String key, int expireSeconds) {
        Objects.requireNonNull(conn, "Database connection cannot be null");
        Objects.requireNonNull(key, "Lock key cannot be null");
        this.conn = conn;
        this.key = key;
        this.hashedKey = ApiTools.createSha1Digest(key);
        this.expireSeconds = expireSeconds;
        this.instanceId = UUID.randomUUID().toString();
    }

    public DistributedLock(RethinkDbConnection conn, String key) {
        this(conn, key, 60 * 60);
    }

    public void lock() throws DbQueryException, DbConnectionException {
        while (true) {
            if (tryLock()) {
                return;
            }
            try {
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
                        .get(hashedKey)
                        .replace(d ->
                                r.branch(
                                        // Lock doesn't exist, create new
                                        d.eq(null),
                                        r.hashMap("id", hashedKey)
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
                        .get(hashedKey)
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

    public String getKey() {
        return key;
    }

    public static class NotOwnerException extends DbQueryException {
        public NotOwnerException() {
        }

        public NotOwnerException(String message) {
            super(message);
        }

        public NotOwnerException(String message, Throwable cause) {
            super(message, cause);
        }

        public NotOwnerException(Throwable cause) {
            super(cause);
        }
    }
}
