package no.nb.nna.veidemann.commons.db;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {
    void lock() throws DbQueryException, DbConnectionException;

    void lockInterruptibly() throws DbQueryException, DbConnectionException, InterruptedException;

    boolean tryLock() throws DbQueryException, DbConnectionException;

    boolean tryLock(long timeout, TimeUnit unit) throws DbQueryException, DbConnectionException, InterruptedException;

    void unlock() throws DbQueryException, DbConnectionException;

    String getInstanceId();

    Key getKey();

    class Key {
        final String domain;
        final String key;

        public Key(String domain, String key) {
            this.domain = domain;
            this.key = key;
        }

        public String getDomain() {
            return domain;
        }

        public String getKey() {
            return key;
        }
    }

    class NotOwnerException extends DbQueryException {
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
