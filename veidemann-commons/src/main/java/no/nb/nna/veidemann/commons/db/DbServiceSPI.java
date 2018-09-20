package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.settings.CommonSettings;

import java.util.List;

public interface DbServiceSPI extends AutoCloseable {
    void connect(CommonSettings settings) throws DbConnectionException;

    @Override
    void close();

    DbAdapter getDbAdapter();

    ConfigAdapter getConfigAdapter();

    CrawlQueueAdapter getCrawlQueueAdapter();

    ExecutionsAdapter getExecutionsAdapter();

    DbInitializer getDbInitializer();

    DistributedLock createDistributedLock(Key key, int expireSeconds);

    List<Key> listExpiredDistributedLocks(String domain) throws DbQueryException, DbConnectionException;

    List<Key> listExpiredDistributedLocks() throws DbQueryException, DbConnectionException;
}
