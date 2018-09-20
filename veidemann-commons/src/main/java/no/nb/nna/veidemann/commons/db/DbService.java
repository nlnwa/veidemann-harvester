package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.commons.db.DistributedLock.Key;
import no.nb.nna.veidemann.commons.settings.CommonSettings;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class DbService implements AutoCloseable {
    private static DbService instance;

    private final DbServiceSPI service;

    private DbService(CommonSettings settings) throws DbConnectionException {
        ServiceLoader<DbServiceSPI> dbServiceLoader = ServiceLoader.load(DbServiceSPI.class);
        Iterator<DbServiceSPI> services = dbServiceLoader.iterator();
        if (!services.hasNext()) {
            throw new DbConnectionException("No database adapter found");
        }
        this.service = services.next();
        if (services.hasNext()) {
            throw new DbConnectionException("More than one database adapter found");
        }

        this.service.connect(settings);
    }

    private DbService(DbServiceSPI service) {
        this.service = service;
    }

    /**
     * Get the singleton instance.
     *
     * @return the single RethinkDbConnection instance
     */
    public static synchronized DbService getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Connection is not configured");
        }
        return instance;
    }

    /**
     * Configure the singleton RethinkDbConnection.
     * <p/>
     * This method must be called before any usage.
     *
     * @param settings a {@link CommonSettings} object with connection parameters
     */
    public static synchronized DbService configure(CommonSettings settings) throws DbConnectionException {
        if (instance != null) {
            throw new IllegalStateException("Connection is already configured");
        }
        instance = new DbService(settings);
        return instance;
    }

    /**
     * Copnfigure the singleton DbService with a preconfigured service provider.
     * <p>
     * This method is mostly useful for unit tests giving the possibility of mocking db.
     *
     * @param service
     * @return
     */
    public static synchronized DbService configure(DbServiceSPI service) {
        if (instance != null) {
            throw new IllegalStateException("Connection is already configured");
        }
        instance = new DbService(service);
        return instance;
    }

    public static boolean isConfigured() {
        return instance != null;
    }

    public DbAdapter getDbAdapter() {
        return service.getDbAdapter();
    }

    public ConfigAdapter getConfigAdapter() {
        return service.getConfigAdapter();
    }

    public CrawlQueueAdapter getCrawlQueueAdapter() {
        return service.getCrawlQueueAdapter();
    }

    public ExecutionsAdapter getExecutionsAdapter() {
        return service.getExecutionsAdapter();
    }

    public DbInitializer getDbInitializer() {
        return service.getDbInitializer();
    }

    public DistributedLock createDistributedLock(Key key, int expireSeconds) {
        return service.createDistributedLock(key, expireSeconds);
    }

    public List<Key> listExpiredDistributedLocks(String domain) throws DbQueryException, DbConnectionException {
        return service.listExpiredDistributedLocks(domain);
    }

    public List<Key> listExpiredDistributedLocks() throws DbQueryException, DbConnectionException {
        return service.listExpiredDistributedLocks();
    }

    @Override
    public void close() {
        service.close();
        instance = null;
    }
}
