package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.commons.settings.CommonSettings;

import java.util.Iterator;
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

    public CrawlQueueAdapter getCrawlQueueAdapter() {
        return service.getCrawlQueueAdapter();
    }

    public DbInitializer getDbInitializer() {
        return service.getDbInitializer();
    }

    @Override
    public void close() {
        service.close();
        instance = null;
    }
}
