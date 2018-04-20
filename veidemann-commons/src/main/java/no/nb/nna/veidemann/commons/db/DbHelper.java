package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;

/**
 * Singleton with helper methods for handling DB objects.
 * <p/>
 * Configure must be called before any usage of the utility methods.
 */
public class DbHelper {
    private static DbHelper ourInstance = new DbHelper();

    private DbAdapter dbAdapter;

    /**
     * Get the singleton instance.
     *
     * @return the single DbUtil instance
     */
    public static DbHelper getInstance() {
        return ourInstance;
    }

    /**
     * Configure the singleton DbUtil.
     * <p/>
     * This method must be called before any usage of the utility methods.
     *
     * @param dbAdapter a configured DbAdapter
     */
    public void configure(DbAdapter dbAdapter) {
        this.dbAdapter = dbAdapter;
    }

    /**
     * Protected constructor should not be used except by subclasses.
     */
    protected DbHelper() {
    }

    /**
     * Get the DbAdapter.
     *
     * @return DbAdapter
     */
    public DbAdapter getDb() {
        return dbAdapter;
    }

    /**
     * Get the CrawlConfig associated with a CrawlJob.
     *
     * @param job the job for which to get the CrawlConfig
     * @return the CrawlConfig associated with the CrawlJob
     */
    public CrawlConfig getCrawlConfigForJob(CrawlJob job) throws DbException {
        return dbAdapter.getCrawlConfig(GetRequest.newBuilder().setId(job.getCrawlConfigId()).build());
    }

    /**
     * Get the PolitenessConfig associated with a CrawlConfig.
     *
     * @param config the CrawlConfig for which to get the PolitenessConfig
     * @return the PolitenessConfig associated with the CrawlConfig
     */
    public PolitenessConfig getPolitenessConfigForCrawlConfig(CrawlConfig config) throws DbException {
        return dbAdapter.getPolitenessConfig(GetRequest.newBuilder().setId(config.getPolitenessId()).build());
    }

    /**
     * Get the BrowserConfig associated with a CrawlConfig.
     *
     * @param config the CrawlConfig for which to get the BrowserConfig
     * @return the BrowserConfig associated with the CrawlConfig
     */
    public BrowserConfig getBrowserConfigForCrawlConfig(CrawlConfig config) throws DbException {
        return dbAdapter.getBrowserConfig(GetRequest.newBuilder().setId(config.getBrowserConfigId()).build());
    }
}
