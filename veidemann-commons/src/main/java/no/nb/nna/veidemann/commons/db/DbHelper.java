package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;

/**
 * Static methods for common Db operations.
 */
public class DbHelper {
    /**
     * Avoid instantiation
     */
    private DbHelper() {
    }

    /**
     * Get the CrawlConfig associated with a CrawlJob.
     *
     * @param job the job for which to get the CrawlConfig
     * @return the CrawlConfig associated with the CrawlJob
     */
    public static CrawlConfig getCrawlConfigForJob(CrawlJob job) throws DbException {
        return DbService.getInstance().getConfigAdapter()
                .getCrawlConfig(GetRequest.newBuilder().setId(job.getCrawlConfigId()).build());
    }

    /**
     * Get the PolitenessConfig associated with a CrawlConfig.
     *
     * @param config the CrawlConfig for which to get the PolitenessConfig
     * @return the PolitenessConfig associated with the CrawlConfig
     */
    public static PolitenessConfig getPolitenessConfigForCrawlConfig(CrawlConfig config)
            throws DbException {
        return DbService.getInstance().getConfigAdapter()
                .getPolitenessConfig(GetRequest.newBuilder().setId(config.getPolitenessId()).build());
    }

    /**
     * Get the BrowserConfig associated with a CrawlConfig.
     *
     * @param config the CrawlConfig for which to get the BrowserConfig
     * @return the BrowserConfig associated with the CrawlConfig
     */
    public static BrowserConfig getBrowserConfigForCrawlConfig(CrawlConfig config)
            throws DbException {
        return DbService.getInstance().getConfigAdapter()
                .getBrowserConfig(GetRequest.newBuilder().setId(config.getBrowserConfigId()).build());
    }
}
