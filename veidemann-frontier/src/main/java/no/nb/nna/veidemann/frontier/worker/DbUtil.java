package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.commons.db.DbHelper;
import no.nb.nna.veidemann.db.ProtoUtils;

/**
 * Singleton with helper methods for handling DB objects.
 * <p/>
 * Configure must be called before any usage of the utility methods.
 */
public class DbUtil extends DbHelper {
    private static DbUtil ourInstance = new DbUtil();

    /**
     * Get the singleton instance.
     *
     * @return the single DbUtil instance
     */
    public static DbUtil getInstance() {
        return ourInstance;
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private DbUtil() {
    }

    /**
     * Write crawl log entry for uris failing preconditions.
     * <p>
     * Normally the crawl log is written by the harvester, but when preconditions fail a fetch will never be tried and
     * the crawl log must be written by the frontier.
     *
     * @param qUri the uri with failed precondition
     */
    public void writeLog(QueuedUriWrapper qUri) {
        writeLog(qUri, qUri.getError().getCode());
    }

    public void writeLog(QueuedUriWrapper qUri, int statusCode) {
        MessagesProto.CrawlLog crawlLog = MessagesProto.CrawlLog.newBuilder()
                .setRequestedUri(qUri.getUri())
                .setJobExecutionId(qUri.getJobExecutionId())
                .setExecutionId(qUri.getExecutionId())
                .setDiscoveryPath(qUri.getDiscoveryPath())
                .setReferrer(qUri.getReferrer())
                .setSurt(qUri.getSurt())
                .setRecordType("response")
                .setStatusCode(statusCode)
                .setError(qUri.getError())
                .setRetries(qUri.getRetries())
                .setFetchTimeStamp(ProtoUtils.getNowTs())
                .build();
        getDb().saveCrawlLog(crawlLog);
    }
}
