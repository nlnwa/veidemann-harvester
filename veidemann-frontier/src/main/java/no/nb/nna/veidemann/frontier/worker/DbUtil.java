package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;

/**
 * Static helper methods for handling DB objects.
 * <p/>
 */
public class DbUtil {
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
    public static void writeLog(QueuedUriWrapper qUri) throws DbException {
        writeLog(qUri, qUri.getError().getCode());
    }

    public static void writeLog(QueuedUriWrapper qUri, int statusCode) throws DbException {
        CrawlLog crawlLog = CrawlLog.newBuilder()
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
                .setCollectionFinalName(qUri.getCollectionName())
                .build();
        DbService.getInstance().getDbAdapter().saveCrawlLog(crawlLog);
    }
}
