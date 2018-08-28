package no.nb.nna.veidemann.commons.db;

import com.google.protobuf.Timestamp;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;

public interface CrawlQueueAdapter {
    long deleteQueuedUrisForExecution(String executionId) throws DbException;

    long queuedUriCount(String executionId) throws DbException;

    boolean uriNotIncludedInQueue(QueuedUri qu, Timestamp since) throws DbException;

    /**
     * Get the first URI wich is ready to be fetched for a CrawlHostGroup.
     *
     * @param crawlHostGroup the CrawlHostGroup for which a URI is requested
     * @return an Optional containing the next URI to be fetched or empty if none are ready yet.
     */
    FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup) throws DbException;

    QueuedUri addToCrawlHostGroup(QueuedUri qUri) throws DbException;

    FutureOptional<CrawlHostGroup> borrowFirstReadyCrawlHostGroup() throws DbException;

    void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) throws DbException;

}
