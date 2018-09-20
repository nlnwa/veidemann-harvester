/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import com.google.protobuf.Timestamp;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.MessagesProto.Error;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StatusWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(StatusWrapper.class);

    private CrawlExecutionStatus.Builder status;

    private CrawlExecutionStatusChange.Builder change;

    private boolean dirty;

    private StatusWrapper(CrawlExecutionStatus status) {
        this.status = status.toBuilder();
    }

    private StatusWrapper(CrawlExecutionStatus.Builder status) {
        this.status = status;
    }

    public static StatusWrapper getStatusWrapper(String executionId) throws DbException {
        return new StatusWrapper(DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(executionId));
    }

    public static StatusWrapper getStatusWrapper(CrawlExecutionStatus status) {
        return new StatusWrapper(status);
    }

    public static StatusWrapper getStatusWrapper(CrawlExecutionStatus.Builder status) {
        return new StatusWrapper(status);
    }

    public synchronized StatusWrapper saveStatus() throws DbException {
        if (change != null) {
            status = DbService.getInstance().getExecutionsAdapter().updateCrawlExecutionStatus(
                    change.setId(status.getId()).build())
                    .toBuilder();
            change = null;
            dirty = false;
        }
        return this;
    }

    public String getId() {
        return status.getId();
    }

    public String getJobId() {
        return status.getJobId();
    }

    public String getJobExecutionId() {
        return status.getJobExecutionId();
    }

    public Timestamp getStartTime() {
        return getCrawlExecutionStatus().getStartTime();
    }

    public Timestamp getCreatedTime() {
        return getCrawlExecutionStatus().getCreatedTime();
    }

    public Timestamp getEndTime() {
        return getCrawlExecutionStatus().getEndTime();
    }

    public boolean isEnded() {
        return getCrawlExecutionStatus().hasEndTime();
    }

    public ConfigProto.CrawlScope getScope() {
        return status.getScope();
    }

    public CrawlExecutionStatus.State getState() {
        return getCrawlExecutionStatus().getState();
    }

    public StatusWrapper setState(CrawlExecutionStatus.State state) {
        dirty = true;
        getChange().setState(state);
        return this;
    }

    public StatusWrapper setEndState(CrawlExecutionStatus.State state) {
        LOG.info("Reached end of crawl '{}' with state: {}", getId(), state);
        dirty = true;
        getChange().setState(state).setEndTime(ProtoUtils.getNowTs());
        return this;
    }

    public long getDocumentsCrawled() {
        return getCrawlExecutionStatus().getDocumentsCrawled();
    }

    public StatusWrapper incrementDocumentsCrawled() {
        getChange().setAddDocumentsCrawled(1);
        return this;
    }

    public long getBytesCrawled() {
        return getCrawlExecutionStatus().getBytesCrawled();
    }

    public StatusWrapper incrementBytesCrawled(long val) {
        getChange().setAddBytesCrawled(val);
        return this;
    }

    public long getUrisCrawled() {
        return getCrawlExecutionStatus().getUrisCrawled();
    }

    public StatusWrapper incrementUrisCrawled(long val) {
        getChange().setAddUrisCrawled(val);
        return this;
    }

    public long getDocumentsFailed() {
        return getCrawlExecutionStatus().getDocumentsFailed();
    }

    public StatusWrapper incrementDocumentsFailed() {
        getChange().setAddDocumentsFailed(1);
        return this;
    }

    public long getDocumentsOutOfScope() {
        return getCrawlExecutionStatus().getDocumentsOutOfScope();
    }

    public StatusWrapper incrementDocumentsOutOfScope() {
        getChange().setAddDocumentsOutOfScope(1);
        return this;
    }

    public long getDocumentsRetried() {
        return getCrawlExecutionStatus().getDocumentsRetried();
    }

    public StatusWrapper incrementDocumentsRetried() {
        getChange().setAddDocumentsRetried(1);
        return this;
    }

    public long getDocumentsDenied() {
        return getCrawlExecutionStatus().getDocumentsDenied();
    }

    public StatusWrapper incrementDocumentsDenied(long val) {
        getChange().setAddDocumentsDenied(val);
        return this;
    }

    public CrawlExecutionStatus getCrawlExecutionStatus() {
        if (dirty) {
            throw new IllegalStateException("CES is dirty " + change);
        }
        return status.build();
    }

    public StatusWrapper addCurrentUri(QueuedUriWrapper uri) {
        getChange().setAddCurrentUri(uri.getQueuedUri());
        return this;
    }

    public StatusWrapper removeCurrentUri(QueuedUriWrapper uri) {
        if (!uri.justAdded) {
            getChange().setDeleteCurrentUri(uri.getQueuedUri());
        }
        return this;
    }

    public StatusWrapper setError(Error error) {
        getChange().setError(error);
        return this;
    }

    public StatusWrapper setError(int code, String message) {
        getChange().setError(Error.newBuilder().setCode(code).setMsg(message).build());
        return this;
    }

    private CrawlExecutionStatusChange.Builder getChange() {
        if (change == null) {
            change = CrawlExecutionStatusChange.newBuilder();
        }
        return change;
    }
}
