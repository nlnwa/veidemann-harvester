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
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StatusWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(StatusWrapper.class);

    private CrawlExecutionStatus.Builder status;

    private StatusWrapper(CrawlExecutionStatus status) {
        this.status = status.toBuilder();
    }

    private StatusWrapper(CrawlExecutionStatus.Builder status) {
        this.status = status;
    }

    public static StatusWrapper getStatusWrapper(String executionId) {
        return new StatusWrapper(DbUtil.getInstance().getDb().getExecutionStatus(executionId));
    }

    public static StatusWrapper getStatusWrapper(CrawlExecutionStatus status) {
        return new StatusWrapper(status);
    }

    public static StatusWrapper getStatusWrapper(CrawlExecutionStatus.Builder status) {
        return new StatusWrapper(status);
    }

    public StatusWrapper saveStatus() {
        status = DbUtil.getInstance().getDb().saveExecutionStatus(status.build()).toBuilder();
        return this;
    }

    public String getId() {
        return status.getId();
    }

    public String getJobId() {
        return status.getJobId();
    }

    public Timestamp getStartTime() {
        return status.getStartTime();
    }

    public Timestamp getEndTime() {
        return status.getEndTime();
    }

    public boolean isEnded() {
        return status.hasEndTime();
    }

    public ConfigProto.CrawlScope getScope() {
        return status.getScope();
    }

    public CrawlExecutionStatus.State getState() {
        return status.getState();
    }

    public StatusWrapper setState(CrawlExecutionStatus.State state) {
        status.setState(state);
        return this;
    }

    public StatusWrapper setStartTimeIfUnset() {
        if (!status.hasStartTime()) {
            // Start execution
            status.setStartTime(ProtoUtils.getNowTs());
        }
        return this;
    }

    public StatusWrapper setEndState(CrawlExecutionStatus.State state) {
        LOG.info("Reached end of crawl '{}' with state: {}", getId(), state);
        status.setState(state).setEndTime(ProtoUtils.getNowTs());
        return this;
    }

    public long getDocumentsCrawled() {
        return status.getDocumentsCrawled();
    }

    public StatusWrapper incrementDocumentsCrawled() {
        status.setDocumentsCrawled(status.getDocumentsCrawled() + 1);
        return this;
    }

    public long getBytesCrawled() {
        return status.getBytesCrawled();
    }

    public StatusWrapper incrementBytesCrawled(long val) {
        status.setBytesCrawled(status.getBytesCrawled() + val);
        return this;
    }

    public long getUrisCrawled() {
        return status.getUrisCrawled();
    }

    public StatusWrapper incrementUrisCrawled(long val) {
        status.setUrisCrawled(status.getUrisCrawled() + val);
        return this;
    }

    public long getDocumentsFailed() {
        return status.getDocumentsFailed();
    }

    public StatusWrapper incrementDocumentsFailed() {
        status.setDocumentsFailed(status.getDocumentsFailed() + 1);
        return this;
    }

    public long getDocumentsOutOfScope() {
        return status.getDocumentsOutOfScope();
    }

    public StatusWrapper incrementDocumentsOutOfScope() {
        status.setDocumentsOutOfScope(status.getDocumentsOutOfScope() + 1);
        return this;
    }

    public long getDocumentsRetried() {
        return status.getDocumentsRetried();
    }

    public StatusWrapper incrementDocumentsRetried() {
        status.setDocumentsRetried(status.getDocumentsRetried() + 1);
        return this;
    }

    public long getDocumentsDenied() {
        return status.getDocumentsDenied();
    }

    public StatusWrapper incrementDocumentsDenied(long val) {
        status.setDocumentsDenied(status.getDocumentsDenied() + val);
        return this;
    }

    public CrawlExecutionStatus getCrawlExecutionStatus() {
        return status.build();
    }

    public StatusWrapper setCurrentUri(String uri) {
        status.setCurrentUri(uri);
        return this;
    }

    public StatusWrapper clearCurrentUri() {
        status.clearCurrentUri();
        return this;
    }
}
