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
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ControllerProto;
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUriOrBuilder;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.List;

/**
 *
 */
public class QueuedUriWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(QueuedUriWrapper.class);

    private QueuedUri.Builder wrapped;

    private Uri surt;

    private QueuedUriWrapper(QueuedUriOrBuilder uri) throws URISyntaxException, DbException {
        if (uri instanceof QueuedUri.Builder) {
            wrapped = (QueuedUri.Builder) uri;
        } else {
            wrapped = ((QueuedUri) uri).toBuilder();
        }
        if (wrapped.getSurt().isEmpty()) {
            createSurt();
        }
    }

    public static QueuedUriWrapper getQueuedUriWrapper(QueuedUri qUri) throws URISyntaxException, DbException {
        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(qUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(qUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(qUri.getPolitenessId(), "Empty PolitenessId");

        return new QueuedUriWrapper(qUri);
    }

    public static QueuedUriWrapper getQueuedUriWrapper(QueuedUriWrapper parentUri, QueuedUri qUri) throws URISyntaxException, DbException {
        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(parentUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(parentUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(parentUri.getPolitenessId(), "Empty PolitenessId");

        QueuedUriWrapper wrapper = new QueuedUriWrapper(qUri)
                .setJobExecutionId(parentUri.getJobExecutionId())
                .setExecutionId(parentUri.getExecutionId())
                .setPolitenessId(parentUri.getPolitenessId());
        wrapper.wrapped.setUnresolved(true);

        return wrapper;
    }

    public static QueuedUriWrapper getQueuedUriWrapper(String uri, String jobExecutionId, String executionId, String politenessId)
            throws URISyntaxException, DbException {
        return new QueuedUriWrapper(QueuedUri.newBuilder()
                .setUri(uri)
                .setJobExecutionId(jobExecutionId)
                .setExecutionId(executionId)
                .setPolitenessId(politenessId)
                .setUnresolved(true)
                .setSequence(1L)
        );
    }

    public QueuedUriWrapper addUriToQueue() throws DbException {
        requireNonEmpty(wrapped.getUri(), "Empty URI string");
        requireNonEmpty(wrapped.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(wrapped.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(wrapped.getPolitenessId(), "Empty PolitenessId");
        if (wrapped.getSequence() <= 0) {
            String msg = "Uri is missing Sequence. Uri: " + wrapped.getUri();
            IllegalStateException ex = new IllegalStateException(msg);
            LOG.error(msg, ex);
            throw ex;
        }

        if (wrapped.getUnresolved() && wrapped.getCrawlHostGroupId().isEmpty()) {
            wrapped.setCrawlHostGroupId(CrawlHostGroupCalculator.createSha1Digest(surt.getDecodedHost()));
        }
        requireNonEmpty(wrapped.getCrawlHostGroupId(), "Empty CrawlHostGroupId");

        QueuedUri q = wrapped.build();
        q = DbService.getInstance().getCrawlQueueAdapter().addToCrawlHostGroup(q);
        wrapped = q.toBuilder();

        return this;
    }

    private void createSurt() throws URISyntaxException, DbException {
        LOG.debug("Parse URI '{}'", wrapped.getUri());
        try {
            surt = UriConfigs.SURT_KEY.buildUri(wrapped.getUri());
            wrapped.setSurt(surt.toString());
        } catch (Throwable t) {
            LOG.info("Unparseable URI '{}'", wrapped.getUri());
            wrapped = wrapped.setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError());
            DbUtil.writeLog(this);
            throw new URISyntaxException(wrapped.getUri(), t.getMessage());
        }
    }

    public String getHost() {
        return UriConfigs.WHATWG.buildUri(wrapped.getUri()).getHost();
    }

    public int getPort() {
        if (surt == null) {
            surt = UriConfigs.SURT_KEY.buildUri(wrapped.getUri());
        }
        return surt.getDecodedPort();
    }

    public String getUri() {
        return wrapped.getUri();
    }

    public String getIp() {
        return wrapped.getIp();
    }

    QueuedUriWrapper setIp(String value) {
        wrapped.setIp(value);
        return this;
    }

    public String getExecutionId() {
        return wrapped.getExecutionId();
    }

    QueuedUriWrapper setExecutionId(String id) {
        wrapped.setExecutionId(id);
        return this;
    }

    public String getJobExecutionId() {
        return wrapped.getJobExecutionId();
    }

    QueuedUriWrapper setJobExecutionId(String id) {
        wrapped.setJobExecutionId(id);
        return this;
    }

    public int getRetries() {
        return wrapped.getRetries();
    }

    QueuedUriWrapper setRetries(int value) {
        wrapped.setRetries(value);
        return this;
    }

    String getDiscoveryPath() {
        return wrapped.getDiscoveryPath();
    }

    String getReferrer() {
        return wrapped.getReferrer();
    }

    String getSurt() {
        return wrapped.getSurt();
    }

    boolean hasError() {
        return wrapped.hasError();
    }

    MessagesProto.Error getError() {
        return wrapped.getError();
    }

    QueuedUriWrapper setError(MessagesProto.Error value) {
        wrapped.setError(value);
        return this;
    }

    QueuedUriWrapper clearError() {
        wrapped.clearError();
        return this;
    }

    String getCrawlHostGroupId() {
        return wrapped.getCrawlHostGroupId();
    }

    QueuedUriWrapper setCrawlHostGroupId(String value) {
        wrapped.setCrawlHostGroupId(value);
        return this;
    }

    QueuedUriWrapper setEarliestFetchDelaySeconds(int value) {
        Timestamp earliestFetch = Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(value));
        wrapped.setEarliestFetchTimeStamp(earliestFetch);
        return this;
    }

    String getPolitenessId() {
        return wrapped.getPolitenessId();
    }

    QueuedUriWrapper setPolitenessId(String value) {
        wrapped.setPolitenessId(value);
        return this;
    }

    public long getSequence() {
        // Internally use a sequence with base 1 to avoid the value beeing deleted from the protobuf object.
        return wrapped.getSequence() - 1L;
    }

    QueuedUriWrapper setSequence(long value) {
        if (value < 0L) {
            throw new IllegalArgumentException("Negative values not allowed");
        }

        // Internally use a sequence with base 1 to avoid the value beeing deleted from the protobuf object.
        wrapped.setSequence(value + 1L);
        return this;
    }

    QueuedUriWrapper incrementRetries() {
        wrapped.setRetries(wrapped.getRetries() + 1);
        return this;
    }

    public boolean isUnresolved() {
        return wrapped.getUnresolved();
    }

    QueuedUriWrapper setResolved(PolitenessConfig politeness) throws DbException {
        if (wrapped.getUnresolved() == false) {
            return this;
        }

        if (wrapped.getIp().isEmpty()) {
            String msg = "Can't set Uri to resolved when ip is missing. Uri: " + wrapped.getUri();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        // Calculate CrawlHostGroup
        List<CrawlHostGroupConfig> groupConfigs = DbService.getInstance().getConfigAdapter()
                .listCrawlHostGroupConfigs(ControllerProto.ListRequest.newBuilder()
                        .addAllLabelSelector(politeness.getCrawlHostGroupSelectorList()).build()).getValueList();
        String crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroup(wrapped.getIp(), groupConfigs);
        wrapped.setCrawlHostGroupId(crawlHostGroupId);

        wrapped.setUnresolved(false);
        return this;
    }

    public QueuedUri getQueuedUri() {
        return wrapped.build();
    }

    private static void requireNonEmpty(String obj, String message) {
        if (obj == null || obj.isEmpty()) {
            LOG.error(message);
            throw new NullPointerException(message);
        }
    }

    @Override
    public String toString() {
        return getUri().toString();
    }
}
