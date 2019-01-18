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
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUriOrBuilder;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class QueuedUriWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(QueuedUriWrapper.class);

    private QueuedUri.Builder wrapped;

    private Uri surt;

    private final String collectionName;

    boolean justAdded = false;

    private QueuedUriWrapper(QueuedUriOrBuilder uri, String collectionName) throws URISyntaxException, DbException {
        this.collectionName = collectionName;
        if (uri instanceof QueuedUri.Builder) {
            wrapped = (QueuedUri.Builder) uri;
        } else {
            wrapped = ((QueuedUri) uri).toBuilder();
        }
        if (wrapped.getSurt().isEmpty()) {
            createSurt(collectionName);
        }
    }

    public static QueuedUriWrapper getQueuedUriWrapper(QueuedUri qUri, String collectionName) throws URISyntaxException, DbException {
        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(qUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(qUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(qUri.getPolitenessRef(), "Empty PolitenessRef");

        return new QueuedUriWrapper(qUri, collectionName);
    }

    public static QueuedUriWrapper getQueuedUriWrapper(QueuedUriWrapper parentUri, QueuedUri qUri, String collectionName) throws URISyntaxException, DbException {
        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(parentUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(parentUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(parentUri.getPolitenessRef(), "Empty PolitenessRef");

        QueuedUriWrapper wrapper = new QueuedUriWrapper(qUri, collectionName)
                .setJobExecutionId(parentUri.getJobExecutionId())
                .setExecutionId(parentUri.getExecutionId())
                .setPolitenessRef(parentUri.getPolitenessRef());
        wrapper.wrapped.setUnresolved(true);

        return wrapper;
    }

    public static QueuedUriWrapper getQueuedUriWrapper(String uri, String jobExecutionId, String executionId, ConfigRef politenessId, String collectionName)
            throws URISyntaxException, DbException {
        return new QueuedUriWrapper(QueuedUri.newBuilder()
                .setUri(uri)
                .setJobExecutionId(jobExecutionId)
                .setExecutionId(executionId)
                .setPolitenessRef(politenessId)
                .setUnresolved(true)
                .setSequence(1L),
                collectionName
        );
    }

    public QueuedUriWrapper addUriToQueue() throws DbException {
        requireNonEmpty(wrapped.getUri(), "Empty URI string");
        requireNonEmpty(wrapped.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(wrapped.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(wrapped.getPolitenessRef(), "Empty PolitenessRef");
        if (wrapped.getSequence() <= 0) {
            String msg = "Uri is missing Sequence. Uri: " + wrapped.getUri();
            IllegalStateException ex = new IllegalStateException(msg);
            LOG.error(msg, ex);
            throw ex;
        }
        if (wrapped.getPriorityWeight() <= 0d) {
            String msg = "Priority weight must be greater than zero. Uri: " + wrapped.getUri();
            IllegalStateException ex = new IllegalStateException(msg);
            LOG.error(msg, ex);
            throw ex;
        }

        if (wrapped.getUnresolved() && wrapped.getCrawlHostGroupId().isEmpty()) {
            wrapped.setCrawlHostGroupId(ApiTools.createSha1Digest(surt.getDecodedHost()));
        }
        requireNonEmpty(wrapped.getCrawlHostGroupId(), "Empty CrawlHostGroupId");

        QueuedUri q = wrapped.build();
        q = DbService.getInstance().getCrawlQueueAdapter().addToCrawlHostGroup(q);
        wrapped = q.toBuilder();
        justAdded = true;

        return this;
    }

    private void createSurt(String collectionName) throws URISyntaxException, DbException {
        LOG.debug("Parse URI '{}'", wrapped.getUri());
        try {
            surt = UriConfigs.SURT_KEY.buildUri(wrapped.getUri());
            wrapped.setSurt(surt.toString());
        } catch (Exception t) {
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

    public Timestamp getFetchStartTimeStamp() {
        return wrapped.getFetchStartTimeStamp();
    }

    QueuedUriWrapper setFetchStartTimeStamp(Timestamp value) {
        wrapped.setFetchStartTimeStamp(value);
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

    Error getError() {
        return wrapped.getError();
    }

    QueuedUriWrapper setError(Error value) {
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

    ConfigRef getPolitenessRef() {
        return wrapped.getPolitenessRef();
    }

    QueuedUriWrapper setPolitenessRef(ConfigRef value) {
        wrapped.setPolitenessRef(value);
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

    public double getPriorityWeight() {
        return wrapped.getPriorityWeight();
    }

    QueuedUriWrapper setPriorityWeight(double value) {
        if (value <= 0d) {
            value = 1.0d;
            LOG.debug("Priority weight should be greater than zero. Using default of 1.0 for uri: {}", wrapped.getUri());
        }

        wrapped.setPriorityWeight(value);
        return this;
    }

    QueuedUriWrapper incrementRetries() {
        wrapped.setRetries(wrapped.getRetries() + 1);
        return this;
    }

    public boolean isUnresolved() {
        return wrapped.getUnresolved();
    }

    QueuedUriWrapper setResolved(ConfigObject politeness) throws DbException {
        if (wrapped.getUnresolved() == false) {
            return this;
        }

        if (wrapped.getIp().isEmpty()) {
            String msg = "Can't set Uri to resolved when ip is missing. Uri: " + wrapped.getUri();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        String crawlHostGroupId;
        if (politeness.getPolitenessConfig().getUseHostname()) {
            // Use host name for politeness
            crawlHostGroupId = ApiTools.createSha1Digest(getHost());
        } else {
            // Use IP for politeness
            // Calculate CrawlHostGroup
            try (ChangeFeed<ConfigObject> cursor = DbService.getInstance().getConfigAdapter().listConfigObjects(ListRequest.newBuilder()
                    .setKind(Kind.crawlHostGroupConfig)
                    .addAllLabelSelector(politeness.getPolitenessConfig().getCrawlHostGroupSelectorList()).build())) {

                List<ConfigObject> groupConfigs = cursor.stream().collect(Collectors.toList());
                crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroup(wrapped.getIp(), groupConfigs);
            }
        }

        wrapped.setCrawlHostGroupId(crawlHostGroupId);

        wrapped.setUnresolved(false);
        return this;
    }

    public QueuedUri getQueuedUri() {
        return wrapped.build();
    }

    public String getCollectionName() {
        return collectionName;
    }

    private static void requireNonEmpty(String obj, String message) {
        if (obj == null || obj.isEmpty()) {
            LOG.error(message);
            throw new NullPointerException(message);
        }
    }

    private static void requireNonEmpty(ConfigRef obj, String message) {
        if (obj == null || obj.getId().isEmpty() || obj.getKind() == Kind.undefined) {
            LOG.error(message);
            throw new NullPointerException(message);
        }
    }

    @Override
    public String toString() {
        return getUri().toString();
    }
}
