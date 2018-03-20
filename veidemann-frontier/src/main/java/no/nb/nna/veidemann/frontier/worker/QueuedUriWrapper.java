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
import no.nb.nna.veidemann.api.MessagesProto;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUriOrBuilder;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

/**
 *
 */
public class QueuedUriWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(QueuedUriWrapper.class);

    private QueuedUri.Builder qUri;

    private Uri surt;

    private QueuedUriWrapper(Frontier frontier, QueuedUriOrBuilder uri) throws URISyntaxException {
        if (uri instanceof QueuedUri.Builder) {
            qUri = (QueuedUri.Builder) uri;
        } else {
            qUri = ((QueuedUri) uri).toBuilder();
        }
        if (qUri.getSurt().isEmpty()) {
            createSurt();
        }
    }

    public static QueuedUriWrapper getQueuedUriWrapper(Frontier frontier, QueuedUri qUri) throws URISyntaxException {
        return new QueuedUriWrapper(frontier, qUri);
    }

    public static QueuedUriWrapper getQueuedUriWrapper(Frontier frontier, String uri) throws URISyntaxException {
        return new QueuedUriWrapper(frontier, QueuedUri.newBuilder().setUri(uri));
    }

    public QueuedUriWrapper addUriToQueue() {
        qUri = DbUtil.getInstance().getDb().saveQueuedUri(qUri.build()).toBuilder();
        return this;
    }

    private void createSurt() throws URISyntaxException {
        LOG.debug("Parse URI '{}'", qUri.getUri());
        try {
            surt = UriConfigs.SURT_KEY.buildUri(qUri.getUri());
            qUri.setSurt(surt.toString());
        } catch (Throwable t) {
            LOG.info("Unparseable URI '{}'", qUri.getUri());
            qUri = qUri.setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError());
            DbUtil.getInstance().writeLog(this);
            throw new URISyntaxException(qUri.getUri(), t.getMessage());
        }
    }

    public String getHost() {
        return UriConfigs.WHATWG.buildUri(qUri.getUri()).getHost();
    }

    public int getPort() {
        if (surt == null) {
            surt = UriConfigs.SURT_KEY.buildUri(qUri.getUri());
        }
        return surt.getDecodedPort();
    }

    public String getUri() {
        return qUri.getUri();
    }

    public String getIp() {
        return qUri.getIp();
    }

    QueuedUriWrapper setIp(String value) {
        qUri.setIp(value);
        return this;
    }

    public String getExecutionId() {
        return qUri.getExecutionId();
    }

    QueuedUriWrapper setExecutionId(String id) {
        qUri.setExecutionId(id);
        return this;
    }

    public int getRetries() {
        return qUri.getRetries();
    }

    QueuedUriWrapper setRetries(int value) {
        qUri.setRetries(value);
        return this;
    }

    String getDiscoveryPath() {
        return qUri.getDiscoveryPath();
    }

    String getReferrer() {
        return qUri.getReferrer();
    }

    String getSurt() {
        return qUri.getSurt();
    }

    boolean hasError() {
        return qUri.hasError();
    }

    MessagesProto.Error getError() {
        return qUri.getError();
    }

    QueuedUriWrapper setError(MessagesProto.Error value) {
        qUri.setError(value);
        return this;
    }

    QueuedUriWrapper clearError() {
        qUri.clearError();
        return this;
    }

    QueuedUriWrapper setCrawlHostGroupId(String value) {
        qUri.setCrawlHostGroupId(value);
        return this;
    }

    QueuedUriWrapper setEarliestFetchDelaySeconds(int value) {
        Timestamp earliestFetch = Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(value));
        qUri.setEarliestFetchTimeStamp(earliestFetch);
        return this;
    }

    QueuedUriWrapper setPolitenessId(String value) {
        qUri.setPolitenessId(value);
        return this;
    }

    QueuedUriWrapper setSequence(long value) {
        qUri.setSequence(value);
        return this;
    }

    QueuedUriWrapper incrementRetries() {
        qUri.setRetries(qUri.getRetries() + 1);
        return this;
    }

    public QueuedUri getQueuedUri() {
        return qUri.build();
    }

}
