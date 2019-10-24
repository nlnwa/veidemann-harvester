/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.harvester;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.browsercontroller.v1.BrowserControllerGrpc;
import no.nb.nna.veidemann.api.browsercontroller.v1.DoReply;
import no.nb.nna.veidemann.api.browsercontroller.v1.DoRequest;
import no.nb.nna.veidemann.api.browsercontroller.v1.NotifyActivity.Activity;
import no.nb.nna.veidemann.api.browsercontroller.v1.RegisterNew;
import no.nb.nna.veidemann.api.config.v1.BrowserScript;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.PolitenessConfig.RobotsPolicy;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog.Builder;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLogOrBuilder;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.client.RobotsServiceClient;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.commons.util.CollectionNameGenerator;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BrowserControllerService extends BrowserControllerGrpc.BrowserControllerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(BrowserControllerService.class);

    private final BrowserSessionRegistry sessionRegistry;

    private final RobotsServiceClient robotsServiceClient;

    public BrowserControllerService(BrowserSessionRegistry sessionRegistry, RobotsServiceClient robotsServiceClient) {
        this.sessionRegistry = sessionRegistry;
        this.robotsServiceClient = robotsServiceClient;
    }

    @Override
    public StreamObserver<DoRequest> do_(StreamObserver<DoReply> responseObserver) {
        return new StreamObserver<DoRequest>() {
            ProxyRequest proxyRequest;

            @Override
            public void onNext(DoRequest value) {
                switch (value.getActionCase()) {
                    case NEW:
                        DoReply.Builder b = DoReply.newBuilder();

                        // Robots.txt request comes directly to proxy from Robots evaluator and should be handled separately
                        proxyRequest = createRequestIfRobotsTxt(value.getNew());
                        if (proxyRequest != null) {
                            b.getNewBuilder()
                                    .setCrawlExecutionId(value.getNew().getCrawlExecutionId())
                                    .setJobExecutionId(value.getNew().getJobExecutionId())
                                    .setCollectionRef(value.getNew().getCollectionRef());
                            responseObserver.onNext(b.build());
                            break;
                        }

                        BrowserSession session = sessionRegistry.get(value.getNew().getProxyId());

                        proxyRequest = session.getCrawlLogs().registerProxyRequest(value.getNew().getUri());
                        proxyRequest.setResponseObserver(responseObserver);
                        proxyRequest.setCollectionRef(session.getCollectionRef());

                        if (checkPrecludedByRobotsTxt(proxyRequest, session)) {
                            break;
                        }

                        b.getNewBuilder()
                                .setCrawlExecutionId(session.getCrawlExecutionId())
                                .setJobExecutionId(session.getJobExecutionId())
                                .setCollectionRef(session.getCollectionRef());
                        BrowserScript replacementScript = getReplacementScript(session, value.getNew().getUri());
                        if (replacementScript != null) {
                            b.getNewBuilder().setReplacementScript(replacementScript);
                        }

                        responseObserver.onNext(b.build());

                        break;
                    case NOTIFY:
                        Activity activity = value.getNotify().getActivity();
                        proxyRequest.notifyActivity(activity);
                        break;
                    case COMPLETED:
                        proxyRequest.setCrawlLog(value.getCompleted().getCrawlLog(), value.getCompleted().getCached());
                        break;
                    case ACTION_NOT_SET:
                        String msg = "Browser controller api was called without action";
                        LOG.error(msg);
                        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(msg).asException());
                }
            }

            @Override
            public void onError(Throwable t) {
                String uri = "";
                if (proxyRequest != null) {
                    uri = proxyRequest.getUri();
                }
                LOG.error("Browser controller api error: {} (uri: {})", t.toString(), uri, t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private BrowserScript getReplacementScript(BrowserSession session, String uri) {
        String normalizedUri = UriConfigs.WHATWG.buildUri(uri).toString();
        Label replacementLabel = ApiTools.buildLabel("type", "replacement");
        for (ConfigObject script : session.getScripts()) {
            if (ApiTools.hasLabel(script.getMeta(), replacementLabel)) {
                for (String urlRegexp : script.getBrowserScript().getUrlRegexpList()) {
                    if (normalizedUri.matches(urlRegexp)) {
                        LOG.warn("Check script {} {} {} {}", script.getMeta().getName(), normalizedUri, urlRegexp, normalizedUri.matches(urlRegexp));
                        return script.getBrowserScript();
                    }
                }
            }
        }
        return null;
    }

    private RobotsTxtRequest createRequestIfRobotsTxt(RegisterNew request) {
        if (request.getUri().toLowerCase().endsWith("robots.txt")
                && request.getCrawlExecutionId() != ""
                && request.getJobExecutionId() != ""
                && request.hasCollectionRef()) {
            RobotsTxtRequest r = new RobotsTxtRequest(request.getUri());
            r.setCollectionRef(request.getCollectionRef());
            return r;
        }
        return null;
    }

    private boolean checkPrecludedByRobotsTxt(ProxyRequest proxyRequest, BrowserSession session) {
        QueuedUri quri = QueuedUri.newBuilder().setUri(proxyRequest.getUri()).build();
        ConfigObject politeness = session.getPolitenessConfig();
        RobotsPolicy resolvedPolicy;
        switch (politeness.getPolitenessConfig().getRobotsPolicy()) {
            case OBEY_ROBOTS_CLASSIC:
                resolvedPolicy = RobotsPolicy.OBEY_ROBOTS;
                break;
            case CUSTOM_ROBOTS_CLASSIC:
                resolvedPolicy = RobotsPolicy.CUSTOM_ROBOTS;
                break;
            case CUSTOM_IF_MISSING_CLASSIC:
                resolvedPolicy = RobotsPolicy.CUSTOM_IF_MISSING;
                break;
            default:
                resolvedPolicy = RobotsPolicy.IGNORE_ROBOTS;
                break;
        }
        if (resolvedPolicy != RobotsPolicy.IGNORE_ROBOTS) {
            ConfigObject.Builder pb = politeness.toBuilder();
            pb.getPolitenessConfigBuilder().setRobotsPolicy(resolvedPolicy);
            politeness = pb.build();
            if (!robotsServiceClient.isAllowed(quri, session.getBrowserConfig().getBrowserConfig().getUserAgent(),
                    politeness, session.getCollectionRef())) {
                proxyRequest.cancelRequest("Blocked by robots.txt");
                return true;
            }
        }
        return false;
    }

    public interface ProxyRequest {
        String getUri();

        CrawlLog.Builder getCrawlLog();

        void setCrawlLog(CrawlLogOrBuilder crawlLog, boolean isFromCache);

        void notifyActivity(Activity activity);

        ConfigRef getCollectionRef();

        void setCollectionRef(ConfigRef collectionRef);

        boolean isFromCache();

        void setResponseObserver(StreamObserver<DoReply> responseObserver);

        void cancelRequest(String reason);
    }

    public class RobotsTxtRequest implements ProxyRequest {
        final String uri;
        CrawlLog.Builder crawlLog;
        ConfigRef collectionRef;
        boolean fromCache;
        StreamObserver<DoReply> responseObserver;

        public RobotsTxtRequest(String uri) {
            this.uri = uri;
        }

        @Override
        public String getUri() {
            return uri;
        }

        @Override
        public Builder getCrawlLog() {
            return crawlLog;
        }

        @Override
        public void setCrawlLog(CrawlLogOrBuilder crawlLog, boolean isFromCache) {
            this.fromCache = isFromCache;
            if (crawlLog instanceof CrawlLog) {
                this.crawlLog = ((CrawlLog) crawlLog).toBuilder();
            } else {
                this.crawlLog = (CrawlLog.Builder) crawlLog;
            }
            if (this.crawlLog.getCollectionFinalName().isEmpty()) {
                this.crawlLog.setCollectionFinalName(getCollectionFinalName(getCollectionRef(), SubCollectionType.UNDEFINED));
            }
            this.crawlLog.setDiscoveryPath("P");

            if (!this.crawlLog.getBlockDigest().isEmpty()) {
                try {
                    DbService.getInstance().getDbAdapter().saveCrawlLog(this.crawlLog.build());
                } catch (DbException e) {
                    throw new RuntimeException("Could not save crawl log", e);
                }
            }

        }

        @Override
        public void notifyActivity(Activity activity) {

        }

        @Override
        public ConfigRef getCollectionRef() {
            return collectionRef;
        }

        @Override
        public void setCollectionRef(ConfigRef collectionRef) {
            this.collectionRef = collectionRef;
        }

        @Override
        public boolean isFromCache() {
            return fromCache;
        }

        @Override
        public void setResponseObserver(StreamObserver<DoReply> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void cancelRequest(String reason) {
            try {
                responseObserver.onNext(DoReply.newBuilder().setCancel(reason).build());
            } catch (IllegalStateException e) {
                LOG.debug("Canceling closed call");
            }
        }
    }

    public static String getCollectionFinalName(ConfigRef collectionRef, SubCollectionType subType) {
        try {
            ConfigObject collection = DbService.getInstance().getConfigAdapter().getConfigObject(collectionRef);
            return CollectionNameGenerator.getCollectionName(collection, subType);
        } catch (DbException e) {
            LOG.warn("Could not get collection from DB", e);
        }
        return "collection missing";
    }
}
