/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.harvester.browsercontroller;

import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.browsercontroller.v1.DoReply;
import no.nb.nna.veidemann.api.browsercontroller.v1.NotifyActivity.Activity;
import no.nb.nna.veidemann.api.config.v1.Collection.SubCollectionType;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLogOrBuilder;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.harvester.BrowserControllerService;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.netpreserve.commons.uri.UriException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CrawlLogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlLogRegistry.class);

    private final BrowserSession browserSession;

    private final List<Entry> crawlLogs = new ArrayList<>();
    private final Lock crawlLogsLock = new ReentrantLock();
    private final Condition crawlLogsUpdate = crawlLogsLock.newCondition();

    private final long pageLoadTimeout;
    private final long maxIdleTime;
    private final Matcher matcherThread;
    private final CountDownLatch finishLatch = new CountDownLatch(1);
    private final MatchStatus status = new MatchStatus();
    private final long startTime = System.currentTimeMillis();
    private long lastActivityTime = System.currentTimeMillis();

    public class Entry implements BrowserControllerService.ProxyRequest {
        final String uri;
        final BrowserSession session;
        private CrawlLog.Builder crawlLog;
        private boolean resolved = false;
        private ConfigRef collectionRef;
        boolean fromCache;
        StreamObserver<DoReply> responseObserver;

        public Entry(String uri, BrowserSession session) {
            this.uri = uri;
            this.session = session;
        }

        @Override
        public String getUri() {
            return uri;
        }

        @Override
        public void setCrawlLog(CrawlLogOrBuilder crawlLog, boolean isFromCache) {
            this.fromCache = isFromCache;
            crawlLogsLock.lock();
            try {
                if (crawlLog instanceof CrawlLog) {
                    this.crawlLog = ((CrawlLog) crawlLog).toBuilder();
                } else {
                    this.crawlLog = (CrawlLog.Builder) crawlLog;
                }

                if (this.crawlLog.getSurt() == "") {
                    try {
                        Uri surtUri = UriConfigs.SURT_KEY.buildUri(uri);
                        this.crawlLog.setSurt(surtUri.toString());
                    } catch (UriException ex) {
                        this.crawlLog.setError(ExtraStatusCodes.ILLEGAL_URI.toFetchError(ex.getMessage()));
                    }
                }

                if (this.crawlLog.hasError() && this.crawlLog.getCollectionFinalName().isEmpty()) {
                    this.crawlLog.setCollectionFinalName(BrowserControllerService.getCollectionFinalName(getCollectionRef(), SubCollectionType.UNDEFINED));
                }

                if (this.crawlLog.getExecutionId().isEmpty()) {
                    this.crawlLog.setExecutionId(session.getCrawlExecutionId());
                }

                if (this.crawlLog.getJobExecutionId().isEmpty()) {
                    this.crawlLog.setJobExecutionId(session.getJobExecutionId());
                }

                lastActivityTime = System.currentTimeMillis();
                crawlLogsUpdate.signalAll();
            } finally {
                crawlLogsLock.unlock();
            }
        }

        @Override
        public CrawlLog.Builder getCrawlLog() {
            if (crawlLog == null) {
                NullPointerException e = new NullPointerException("Crawl log is null");
                LOG.warn("Trying to access missing crawl log", e);
                throw e;
            }
            return crawlLog;
        }

        @Override
        public void notifyActivity(Activity activity) {
        }

        boolean isResponseReceived() {
            return crawlLog != null;
        }

        boolean isResolved() {
            return resolved;
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

    public CrawlLogRegistry(final BrowserSession session, final long pageLoadTimeout, final long maxIdleTime) {
        this.browserSession = session;
        this.pageLoadTimeout = pageLoadTimeout;
        this.maxIdleTime = maxIdleTime;
        matcherThread = new Matcher();
        matcherThread.start();
    }

    public Entry registerProxyRequest(String uri) {
        crawlLogsLock.lock();
        try {
            Entry crawlLogEntry = new Entry(uri, browserSession);
            crawlLogs.add(crawlLogEntry);
            lastActivityTime = System.currentTimeMillis();
            crawlLogsUpdate.signalAll();
            return crawlLogEntry;
        } finally {
            crawlLogsLock.unlock();
        }
    }

    public void signalRequestsUpdated() {
        crawlLogsLock.lock();
        try {
            lastActivityTime = System.currentTimeMillis();
            crawlLogsUpdate.signalAll();
        } finally {
            crawlLogsLock.unlock();
        }
    }

    public void signalActivity() {
        crawlLogsLock.lock();
        try {
            lastActivityTime = System.currentTimeMillis();
        } finally {
            crawlLogsLock.unlock();
        }
    }

    public long getFetchTimeMs() {
        return lastActivityTime - startTime;
    }

    private boolean isCrawlLogsResolved() {
        crawlLogsLock.lock();
        try {
            return crawlLogs.stream().allMatch(e -> e.resolved);
        } finally {
            crawlLogsLock.unlock();
        }
    }

    private class Matcher extends Thread {
        @Override
        public void run() {
            MDC.put("eid", browserSession.queuedUri.getExecutionId());
            MDC.put("uri", browserSession.queuedUri.getUri());
            LOG.debug("Page load timeout: {}", pageLoadTimeout);
            LOG.debug("Max idle time: {}", maxIdleTime);

            while (finishLatch.getCount() > 0) {
                waitForIdle();
                crawlLogsLock.lock();
                try {
                    innerMatchCrawlLogAndRequest(status);

                    if (!status.allHandled()) {
                        checkForFileDownload();
                    }

                    if (!status.unhandledRequests.isEmpty()) {
                        checkForCachedRequests();
                    }

                    if (status.allHandled()) {
                        finishLatch.countDown();
                    }
                } catch (Exception e) {
                    LOG.error(e.toString(), e);
                } finally {
                    crawlLogsLock.unlock();
                }
            }

            if (status.allHandled()) {
                LOG.debug("Finished matching crawl logs and uri requests");
            } else {
                LOG.warn("Not resolved. Status: {}", status);
            }
        }

        private void waitForIdle() {
            crawlLogsLock.lock();
            try {
                boolean running = true;
                while (running) {
                    boolean gotSignal = crawlLogsUpdate.await(maxIdleTime, TimeUnit.MILLISECONDS);
                    if (gotSignal) {
                        LOG.trace("Got activity signal");
                    }
                    if (finishLatch.getCount() == 0 || (System.currentTimeMillis() - lastActivityTime) >= maxIdleTime) {
                        LOG.debug("Timed out waiting for network activity");
                        running = false;

                        long timeout = pageLoadTimeout - (System.currentTimeMillis() - startTime);
                        if (timeout > pageLoadTimeout) {
                            LOG.error("High pageload timeout calculation: {} = pl {} - (ts {} - start {})", timeout, pageLoadTimeout,
                                    System.currentTimeMillis(), startTime);
                        }
                        if (timeout < 0) {
                            LOG.info("Pageload timed out");
                            finishLatch.countDown();
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.error(e.toString(), e);
            } finally {
                crawlLogsLock.unlock();
            }
        }
    }

    private void checkForCachedRequests() {
        for (int i = 0; i < status.unhandledRequests.size(); i++) {
            UriRequest r = status.unhandledRequests.get(i);
            boolean isWaitingForResponse = crawlLogs.stream()
                    .filter(e -> e.uri.equals(r.getUrl()))
                    .anyMatch(e -> !e.isResponseReceived());

            if (!isWaitingForResponse) {
                crawlLogs.stream()
                        .filter(e -> (e.uri.equals(r.getUrl()) && e.getCrawlLog().getStatusCode() == r.getStatusCode()))
                        .findFirst().ifPresent(e -> {
                    LOG.info("Found already resolved CrawlLog for {}. Setting fromCache for request {} to true",
                            r.getUrl(), r.getRequestId());
                    r.setFromProxy(true);
                    r.setFromCache(true);
                });
            }
        }

        innerMatchCrawlLogAndRequest(status);
    }

    /**
     * TODO: When there are crawllogs, but no request events have been received, then this is probably
     * a file download. At the moment we are creating UriRequests to simulate events. This should be
     * refactored into real events as soon as Chrome supports it.
     * https://bugs.chromium.org/p/chromium/issues/detail?id=696481
     */
    private void checkForFileDownload() {
        if (browserSession.getUriRequests().getInitialRequest() == null) {
            LOG.debug("Guessing that we are downloading a file. Status: {}", status);
            crawlLogs.forEach(c -> {
                browserSession.getUriRequests().resolveCurrentUriRequest("1").ifPresent(parent -> {
                    UriRequest r = UriRequest.create("1",
                            c.getCrawlLog().getRequestedUri(), browserSession.queuedUri.getReferrer(), ResourceType.Other,
                            'R', parent, browserSession.getUriRequests().getPageSpan());
                    r.setStatusCode(c.getCrawlLog().getStatusCode());
                    browserSession.getUriRequests().add(r);
                }).otherwise(() -> {
                    // No parent, this is a root request;
                    if (c.isResponseReceived()) {
                        UriRequest r = UriRequest.createRoot("1",
                                c.uri, browserSession.queuedUri.getReferrer(), ResourceType.Other,
                                browserSession.queuedUri.getDiscoveryPath(), browserSession.getUriRequests().getPageSpan());
                        r.setStatusCode(c.getCrawlLog().getStatusCode());
                        browserSession.getUriRequests().add(r);
                    }
                });
            });

            innerMatchCrawlLogAndRequest(status);
        }
    }

    public boolean waitForMatcherToFinish() {
        try {
            long timeout = pageLoadTimeout - (System.currentTimeMillis() - startTime);
            boolean success = finishLatch.await(timeout, TimeUnit.MILLISECONDS);
            if (!success) {
                LOG.info("Pageload timed out");
                finishLatch.countDown();
                // Send signal to stop waitForIdle loop
                signalRequestsUpdated();
            }
            innerMatchCrawlLogAndRequest(status);
            return success;
        } catch (InterruptedException e) {
            LOG.info("Pageload interrupted", e);
            finishLatch.countDown();
            // Send signal to stop waitForIdle loop
            signalRequestsUpdated();
            innerMatchCrawlLogAndRequest(status);
            return false;
        }
    }

    private boolean findRequestForCrawlLog(Entry crawlLogEntry, UriRequest r) {
        if (r == null) {
            return false;
        }

        if (innerFindRequestForCrawlLog(crawlLogEntry, r)) {
            return true;
        }
        for (UriRequest c : r.getChildren()) {
            if (findRequestForCrawlLog(crawlLogEntry, c)) {
                return true;
            }
        }
        return false;
    }

    private boolean uriEquals(String u1, String u2) {
        Uri uri1 = UriConfigs.WHATWG.buildUri(u1);
        Uri uri2 = UriConfigs.WHATWG.buildUri(u2);
        return uri1.equals(uri2);
    }

    private boolean innerFindRequestForCrawlLog(Entry crawlLogEntry, UriRequest r) {
        boolean requestFound = false;
        Timestamp now = ProtoUtils.getNowTs();
        if (r.getCrawlLog() == null
                && !r.isFromCache()
                && uriEquals(r.getUrl(), crawlLogEntry.uri)) {

            if (crawlLogEntry.isResponseReceived()) {
                if (crawlLogEntry.getCrawlLog().getStatusCode() == r.getStatusCode()) {
                    requestFound = true;
                } else if (r.getStatusCode() == ExtraStatusCodes.CANCELED_BY_BROWSER.getCode()) {
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else if (crawlLogEntry.getCrawlLog().getStatusCode() == ExtraStatusCodes.CANCELED_BY_BROWSER.getCode()) {
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else if (r.getStatusCode() == 504 && crawlLogEntry.getCrawlLog().getStatusCode() == ExtraStatusCodes.HTTP_TIMEOUT.getCode()) {
                    // If http times out, the proxy will return 504, but proxy sets crawllogstatus to -4 which is the underlying status.
                    // Update request to match crawllog
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else if (r.getStatusCode() == 0 && crawlLogEntry.getCrawlLog().getStatusCode() == ExtraStatusCodes.CONNECT_FAILED.getCode()) {
                    // If https connect fails, the proxy will return 0, but proxy sets crawllogstatus to -2 which is the underlying status.
                    // Update request to match crawllog
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else if (r.getStatusCode() == 403 && crawlLogEntry.getCrawlLog().getStatusCode() == ExtraStatusCodes.PRECLUDED_BY_ROBOTS.getCode()) {
                    // If request is precluded by robots.txt, the proxy will return 403, but proxy sets crawllogstatus to -9998 which is the underlying status.
                    // Update request to match crawllog
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else {
                    LOG.warn("Unhandled response: Request status: {}, CrawlLog status: {}, URL:{}", r.getStatusCode(), crawlLogEntry.getCrawlLog().getStatusCode(), r.getUrl());
                }
            } else {
                if (r.getStatusCode() == ExtraStatusCodes.CANCELED_BY_BROWSER.getCode()) {
                    requestFound = true;
                } else {
                    LOG.debug("Response not received (yet?): Request status: {}, URL:{}", r.getStatusCode(), r.getUrl());
                }
            }
        }

        if (requestFound) {
            if (crawlLogEntry.isResponseReceived()) {
                crawlLogEntry.getCrawlLog().setTimeStamp(now);
                CrawlLog enrichedCrawlLog = r.setCrawlLog(crawlLogEntry.getCrawlLog(), crawlLogEntry.isFromCache());
                if (!r.isFromCache()) {
                    try {
                        DbService.getInstance().getDbAdapter().saveCrawlLog(enrichedCrawlLog);
                    } catch (DbException e) {
                        throw new RuntimeException("Could not save crawl log", e);
                    }
                }
            }
            crawlLogEntry.resolved = true;
        } else {
            LOG.trace("Did not find request for {}", crawlLogEntry.uri);
        }
        return requestFound;
    }

    private void innerMatchCrawlLogAndRequest(MatchStatus status) {
        status.reset();
        if (!isCrawlLogsResolved()) {
            crawlLogs.stream().filter(e -> (!e.isResolved()))
                    .forEach(e -> findRequestForCrawlLog(e, browserSession.getUriRequests().getInitialRequest()));
            if (!isCrawlLogsResolved()) {
                LOG.trace("There are still unhandled crawl logs");
            }
        }

        browserSession.getUriRequests().getRequestStream().forEach(re -> {
            if (re.getCrawlLog() == null) {
                // Only requests that comes from the origin server should be added to the unhandled requests list
                if (!re.isFromCache() && re.isFromProxy() && re.getStatusCode() >= 0) {
                    LOG.error("Missing CrawlLog for {} {} {} {}, fromCache: {}, fromProxy: {}", re.getRequestId(),
                            re.getStatusCode(), re.getUrl(), re.getDiscoveryPath(), re.isFromCache(), re.isFromProxy());

                    status.unhandledRequests.add(re);
                }
            }
        });
    }

    private class MatchStatus {
        List<UriRequest> unhandledRequests = new ArrayList<>();

        void reset() {
            unhandledRequests.clear();
        }

        boolean allHandled() {
            return isCrawlLogsResolved() && unhandledRequests.isEmpty();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();

            sb.append("unhandledCrawlLogs={");
            crawlLogsLock.lock();
            try {
                crawlLogs.stream().filter(e -> !e.isResolved()).forEach(e -> sb.append("\n    [")
                        .append(e.isResponseReceived() ? e.getCrawlLog().getStatusCode() : "abort").append(", ")
                        .append(e.uri).append("], "));
            } finally {
                crawlLogsLock.unlock();
            }

            sb.append("},\nunhandledRequests={");
            unhandledRequests.forEach(r -> {
                sb.append("\n    [").append(r.getStatusCode()).append(", ");
                if (r.isFromCache()) {
                    sb.append("From Cache, ");
                }
                sb.append(r.getUrl()).append("], ");
            });

            sb.append("}");
            return sb.toString();
        }
    }
}
