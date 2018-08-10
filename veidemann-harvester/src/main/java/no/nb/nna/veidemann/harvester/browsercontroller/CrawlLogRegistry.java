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
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog.Builder;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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

    public class Entry {
        final long proxyRequestId;
        final String uri;
        private CrawlLog.Builder crawlLog;
        private boolean resolved = false;

        public Entry(long proxyRequestId, String uri) {
            this.proxyRequestId = proxyRequestId;
            this.uri = uri;
        }

        public void setCrawlLog(Builder crawlLog) {
            crawlLogsLock.lock();
            try {
                this.crawlLog = crawlLog;
                lastActivityTime = System.currentTimeMillis();
                crawlLogsUpdate.signalAll();
            } finally {
                crawlLogsLock.unlock();
            }
        }

        public Builder getCrawlLog() {
            if (crawlLog == null) {
                NullPointerException e = new NullPointerException("Crawl log is null");
                LOG.warn("Trying to access missing crawl log", e);
                throw e;
            }
            return crawlLog;
        }

        boolean isResponseReceived() {
            return crawlLog != null;
        }

        boolean isResolved() {
            return resolved;
        }
    }

    public CrawlLogRegistry(final BrowserSession session, final long pageLoadTimeout, final long maxIdleTime) {
        this.browserSession = session;
        this.pageLoadTimeout = pageLoadTimeout;
        this.maxIdleTime = maxIdleTime;
        matcherThread = new Matcher();
        matcherThread.start();
    }

    public Entry registerProxyRequest(long proxyRequestId, String uri) {
        crawlLogsLock.lock();
        try {
            Entry crawlLogEntry = new Entry(proxyRequestId, uri);
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

    private boolean innerFindRequestForCrawlLog(Entry crawlLogEntry, UriRequest r) {
        boolean requestFound = false;
        Timestamp now = ProtoUtils.getNowTs();
        if (r.getCrawlLog() == null
                && !r.isFromCache()
                && Objects.equals(r.getUrl(), crawlLogEntry.uri)) {

            if (crawlLogEntry.isResponseReceived() && crawlLogEntry.getCrawlLog().getStatusCode() == r.getStatusCode()) {
                requestFound = true;
            } else if (r.getStatusCode() == ExtraStatusCodes.CANCELED_BY_BROWSER.getCode()) {
                if (crawlLogEntry.isResponseReceived()) {
                    r.setStatusCode(crawlLogEntry.getCrawlLog().getStatusCode());
                    requestFound = true;
                } else {
                    requestFound = true;
                }
            }
        }

        if (requestFound) {
            if (crawlLogEntry.isResponseReceived()) {
                crawlLogEntry.getCrawlLog().setTimeStamp(now);
                CrawlLog enrichedCrawlLog = r.setCrawlLog(crawlLogEntry.getCrawlLog());
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
                LOG.trace("Missing CrawlLog for {} {} {} {}", re.getRequestId(), re.getStatusCode(), re.getUrl(),
                        re.getDiscoveryPath());

                // Only requests that comes from the origin server should be added to the unhandled requests list
                if (!re.isFromCache() && re.isFromProxy() && re.getStatusCode() >= 0) {
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

            sb.append("}, unhandledRequests={");
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
