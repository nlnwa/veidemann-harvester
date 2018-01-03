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

import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog.Builder;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CrawlLogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(CrawlLogRegistry.class);

    private final DbAdapter db;
    private final BrowserSession browserSession;

    private final List<Builder> crawlLogs = new ArrayList<>();
    private final Lock crawlLogsLock = new ReentrantLock();
    private final Condition crawlLogsUpdate = crawlLogsLock.newCondition();
    private final Condition matchingFinished = crawlLogsLock.newCondition();

    private final long pageLoadTimeout;
    private final long maxIdleTime;
    private final Matcher matcherThread;
    private boolean running;
    private long startTime = System.currentTimeMillis();
    private long lastActivityTime = System.currentTimeMillis();

    public CrawlLogRegistry(final DbAdapter db, final BrowserSession session, final long pageLoadTimeout, final long maxIdleTime) {
        this.db = db;
        this.browserSession = session;
        this.pageLoadTimeout = pageLoadTimeout;
        this.maxIdleTime = maxIdleTime;
        matcherThread = new Matcher();
        running = true;
        matcherThread.start();
    }

    public void addCrawlLog(CrawlLog.Builder crawlLog) {
        crawlLogsLock.lock();
        try {
            crawlLogs.add(crawlLog);
            lastActivityTime = System.currentTimeMillis();
            crawlLogsUpdate.signalAll();
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

    public void deleteCrawlLog(CrawlLog.Builder crawlLog) {
        crawlLogsLock.lock();
        try {
            crawlLogs.remove(crawlLog);
        } finally {
            crawlLogsLock.unlock();
        }
    }

    public List<CrawlLog.Builder> getCrawlLogs() {
        crawlLogsLock.lock();
        try {
            return new ArrayList<>(crawlLogs);
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
            crawlLogsLock.lock();
            MatchStatus status = new MatchStatus();
            try {
                while (running) {
                    boolean gotSignal = crawlLogsUpdate.await(maxIdleTime, TimeUnit.MILLISECONDS);
                    if (gotSignal) {
                        LOG.trace("Got activity signal");
                    }
                    if ((System.currentTimeMillis() - lastActivityTime) >= maxIdleTime) {
                        LOG.debug("Timed out waiting for network activity");
                        running = false;
                    } else if ((System.currentTimeMillis() - startTime) >= pageLoadTimeout) {
                        LOG.info("Pageload timed out");
                        running = false;
                    }
                }
                innerMatchCrawlLogAndRequest(status);
                if (!status.allHandled()) {
                    LOG.warn("Not resolved. Status: {}", status);
                }

                matchingFinished.signalAll();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                crawlLogsLock.unlock();
            }
            LOG.debug("Finished matching. Status {}", status);
        }
    }

    public void waitForMatcherToFinish() {
        crawlLogsLock.lock();
        try {
            while (running) {
                matchingFinished.awaitUninterruptibly();
            }
        } finally {
            crawlLogsLock.unlock();
        }
    }

    private boolean findRequestForCrawlLog(CrawlLog.Builder crawlLog, UriRequest r) {
        if (innerFindRequestForCrawlLog(crawlLog, r)) {
            return true;
        }
        for (UriRequest c : r.getChildren()) {
            findRequestForCrawlLog(crawlLog, c);
        }
        return false;
    }

    private boolean innerFindRequestForCrawlLog(CrawlLog.Builder crawlLog, UriRequest r) {
        if (!r.isFromCache() && Objects.equals(r.getUrl(), crawlLog.getRequestedUri()) && crawlLog.getStatusCode() == r.getStatusCode()) {
            if (crawlLog.getWarcId().isEmpty()) {
                r.setFromCache(true);
            } else {
                db.saveCrawlLog(r.setCrawlLog(crawlLog));
            }
            deleteCrawlLog(crawlLog);
            return true;
        }
        LOG.debug("Did not find request for {}", crawlLog.getRequestedUri());
        return false;
    }

    private void innerMatchCrawlLogAndRequest(MatchStatus status) throws InterruptedException {
        status.reset();
        List<CrawlLog.Builder> crawlLogList = getCrawlLogs();
        if (!crawlLogList.isEmpty()) {
            for (CrawlLog.Builder c : crawlLogList) {
                findRequestForCrawlLog(c, browserSession.getUriRequests().getInitialRequest());
            }
            crawlLogList = getCrawlLogs();
            if (!crawlLogList.isEmpty()) {
                LOG.trace("There are still {} unhandled crawl logs", crawlLogList.size());
                status.unhandledCrawlLogs.addAll(crawlLogList);
            }
        }
        for (UriRequest re : browserSession.getUriRequests().getAllRequests()) {
            if (!re.isFromCache() && re.getStatusCode() >= 0 && re.getCrawlLog() == null) {
                LOG.trace("Missing CrawlLog for {}", re.getRequestId());
                status.unhandledRequests.add(re);
            }
        }
    }

    private class MatchStatus {
        List<CrawlLog.Builder> unhandledCrawlLogs = new ArrayList<>();
        List<UriRequest> unhandledRequests = new ArrayList<>();

        void reset() {
            unhandledCrawlLogs.clear();
            unhandledRequests.clear();
        }

        boolean allHandled() {
            return unhandledCrawlLogs.isEmpty() && unhandledRequests.isEmpty();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("unhandledCrawlLogs={");
            unhandledCrawlLogs.forEach(c -> sb.append(c.getRequestedUri()).append(", "));
            sb.append("}, unhandledRequests=");
            unhandledRequests.forEach(r -> sb.append(r.getUrl()).append(", "));
            sb.append("}");
            return sb.toString();
        }
    }
}
