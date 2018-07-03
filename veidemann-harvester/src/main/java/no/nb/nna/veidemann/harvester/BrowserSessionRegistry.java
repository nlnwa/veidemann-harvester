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
package no.nb.nna.veidemann.harvester;

import no.nb.nna.veidemann.harvester.browsercontroller.BrowserSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Registry which allows both BrowserController and Proxy to access information about a request.
 */
public class BrowserSessionRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(BrowserSessionRegistry.class);

    Map<Integer, BrowserSession> proxyIdToSession = new HashMap<>();

    public synchronized void put(BrowserSession session) {
        proxyIdToSession.put(session.getProxyId(), session);
        LOG.debug("Currently open sessions: {}", proxyIdToSession.size());
    }

    public synchronized BrowserSession get(Integer proxyId) {
        BrowserSession session = proxyIdToSession.get(proxyId);
        if (session == null) {
            LOG.error("Missing session for proxyId {}", proxyId);
        }
        return session;
    }

    public synchronized BrowserSession remove(Integer proxyId) {
        return proxyIdToSession.remove(proxyId);
    }

    public synchronized BrowserSession remove(BrowserSession session) {
        Objects.requireNonNull(session);
        return proxyIdToSession.remove(session.getProxyId());
    }

    public boolean isEmpty() {
        return proxyIdToSession.isEmpty();
    }

    public int size() {
        return proxyIdToSession.size();
    }
}
