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

    Map<String, BrowserSession> executionIdToSession = new HashMap<>();

    public synchronized void put(BrowserSession session) {
        executionIdToSession.put(session.getExecutionId(), session);
        LOG.debug("Currently open sessions: {}", executionIdToSession.size());
    }

    public synchronized BrowserSession get(String executionId) {
        BrowserSession session = executionIdToSession.get(executionId);
        if (session == null) {
            LOG.debug("Missing session for executionId {}", executionId);
        }
        return session;
    }

    public synchronized BrowserSession remove(String executionId) {
        return executionIdToSession.remove(executionId);
    }

    public synchronized BrowserSession remove(BrowserSession session) {
        Objects.requireNonNull(session);
        return executionIdToSession.remove(session.getExecutionId());
    }

    public boolean isEmpty() {
        return executionIdToSession.isEmpty();
    }

    public int size() {
        return executionIdToSession.size();
    }
}
