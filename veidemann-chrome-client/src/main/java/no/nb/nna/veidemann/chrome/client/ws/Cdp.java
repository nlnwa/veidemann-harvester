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
package no.nb.nna.veidemann.chrome.client.ws;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.opentracing.ActiveSpan;
import io.opentracing.NoopActiveSpanSource;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 *
 */
public abstract class Cdp implements WebSocketCallback {

    private static final Logger LOG = LoggerFactory.getLogger(Cdp.class);

    final static int MAX_TOSTRING_SIZE = 250;

    final static Gson GSON = new Gson();

    private final AtomicLong idSeq = new AtomicLong(1);

    final ConcurrentHashMap<Long, CompletableFuture<JsonObject>> methodFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<CompletableFuture<JsonObject>>> eventFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<Consumer<JsonObject>>> eventListeners = new ConcurrentHashMap<>();

    protected final ConcurrentHashMap<String, CdpSession> sessions = new ConcurrentHashMap<>();

    final ChromeDebugProtocolConfig config;

    ClientClosedListener clientClosedListener;

    public Cdp(final ChromeDebugProtocolConfig config) {
        this.config = config;
    }

    public CdpSession createSessionClient(final String sessionId) {
        CdpSession newSession = new CdpSession(sessionId, this);
        sessions.put(sessionId, newSession);
        return newSession;
    }

    public void removeSessionClient(final String sessionId) {
        sessions.remove(sessionId);
    }

    public abstract <T> CompletableFuture<T> call(Command<T> command);

    public void addEventListener(String method, Consumer<JsonObject> listener) {
        List<Consumer<JsonObject>> list = eventListeners.compute(method, (k, v) -> {
            if (v == null) {
                v = Collections.synchronizedList(new ArrayList<>());
            }
            v.add(listener);
            return v;
        });
    }

    public <T> void addEventListener(String method, Consumer<T> listener, Class<T> eventType) {
        addEventListener(method, el -> listener.accept(parseResult(el, eventType)));
    }

    public <T> CompletableFuture<T> eventFuture(String method, Class<T> eventType) {
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        List<CompletableFuture<JsonObject>> list = eventFutures.compute(method, (k, v) -> {
            if (v == null) {
                v = Collections.synchronizedList(new ArrayList<>());
            }
            v.add(future);
            return v;
        });
        return future.thenApply(el -> parseResult(el, eventType));
    }

    @Override
    public void onMessageReceived(String msg) {
        CdpResponse response = GSON.fromJson(msg, CdpResponse.class);

        if (response.id != 0) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received: {}", response);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Received: id={}, error={}", response.id, response.error);
            }

            dispatchResponse(response);
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received: {}", response);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Received: event={}", response.method);
            }

            if ("Target.receivedMessageFromTarget".equals(response.method)) {
                CdpSession session = sessions.get(response.params.get("sessionId").getAsString());
                if (session != null) {
                    session.onMessageReceived(response.params.get("message").getAsString());
                }
            } else if ("Target.detachedFromTarget".equals(response.method)) {
                CdpSession session = sessions.get(response.params.get("sessionId").getAsString());
                if (session != null) {
                    session.onClose("");
                }
                sessions.remove(session.sessionId);
            } else {
                dispatchEvent(response.method, response.params);
            }
        }
    }

    void dispatchResponse(CdpResponse response) {
        CompletableFuture<JsonObject> future = methodFutures.remove(response.id);
        if (future != null) {
            if (response.error != null) {
                future.completeExceptionally(new CdpException(response.error.code, response.error.message));
            } else {
                future.complete(response.result);
            }
        }
    }

    void dispatchEvent(String method, JsonObject event) {
        List<CompletableFuture<JsonObject>> futures = eventFutures.remove(method);
        if (futures != null) {
            for (CompletableFuture<JsonObject> future : futures) {
                future.complete(event);
            }
        }

        for (Consumer<JsonObject> listener : eventListeners.getOrDefault(method, Collections.emptyList())) {
            listener.accept(event);
        }
    }

    <T> T parseResult(JsonObject result, Class<T> resultType) {
        if (resultType.equals(Void.TYPE)) {
            return null;
        }
        return GSON.fromJson(result, resultType);
    }

    ActiveSpan buildSpan(String operationName) {
        if (config.getTracer() == null || (config.isActiveSpanOnly() && config.getTracer().activeSpan() == null)) {
            return NoopActiveSpanSource.NoopActiveSpan.INSTANCE;
        }

        Tracer.SpanBuilder spanBuilder = config.getTracer().buildSpan(operationName)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .withTag(Tags.COMPONENT.getKey(), "java-ChromeDebugProtocolClient");

        ActiveSpan span = spanBuilder.startActive();
        return span;
    }

    public void setClientClosedListener(ClientClosedListener clientClosedListener) {
        this.clientClosedListener = clientClosedListener;
    }

    public abstract boolean isClosed();

    public abstract String getClosedReason();

    public abstract String getRemoteVersion();

    public ChromeDebugProtocolConfig getConfig() {
        return config;
    }

    public long getNextRequestId() {
        return idSeq.getAndIncrement();
    }
}
