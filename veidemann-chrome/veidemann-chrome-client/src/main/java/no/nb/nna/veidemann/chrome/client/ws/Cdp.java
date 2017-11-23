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
import com.google.gson.JsonElement;
import io.opentracing.ActiveSpan;
import io.opentracing.NoopActiveSpanSource;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 *
 */
public class Cdp implements WebSocketCallback {

    private static final Logger LOG = LoggerFactory.getLogger(Cdp.class);

    static Gson gson = new Gson();

    final AtomicLong idSeq = new AtomicLong(1);

    final ConcurrentHashMap<Long, CompletableFuture<JsonElement>> methodFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<CompletableFuture<JsonElement>>> eventFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<Consumer<JsonElement>>> eventListeners = new ConcurrentHashMap<>();

    final WebsocketClient websocketClient;

    final String host;

    final int port;

    final String scheme;

    final Tracer tracer;

    final boolean withActiveSpanOnly;

    public Cdp(final String host, final int port, final Tracer tracer, final boolean withActiveSpanOnly) {
        this.host = host;
        this.port = port;

        try {
            URL versionUrl = new URL("http", host, port, "/json/version");
            try (InputStream in = versionUrl.openStream()) {
                InputStreamReader inr = new InputStreamReader(in);
                Map version = gson.fromJson(inr, Map.class);
                URI webSocketUri = new URI((String) version.get("webSocketDebuggerUrl"));
                String browserVersion = (String) version.get("Browser");
                this.scheme = webSocketUri.getScheme();

                this.websocketClient = new WebsocketClient(this, webSocketUri);
                this.tracer = tracer;
                this.withActiveSpanOnly = withActiveSpanOnly;
            }
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Cdp(final String scheme, final String host, final int port, final String path, final Tracer tracer, final boolean withActiveSpanOnly) {
        this.scheme = scheme;
        this.host = host;
        this.port = port;

        try {
            URI webSocketUri = new URI(scheme, null, host, port, path, null, null);

            this.websocketClient = new WebsocketClient(this, webSocketUri);
            this.tracer = tracer;
            this.withActiveSpanOnly = withActiveSpanOnly;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Cdp createSessionClient(final String targetId) {
        return new Cdp(scheme, host, port, "/devtools/page/" + targetId, tracer, withActiveSpanOnly);
    }

    public CompletableFuture<JsonElement> call(String method, Map<String, Object> params) {
        try (ActiveSpan span = buildSpan(method)) {

            final ActiveSpan.Continuation cont = span.capture();
            CdpRequest request = new CdpRequest(idSeq.getAndIncrement(), method, params);
            CompletableFuture<JsonElement> future = new CompletableFuture<JsonElement>().whenComplete((json, error) -> {
                try (ActiveSpan activeSpan = cont.activate()) {
                    if (error != null) {
                        activeSpan.log(error.toString());
                    }
                }
            });

            methodFutures.put(request.id, future);

            String msg = gson.toJson(request);

            span.setTag("request", msg);

            if (LOG.isDebugEnabled()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Sent: {}", msg);
                } else {
                    LOG.debug("Sent: id={}, method={}", request.id, request.method);
                }
            }

            websocketClient.sendMessage(msg);
            return future;
        }
    }

    public <T> CompletableFuture<T> call(String method, Map<String, Object> params, Class<T> resultType) {
        return call(method, params).thenApply(result -> gson.fromJson(result, resultType));
    }

    public synchronized void addEventListener(String method, Consumer<JsonElement> listener) {
        List<Consumer<JsonElement>> list = eventListeners.get(method);
        if (list == null) {
            list = Collections.synchronizedList(new ArrayList<>());
            eventListeners.put(method, list);
        }
        list.add(listener);
    }

    public <T> void addEventListener(String method, Consumer<T> listener, Class<T> eventType) {
        addEventListener(method, el -> listener.accept(gson.fromJson(el, eventType)));
    }

    public synchronized <T> CompletableFuture<T> eventFuture(String method, Class<T> eventType) {
        CompletableFuture<JsonElement> future = new CompletableFuture<>();
        List<CompletableFuture<JsonElement>> list = eventFutures.get(method);
        if (list == null) {
            list = Collections.synchronizedList(new ArrayList<>());
            eventFutures.put(method, list);
        }
        list.add(future);
        return future.thenApply(el -> gson.fromJson(el, eventType));
    }

    @Override
    public void onMessageReceived(String msg) {
        CdpResponse response = gson.fromJson(msg, CdpResponse.class);

        if (response.method == null) {
            if (LOG.isDebugEnabled()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Received: {}", msg.substring(0, Math.min(msg.length(), 2048)));
                } else {
                    LOG.debug("Received: id={}, error={}", response.id, response.method, response.error != null);
                }
            }

            dispatchResponse(response);
        } else {
            if (LOG.isDebugEnabled()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Received: {}", msg.substring(0, Math.min(msg.length(), 2048)));
                } else {
                    LOG.debug("Received: event={}", response.method);
                }
            }

            dispatchEvent(response.method, response.params);
        }
    }

    synchronized void dispatchResponse(CdpResponse response) {
        CompletableFuture<JsonElement> future = methodFutures.remove(response.id);
        if (future != null) {
            if (response.error != null) {
                future.completeExceptionally(new CdpException(response.error.code, response.error.message));
            } else {
                future.complete(response.result);
            }
        }
    }

    synchronized void dispatchEvent(String method, JsonElement event) {
        List<CompletableFuture<JsonElement>> futures = eventFutures.remove(method);
        if (futures != null) {
            for (CompletableFuture<JsonElement> future : futures) {
                future.complete(event);
            }
        }

        for (Consumer<JsonElement> listener : eventListeners.getOrDefault(method, Collections.emptyList())) {
            listener.accept(event);
        }
    }

    ActiveSpan buildSpan(String operationName) {
        if (tracer == null || (withActiveSpanOnly && tracer.activeSpan() == null)) {
            return NoopActiveSpanSource.NoopActiveSpan.INSTANCE;
        }

        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .withTag(Tags.COMPONENT.getKey(), "java-ChromeDebugProtocolClient");

        ActiveSpan span = spanBuilder.startActive();
        return span;
    }

    public void close() {
        websocketClient.close();
    }

}
