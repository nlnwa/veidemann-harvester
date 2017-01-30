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
package no.nb.nna.broprox.chrome.client.ws;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Cdp implements WebSocketCallback {

    private static final Logger LOG = LoggerFactory.getLogger(Cdp.class);

    static Gson gson = new Gson();

    final AtomicLong idSeq = new AtomicLong(0);

    final ConcurrentHashMap<Long, CompletableFuture<JsonElement>> methodFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<CompletableFuture<JsonElement>>> eventFutures = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, List<Consumer<JsonElement>>> eventListeners = new ConcurrentHashMap<>();

    final WebsocketClient websocketClient;

    public Cdp(String uri) {
        this.websocketClient = new WebsocketClient(this);
        this.websocketClient.connect(uri);
    }

    public Cdp(URI uri) {
        this.websocketClient = new WebsocketClient(this);
        this.websocketClient.connect(uri);
    }

    public CompletableFuture<JsonElement> call(String method, Map<String, Object> params) {

        CdpRequest request = new CdpRequest(idSeq.getAndIncrement(), method, params);
        CompletableFuture<JsonElement> future = new CompletableFuture<>();
        methodFutures.put(request.id, future);

        String msg = gson.toJson(request);
        LOG.trace(msg);

        websocketClient.sendMessage(msg);

        return future;
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
        LOG.trace("Message: {}", msg.substring(0, Math.min(msg.length(), 2048)));
        CdpResponse response = gson.fromJson(msg, CdpResponse.class);

        if (response.method == null) {
            dispatchResponse(response);
        } else {
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

    public void close() {
        websocketClient.close();
    }

}
