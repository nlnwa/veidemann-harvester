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

import com.google.gson.JsonObject;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.opentracing.ActiveSpan;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolBase;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class CdpConnection extends Cdp implements WebSocketCallback {

    private static final Logger LOG = LoggerFactory.getLogger(CdpConnection.class);

    WebsocketClient websocketClient;

    final AtomicBoolean closed = new AtomicBoolean(false);

    String closedReason;

    private final EventLoopGroup workerGroup;

    URI webSocketUri;

    public CdpConnection(final ChromeDebugProtocolConfig config) {
        super(config);
        workerGroup = new NioEventLoopGroup(getConfig().getWorkerThreads());

        try {
            boolean connected = false;
            int connectAttempts = 0;
            while (!connected && connectAttempts < getConfig().getMaxConnectionAttempts()) {
                try {
                    if (getConfig().getBrowserWSEndpoint() != null && !getConfig().getBrowserWSEndpoint().trim().isEmpty()) {
                        webSocketUri = new URI(getConfig().getBrowserWSEndpoint());
                    } else {
                        URL versionUrl = new URL("http", getConfig().getHost(), getConfig().getPort(), "/json/version");
                        try (InputStream in = versionUrl.openStream()) {
                            InputStreamReader inr = new InputStreamReader(in);
                            Map version = GSON.fromJson(inr, Map.class);
                            webSocketUri = new URI((String) version.get("webSocketDebuggerUrl"));
                        }
                    }

                    this.websocketClient = new WebsocketClient(this, webSocketUri, getConfig(), workerGroup);
                    connected = true;
                } catch (IOException e) {
                    connectAttempts++;
                    LOG.debug("Could not connect to Chrome BrowserClient. Retrying in {}ms", getConfig().getReconnectDelay());
                    try {
                        Thread.sleep(getConfig().getReconnectDelay());
                    } catch (InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            }
        } catch (URISyntaxException e) {
            LOG.error("Failed to connect to Chrome BrowserClient.", e);
            closed.set(true);
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<CdpSession> createNewSessionClient(TargetInfo targetInfo) {
        return new AttachToTargetCmd(CdpConnection.this, targetInfo.targetId()).runAsync()
                .thenApply(r -> createSessionClient(r.sessionId()));
    }

    public <T> CompletableFuture<T> call(Command<T> command) {
        try (ActiveSpan span = buildSpan(command.getMethod())) {

            final ActiveSpan.Continuation cont = span.capture();

            if (isClosed()) {
                LOG.info("Calling {} on closed session. {}", command.getMethod(), getClosedReason());
                CompletableFuture<JsonObject> future = new CompletableFuture<>();
                future.completeExceptionally(new ClientClosedException(getClosedReason()));
            }

            CompletableFuture<JsonObject> future = new CompletableFuture<JsonObject>().whenComplete((json, error) -> {
                try (ActiveSpan activeSpan = cont.activate()) {
                    if (error != null) {
                        activeSpan.log(error.toString());
                    }
                }
            });

            methodFutures.put(command.getRequestId(), future);

            span.setTag("request", command.toString());

            if (LOG.isTraceEnabled()) {
                LOG.trace("Sent: {}", command);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Sent: id={}, method={}", command.getRequestId(), command.getMethod());
            }

            try {
                websocketClient.sendMessage(command.serialize());
            } catch (Throwable t) {
                methodFutures.remove(command.getRequestId());
                future.completeExceptionally(t);
            }
            return future.thenApply(result -> parseResult(result, command.getResultType()));
        }
    }

    public void setClientClosedListener(ClientClosedListener clientClosedListener) {
        this.clientClosedListener = clientClosedListener;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public String getClosedReason() {
        return closedReason;
    }

    public String getRemoteVersion() {
        try {
            return new GetBrowserVersionCmd(this).run().product();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onClose(String reason) {
        closedReason = reason;
        closed.set(true);
        websocketClient.close();
        if (clientClosedListener != null) {
            clientClosedListener.clientClosed(reason);
        }
        for (CdpSession session : sessions.values()) {
            session.onClose(reason);
        }
        sessions.clear();
    }

    public void dispose() {
        onClose("Closed by client");
        workerGroup.shutdownGracefully();
        websocketClient.close();
    }
}
