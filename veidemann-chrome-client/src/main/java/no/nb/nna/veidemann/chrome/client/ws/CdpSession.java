package no.nb.nna.veidemann.chrome.client.ws;

import com.google.gson.JsonObject;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class CdpSession extends Cdp {
    private static final Logger LOG = LoggerFactory.getLogger(CdpSession.class);
    protected final String sessionId;
    protected final Cdp client;

    public CdpSession(String sessionId, Cdp client) {
        super(client.config);
        this.sessionId = sessionId;
        this.client = client;
    }

    public <T> CompletableFuture<T> call(Command<T> command) {
        if (isClosed()) {
            LOG.info("Calling {} on closed session. {}", command.getMethod(), getClosedReason());
            CompletableFuture<JsonObject> future = new CompletableFuture<>();
            future.completeExceptionally(new ClientClosedException(getClosedReason()));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        methodFutures.put(command.getRequestId(), future);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Sent: {}", command.serialize());
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Sent: id={}, method={}", command.getRequestId(), command.getMethod());
        }

        SendMessageToTargetCmd encapsulatedMessage = new SendMessageToTargetCmd(client, sessionId, command);
        try {
            CompletableFuture f = encapsulatedMessage.runAsync();
            if (f.isCompletedExceptionally()) {
                // Force the exception to be thrown
                f.get();
            }
        } catch (Throwable t) {
            methodFutures.remove(command.getRequestId());
            future.completeExceptionally(new CdpException("Calling method " + command.getMethod() + " failed", t));
        }
        return future.thenApply(result -> parseResult(result, command.resultType));
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    @Override
    public String getClosedReason() {
        return client.getClosedReason();
    }

    @Override
    public String getRemoteVersion() {
        return client.getRemoteVersion();
    }

    @Override
    public void onClose(String reason) {
        Exception ex = new ClientClosedException(reason);
        for (CompletableFuture<JsonObject> m : methodFutures.values()) {
            m.obtrudeException(ex);
        }
        methodFutures.clear();
        eventListeners.clear();
        client.removeSessionClient(sessionId);
        LOG.debug("Session closed. Session id: {}", sessionId);
    }

    public void detach() throws TimeoutException, ExecutionException {
        new DetachFromTargetCmd(client, sessionId).run();
    }
}
