package no.nb.nna.veidemann.chrome.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract class CommandBuilder<T extends Object> {
    private final ChromeDebugProtocolConfig config;

    protected CommandBuilder(ChromeDebugProtocolConfig config) {
        this.config = config;
    }

    public abstract CompletableFuture<T> runAsync() throws ClientClosedException, SessionClosedException;

    /**
     * Run request and wait for response.
     * <p/>
     *
     * @return the response
     * @throws ExecutionException     is thrown if the call fails
     * @throws TimeoutException       is thrown if no response is received within the time limit
     * @throws ClientClosedException  is thrown if the client is closed
     * @throws SessionClosedException is thrown if the current session is closed
     */
    public T run() throws ExecutionException, TimeoutException, ClientClosedException, SessionClosedException {
        try {
            return runAsync().get(config.getProtocolTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new TimeoutException("Call was interrupted: " + e.toString());
        }
    }

}
