package no.nb.nna.veidemann.chrome.client.ws;

import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.SessionClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class Command<T extends Object> {
    private static final Logger LOG = LoggerFactory.getLogger(Command.class);

    private final Cdp client;
    private final CdpRequest request;
    final String domain;
    final Class<T> resultType;

    protected Command(Cdp client, String domain, String method, Class<T> resultType) {
        this.client = client;
        this.domain = domain;
        this.request = new CdpRequest(client.getNextRequestId(), domain + "." + method);
        this.resultType = resultType;
    }

    protected Command withParam(String name, Object value) {
        this.request.params.put(name, value);
        return this;
    }

    public String getDomain() {
        return domain;
    }

    public String getMethod() {
        return request.method;
    }

    public long getRequestId() {
        return request.id;
    }

    String serialize() {
        try {
            return Cdp.GSON.toJson(request);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    Class<T> getResultType() {
        return resultType;
    }

    /**
     * Run request and wait for response.
     * <p/>
     *
     * @return the response
     * @throws ExecutionException     is thrown if the call fails
     * @throws TimeoutException       is thrown if no response is received within the time limit
     */
    public T run() throws ExecutionException, TimeoutException {
        try {
            return runAsync().get(client.getConfig().getProtocolTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new TimeoutException("Call was interrupted: " + e.toString());
        }
    }

    /**
     * Run request.
     * <p>
     *
     * @return a {@link CompletableFuture} from which the result might be retrieved
     */
    public CompletableFuture<T> runAsync() {
        return client.call(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
        String msg = serialize();

        // Restrict size of msg
        if (msg.length() > Cdp.MAX_TOSTRING_SIZE) {
            msg = msg.substring(0, Cdp.MAX_TOSTRING_SIZE) + "... ";
        }

        sb.append(msg);
        return sb.toString();
    }

    public final class CdpRequest {
        final long id;
        final String method;
        final Map<String, Object> params;

        CdpRequest(long id, String method) {
            this.id = id;
            this.method = method;
            this.params = new HashMap<>();
        }
    }

}
