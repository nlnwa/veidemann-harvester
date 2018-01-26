package no.nb.nna.veidemann.chrome.client;

import io.opentracing.Tracer;

import java.util.Objects;

public class ChromeDebugProtocolConfig {
    private final String host;
    private final int port;
    private Tracer tracer;
    private boolean activeSpanOnly = true;
    private int maxConnectionAttempts = 10;
    private int maxSendMessageAttempts = 3;
    private int maxOpenSessions = 20;
    private long protocolTimeoutMs = 5000;

    public ChromeDebugProtocolConfig(String host, int port) {
        this.host = Objects.requireNonNull(host, "Host must be set");
        this.port = port;
    }

    public ChromeDebugProtocolConfig withTracer(Tracer tracer) {
        this.tracer = tracer;
        return this;
    }

    public ChromeDebugProtocolConfig withActiveSpanOnly(boolean activeSpanOnly) {
        this.activeSpanOnly = activeSpanOnly;
        return this;
    }

    public ChromeDebugProtocolConfig withMaxConnectionAttempts(int maxConnectionAttempts) {
        this.maxConnectionAttempts = maxConnectionAttempts;
        return this;
    }

    public ChromeDebugProtocolConfig withMaxSendMessageAttempts(int maxSendMessageAttempts) {
        this.maxSendMessageAttempts = maxSendMessageAttempts;
        return this;
    }

    public ChromeDebugProtocolConfig withMaxOpenSessions(int maxOpenSessions) {
        this.maxOpenSessions = maxOpenSessions;
        return this;
    }

    public ChromeDebugProtocolConfig withProtocolTimeoutMs(long protocolTimeoutMs) {
        this.protocolTimeoutMs = protocolTimeoutMs;
        return this;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Tracer getTracer() {
        return tracer;
    }

    public boolean isActiveSpanOnly() {
        return activeSpanOnly;
    }

    public int getMaxConnectionAttempts() {
        return maxConnectionAttempts;
    }

    public int getMaxSendMessageAttempts() {
        return maxSendMessageAttempts;
    }

    public int getMaxOpenSessions() {
        return maxOpenSessions;
    }

    public long getProtocolTimeoutMs() {
        return protocolTimeoutMs;
    }
}
