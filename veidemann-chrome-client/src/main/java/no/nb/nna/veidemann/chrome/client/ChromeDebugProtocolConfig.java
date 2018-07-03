/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.chrome.client;

import io.opentracing.Tracer;

import java.util.Objects;

public class ChromeDebugProtocolConfig {
    private String host;
    private int port;
    private String browserWSEndpoint;
    private Tracer tracer;
    private boolean activeSpanOnly = true;
    private int maxConnectionAttempts = 10;
    private long protocolTimeoutMs = 5000;
    private long reconnectDelay = 2000;
    private int workerThreads = 8;

    /**
     * Construct a new ChromeDebugProtocolConfig.
     *
     * @param host host name or ip where Chrome is listening
     * @param port port where Chrome is listening
     */
    public ChromeDebugProtocolConfig(String host, int port) {
        this.host = Objects.requireNonNull(host, "Host must be set");
    }

    /**
     * Construct a new ChromeDebugProtocolConfig.
     *
     * @param browserWSEndpoint Websocket endpoint where Chrome is listening
     */
    public ChromeDebugProtocolConfig(String browserWSEndpoint) {
        this.browserWSEndpoint = browserWSEndpoint;
    }

    public ChromeDebugProtocolConfig() {
    }

    public ChromeDebugProtocolConfig withHost(String host) {
        this.host = host;
        return this;
    }

    public ChromeDebugProtocolConfig withPort(int port) {
        this.port = port;
        return this;
    }

    public ChromeDebugProtocolConfig withBrowserWSEndpoint(String browserWSEndpoint) {
        this.browserWSEndpoint = browserWSEndpoint;
        return this;
    }

    /**
     * Set an OpenTracing tracer for tracing requests.
     *
     * @param tracer the OpenTracing tracer
     * @return this object for chaining
     */
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

    public ChromeDebugProtocolConfig withProtocolTimeoutMs(long protocolTimeoutMs) {
        this.protocolTimeoutMs = protocolTimeoutMs;
        return this;
    }

    public ChromeDebugProtocolConfig withReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
        return this;
    }

    public ChromeDebugProtocolConfig withWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
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

    public long getProtocolTimeoutMs() {
        return protocolTimeoutMs;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public String getBrowserWSEndpoint() {
        return browserWSEndpoint;
    }

}
