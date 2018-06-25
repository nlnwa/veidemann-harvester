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
package no.nb.nna.veidemann.harvester.settings;

import no.nb.nna.veidemann.commons.settings.CommonSettings;

/**
 * Configuration settings for Veidemann harvester.
 */
public class Settings extends CommonSettings {

    private int proxyPort;

    private String frontierHost;

    private int frontierPort;

    private String browserHost;

    private int browserPort;

    private String browserWSEndpoint;

    private String cacheHost;

    private int cachePort;

    private String workDir;

    private String contentWriterHost;

    private int contentWriterPort;

    private String dnsResolverHost;

    private int dnsResolverPort;

    private int maxOpenSessions;

    public int getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getFrontierHost() {
        return frontierHost;
    }

    public void setFrontierHost(String frontierHost) {
        this.frontierHost = frontierHost;
    }

    public int getFrontierPort() {
        return frontierPort;
    }

    public void setFrontierPort(int frontierPort) {
        this.frontierPort = frontierPort;
    }

    public String getBrowserWSEndpoint() {
        return browserWSEndpoint;
    }

    public void setBrowserWSEndpoint(String browserWSEndpoint) {
        this.browserWSEndpoint = browserWSEndpoint;
    }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public String getBrowserHost() {
        return browserHost;
    }

    public void setBrowserHost(String browserHost) {
        this.browserHost = browserHost;
    }

    public int getBrowserPort() {
        return browserPort;
    }

    public void setBrowserPort(int browserPort) {
        this.browserPort = browserPort;
    }

    public String getContentWriterHost() {
        return contentWriterHost;
    }

    public void setContentWriterHost(String contentWriterHost) {
        this.contentWriterHost = contentWriterHost;
    }

    public int getContentWriterPort() {
        return contentWriterPort;
    }

    public void setContentWriterPort(int contentWriterPort) {
        this.contentWriterPort = contentWriterPort;
    }

    public String getDnsResolverHost() {
        return dnsResolverHost;
    }

    public void setDnsResolverHost(String dnsResolverHost) {
        this.dnsResolverHost = dnsResolverHost;
    }

    public int getDnsResolverPort() {
        return dnsResolverPort;
    }

    public void setDnsResolverPort(int dnsResolverPort) {
        this.dnsResolverPort = dnsResolverPort;
    }

    public String getCacheHost() {
        return cacheHost;
    }

    public void setCacheHost(String cacheHost) {
        this.cacheHost = cacheHost;
    }

    public int getCachePort() {
        return cachePort;
    }

    public void setCachePort(int cachePort) {
        this.cachePort = cachePort;
    }

    public int getMaxOpenSessions() {
        return maxOpenSessions;
    }

    public void setMaxOpenSessions(int maxOpenSessions) {
        this.maxOpenSessions = maxOpenSessions;
    }
}
