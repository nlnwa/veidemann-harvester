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
package no.nb.nna.broprox.harvester.settings;

import no.nb.nna.broprox.commons.settings.CommonSettings;

/**
 * Configuration settings for Broprox.
 */
public class Settings extends CommonSettings {

    private int proxyPort;

    private int apiPort;

    private String browserHost;

    private int browserPort;

    private String workDir;

    private String contentWriterHost;

    private int contentWriterPort;

    private String robotsServiceHost;

    private int robotsServicePort;

    private String dnsServiceHost;

    private int dnsServicePort;

    public int getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
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

    public String getRobotsServiceHost() {
        return robotsServiceHost;
    }

    public void setRobotsServiceHost(String robotsServiceHost) {
        this.robotsServiceHost = robotsServiceHost;
    }

    public int getRobotsServicePort() {
        return robotsServicePort;
    }

    public void setRobotsServicePort(int robotsServicePort) {
        this.robotsServicePort = robotsServicePort;
    }

    public String getDnsServiceHost() {
        return dnsServiceHost;
    }

    public void setDnsServiceHost(String dnsServiceHost) {
        this.dnsServiceHost = dnsServiceHost;
    }

    public int getDnsServicePort() {
        return dnsServicePort;
    }

    public void setDnsServicePort(int dnsServicePort) {
        this.dnsServicePort = dnsServicePort;
    }

}
