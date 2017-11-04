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
package no.nb.nna.veidemann.dnsresolver.settings;

import java.util.List;

import no.nb.nna.veidemann.commons.settings.CommonSettings;

/**
 * Configuration settings for Veidemann DNS Resolver.
 */
public class Settings extends CommonSettings {

    private int apiPort;

    private String contentWriterHost;

    private int contentWriterPort;

    private List<String> dnsServers;

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
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

    public List<String> getDnsServers() {
        return dnsServers;
    }

    public void setDnsServers(List<String> dnsServers) {
        this.dnsServers = dnsServers;
    }

}
