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
package no.nb.nna.veidemann.contentwriter.settings;

import com.typesafe.config.ConfigMemorySize;
import no.nb.nna.veidemann.commons.settings.CommonSettings;

/**
 * Configuration settings for Veidemann Content Writer.
 */
public class Settings extends CommonSettings {

    private int apiPort;

    private String hostName;

    private String warcDir;

    private String filePrefix;

    private int warcWriterPoolSize;

    private ConfigMemorySize warcFileSize;

    private boolean compressWarc;

    private String workDir;

    private boolean unsafe;

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getWarcDir() {
        return warcDir;
    }

    public void setWarcDir(String warcDir) {
        this.warcDir = warcDir;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public int getWarcWriterPoolSize() {
        return warcWriterPoolSize;
    }

    public void setWarcWriterPoolSize(int warcWriterPoolSize) {
        this.warcWriterPoolSize = warcWriterPoolSize;
    }

    public ConfigMemorySize getWarcFileSize() {
        return warcFileSize;
    }

    public void setWarcFileSize(ConfigMemorySize warcFileSize) {
        this.warcFileSize = warcFileSize;
    }

    public boolean isCompressWarc() {
        return compressWarc;
    }

    public void setCompressWarc(boolean compressWarc) {
        this.compressWarc = compressWarc;
    }

    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public boolean isUnsafe() {
        return unsafe;
    }

    public void setUnsafe(boolean unsafe) {
        this.unsafe = unsafe;
    }

}
