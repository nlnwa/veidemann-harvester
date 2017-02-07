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
package no.nb.nna.broprox.contentwriter.settings;

/**
 * Configuration settings for Broprox.
 */
public class Settings {

    private int apiPort;

    private String warcDir;

    private int warcWriterPoolSize;

    private long warcFileSize;

    private boolean compressWarc;

    private String workDir;

    private String dbHost;

    private int dbPort;

    private String dbName;

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public String getWarcDir() {
        return warcDir;
    }

    public void setWarcDir(String warcDir) {
        this.warcDir = warcDir;
    }

    public int getWarcWriterPoolSize() {
        return warcWriterPoolSize;
    }

    public void setWarcWriterPoolSize(int warcWriterPoolSize) {
        this.warcWriterPoolSize = warcWriterPoolSize;
    }

    public long getWarcFileSize() {
        return warcFileSize;
    }

    public void setWarcFileSize(long warcFileSize) {
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

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public void setDbPort(int dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

}
