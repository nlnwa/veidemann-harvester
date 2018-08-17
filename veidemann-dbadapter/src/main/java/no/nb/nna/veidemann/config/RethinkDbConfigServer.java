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
package no.nb.nna.veidemann.config;

import java.util.Map;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.typesafe.config.Config;
import no.nb.nna.veidemann.commons.settings.ConfigServer;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RethinkDbConfigServer implements ConfigServer {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConfigServer.class);

    Config config;

    static final RethinkDB r = RethinkDB.r;

    @Override
    public LogLevels getLogLevels() {
        String dbHost = config.getString("dbHost");
        int dbPort = config.getInt("dbPort");
        String dbName = config.getString("dbName");
        String dbUser = config.getString("dbUser");
        String dbPassword = config.getString("dbPassword");

        LOG.debug("Connecting Config Server to: {}:{}", dbHost, dbPort);
        LogLevels logLevels = LogLevels.getDefaultInstance();
        try (Connection conn = connect(dbHost, dbPort, dbName, dbUser, dbPassword);) {
            Map msg = r.table(Tables.SYSTEM.name).get("log_levels").pluck("logLevel").run(conn);
            logLevels = ProtoUtils.rethinkToProto(msg, LogLevels.class);
        } catch (Exception ex) {
            LOG.debug("Failed reading logger config from db");
        }

        return logLevels;
    }

    private Connection connect(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword) {
        return r.connection()
                .hostname(dbHost)
                .port(dbPort)
                .db(dbName)
                .user(dbUser, dbPassword)
                .connect();
    }

    @Override
    public int getConfigReloadInterval() {
        return config.getInt("configReloadInterval");
    }

    @Override
    public void init(Config config) {
        this.config = config;
    }

}
