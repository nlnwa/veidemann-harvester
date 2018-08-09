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
package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.DbDrop;
import com.rethinkdb.gen.ast.TableDrop;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import no.nb.nna.veidemann.commons.db.CrawlQueueAdapter;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbInitializer;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.db.opentracing.ConnectionTracingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RethinkDbConnection implements DbServiceSPI {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConnection.class);

    static final RethinkDB r = RethinkDB.r;

    private Connection conn;

    private RethinkDbAdapter dbAdapter;

    private RethinkDbCrawlQueueAdapter queueAdapter;

    private RethinkDbInitializer dbInitializer;

    public <T> T exec(ReqlAst qry) throws DbConnectionException, DbQueryException {
        return exec("db-query", qry);
    }

    public <T> T exec(String operationName, ReqlAst qry) throws DbConnectionException, DbQueryException {
        synchronized (this) {
            if (!conn.isOpen()) {
                try {
                    conn.connect();
                } catch (TimeoutException ex) {
                    LOG.debug(ex.toString(), ex);
                    throw new DbConnectionException("Timed out waiting for connection", ex);
                }
            }
        }

        while (true) {
            try {
                OptArgs globalOpts = OptArgs.of(ConnectionTracingInterceptor.OPERATION_NAME_KEY, operationName);
                T result = qry.run(conn, globalOpts);
                if (result instanceof Map
                        && ((Map) result).containsKey("errors")
                        && !((Map) result).get("errors").equals(0L)) {
                    DbQueryException ex = new DbQueryException((String) ((Map) result).get("first_error"));
                    LOG.debug(ex.toString(), ex);
                    throw ex;
                }
                return result;
            } catch (ReqlOpFailedError e) {
                if (e.getTerm().isPresent()
                        && !((e.getTerm().get() instanceof DbDrop) || e.getTerm().get() instanceof TableDrop)) {

                    LOG.error("DB not available, waiting. Cause: {}", e.toString(), e);
                    r.db(conn.db().get()).wait_().optArg("wait_for", "ready_for_writes").run(conn);
                } else {
                    LOG.warn(e.toString(), e);
                    throw new DbQueryException(e.getMessage(), e);
                }
            } catch (ReqlError e) {
                LOG.warn(e.toString(), e);
                throw new DbQueryException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() {
        conn.close();
    }

    public Connection getConnection() {
        return conn;
    }

    @Override
    public DbAdapter getDbAdapter() {
        return dbAdapter;
    }

    @Override
    public CrawlQueueAdapter getCrawlQueueAdapter() {
        return queueAdapter;
    }

    @Override
    public DbInitializer getDbInitializer() {
        return dbInitializer;
    }

    public void connect(CommonSettings settings) throws DbConnectionException {
        conn = connect(settings.getDbHost(), settings.getDbPort(), settings.getDbName(), settings.getDbUser(),
                settings.getDbPassword(), 30);

        dbAdapter = new RethinkDbAdapter(this);
        queueAdapter = new RethinkDbCrawlQueueAdapter(this);
        dbInitializer = new RethinkDbInitializer(this);
    }

    private Connection connect(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword,
                               int reConnectAttempts) throws DbConnectionException {
        Connection c = null;
        int attempts = 0;
        while (c == null) {
            attempts++;
            try {
                c = r.connection()
                        .hostname(dbHost)
                        .port(dbPort)
                        .db(dbName)
                        .user(dbUser, dbPassword)
                        .connect();
            } catch (ReqlDriverError e) {
                LOG.warn(e.getMessage());
                if (attempts < reConnectAttempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    LOG.error("Too many connection attempts, giving up");
                    throw new DbConnectionException("Too many connection attempts", e);
                }
            }
        }
        return new ConnectionTracingInterceptor(c, true);
    }
}
