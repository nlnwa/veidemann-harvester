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
package no.nb.nna.broprox.db;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.net.Connection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import no.nb.nna.broprox.commons.AlreadyCrawledCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RethinkDbAlreadyCrawledCache implements AlreadyCrawledCache {

    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbAlreadyCrawledCache.class);

    static final RethinkDB r = RethinkDB.r;

    final Connection conn;

    public RethinkDbAlreadyCrawledCache(String dbHost, int dbPort, String dbName) {
        this(r.connection().hostname(dbHost).port(dbPort).db(dbName).connect());
    }

    public RethinkDbAlreadyCrawledCache(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void cleanExecution(String executionId) {
        executeRequest(r.table(RethinkDbAdapter.TABLES.ALREADY_CRAWLED_CACHE.name)
                .between(r.array(executionId, r.minval()), r.array(executionId, r.maxval()))
                .delete());
    }

    @Override
    public FullHttpResponse get(String uri, String executionId) {
        Map<String, Object> response = executeRequest(r.table(RethinkDbAdapter.TABLES.ALREADY_CRAWLED_CACHE.name)
                .get(r.array(executionId, uri)));

        if (response == null) {
            return null;
        }

        HttpVersion httpVersion = HttpVersion.valueOf((String) response.get("httpVersion"));
        HttpResponseStatus status = HttpResponseStatus.valueOf(((Long) response.get("responseStatus")).intValue());
        ByteBuf data = Unpooled.wrappedBuffer((byte[]) response.get("data"));

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(httpVersion, status, data);

        return httpResponse;
    }

    @Override
    public void put(HttpVersion httpVersion, HttpResponseStatus status, String uri, String executionId, ByteString cacheValue) {
        if (executionId == null) {
            return;
        }

        // Refuse to cache objects larger than 1MB
        if (cacheValue == null || cacheValue.size() > 1024 * 1024) {
            return;
        }

        Map<String, Object> rMap = r.hashMap("id", r.array(executionId, uri))
                .with("httpVersion", httpVersion.text())
                .with("responseStatus", status.code())
                .with("data", r.binary(cacheValue.toByteArray()));

        executeRequest(r.table(RethinkDbAdapter.TABLES.ALREADY_CRAWLED_CACHE.name)
                .insert(rMap)
                .optArg("conflict", "replace"));
    }

    <T> T executeRequest(ReqlExpr qry) {
        synchronized (this) {
            if (!conn.isOpen()) {
                try {
                    conn.connect();
                } catch (TimeoutException ex) {
                    throw new RuntimeException("Timed out waiting for connection");
                }
            }
        }

        try {
            T result = qry.run(conn);
            if (result instanceof Map
                    && ((Map) result).containsKey("errors")
                    && !((Map) result).get("errors").equals(0L)) {
                throw new DbException((String) ((Map) result).get("first_error"));
            }
            return result;
        } catch (ReqlError e) {
            throw new DbException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        conn.close();
    }

}
