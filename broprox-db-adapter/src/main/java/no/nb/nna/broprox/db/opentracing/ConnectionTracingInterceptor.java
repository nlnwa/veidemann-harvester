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
package no.nb.nna.broprox.db.opentracing;

import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import io.opentracing.ActiveSpan;
import io.opentracing.NoopActiveSpanSource;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConnectionTracingInterceptor extends Connection {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTracingInterceptor.class);

    public static final String OPERATION_NAME_KEY = "opertaionName";

    private final Connection conn;

    private final boolean withActiveSpanOnly;

    public ConnectionTracingInterceptor(Connection conn) {
        this(conn, false);
    }

    public ConnectionTracingInterceptor(Connection conn, boolean withActiveSpanOnly) {
        super(Connection.build());
        this.conn = conn;
        this.withActiveSpanOnly = withActiveSpanOnly;
    }

    @Override
    public void runNoReply(ReqlAst term, OptArgs globalOpts) {
        try (ActiveSpan span = buildSpan(globalOpts, "runNoReply")) {
            conn.runNoReply(term, globalOpts);
        }
    }

    @Override
    public <T, P> T run(ReqlAst term, OptArgs globalOpts, Optional<Class<P>> pojoClass, Optional<Long> timeout) {
        try (ActiveSpan span = buildSpan(globalOpts, "run")) {
            return conn.run(term, globalOpts, pojoClass, timeout);
        }
    }

    @Override
    public <T, P> T run(ReqlAst term, OptArgs globalOpts, Optional<Class<P>> pojoClass) {
        try (ActiveSpan span = buildSpan(globalOpts, "run")) {
            return conn.run(term, globalOpts, pojoClass);
        }
    }

    @Override
    public void noreplyWait() {
        conn.noreplyWait();
    }

    @Override
    public Optional<Long> timeout() {
        return conn.timeout();
    }

    @Override
    public void use(String db) {
        conn.use(db);
    }

    @Override
    public void close(boolean shouldNoreplyWait) {
        conn.close(shouldNoreplyWait);
    }

    @Override
    public void close() {
        conn.close();
    }

    @Override
    public boolean isOpen() {
        return conn.isOpen();
    }

    @Override
    public Optional<SocketAddress> clientAddress() {
        return conn.clientAddress();
    }

    @Override
    public Optional<Integer> clientPort() {
        return conn.clientPort();
    }

    @Override
    public Connection reconnect(boolean noreplyWait, Optional<Long> timeout) throws TimeoutException {
        return conn.reconnect(noreplyWait, timeout);
    }

    @Override
    public Connection reconnect() {
        return conn.reconnect();
    }

    @Override
    public void connect() throws TimeoutException {
        conn.connect();
    }

    @Override
    public Optional<String> db() {
        return conn.db();
    }

    ActiveSpan buildSpan(OptArgs globalOpts, String defaultOperationName) {
        String operationName;
        ReqlAst operationNameDatum = globalOpts.remove(OPERATION_NAME_KEY);

        if (withActiveSpanOnly && GlobalTracer.get().activeSpan() == null) {
            return NoopActiveSpanSource.NoopActiveSpan.INSTANCE;
        }

        if (operationNameDatum != null && (operationNameDatum instanceof Datum)) {
            operationName = ((Datum) operationNameDatum).datum.toString();
        } else {
            operationName = defaultOperationName;
        }

        Tracer.SpanBuilder spanBuilder = GlobalTracer.get().buildSpan(operationName)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .withTag(Tags.COMPONENT.getKey(), "java-rethinkDb")
                .withTag(Tags.DB_TYPE.getKey(), "rethinkDb");

        ActiveSpan span = spanBuilder.startActive();
        return span;
    }

}
