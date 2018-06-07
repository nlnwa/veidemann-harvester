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
package no.nb.nna.veidemann.controller;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.net.Cursor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import no.nb.nna.veidemann.api.ReportGrpc;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ExecuteDbQueryReply;
import no.nb.nna.veidemann.api.ReportProto.ExecuteDbQueryRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.controller.query.QueryEngine;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ReportService extends ReportGrpc.ReportImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReportService.class);

    private final DbAdapter db;

    private final Gson gson;

    public ReportService(DbAdapter db) {
        this.db = db;
        gson = new GsonBuilder()
                .create();
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void listCrawlLogs(CrawlLogListRequest request, StreamObserver<CrawlLogListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlLogs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void listPageLogs(PageLogListRequest request, StreamObserver<PageLogListReply> respObserver) {
        try {
            respObserver.onNext(db.listPageLogs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void listScreenshots(ScreenshotListRequest request, StreamObserver<ScreenshotListReply> respObserver) {
        try {
            respObserver.onNext(db.listScreenshots(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void executeDbQuery(ExecuteDbQueryRequest request, StreamObserver<ExecuteDbQueryReply> respObserver) {
        try {
            ReqlAst qry = QueryEngine.getInstance().parseQuery(request.getQuery());
            int limit = request.getLimit();

            // Default limit
            if (limit == 0) {
                limit = 50;
            }

            Object result = RethinkDbConnection.getInstance().exec("js-query", qry);
            if (result != null) {
                if (result instanceof Cursor) {
                    try (Cursor c = (Cursor) result) {
                        int index = 0;
                        while (!((ServerCallStreamObserver) respObserver).isCancelled()
                                && c.hasNext() && (limit == -1 || index++ < limit)) {
                            try {
                                Object r = c.next(1000);
                                respObserver.onNext(recordToExecuteDbQueryReply(r));
                            } catch (TimeoutException e) {
                                // Timeout is ok
                            }
                        }
                    }
                } else {
                    respObserver.onNext(recordToExecuteDbQueryReply(result));
                }
            }

            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    private ExecuteDbQueryReply recordToExecuteDbQueryReply(Object record) {
        return ExecuteDbQueryReply.newBuilder()
                .setRecord(gson.toJson(record))
                .build();
    }
}
