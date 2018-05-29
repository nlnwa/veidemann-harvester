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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ReportGrpc;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListReply;
import no.nb.nna.veidemann.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListReply;
import no.nb.nna.veidemann.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public class ReportService extends ReportGrpc.ReportImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReportService.class);

    private final DbAdapter db;

    public ReportService(DbAdapter db) {
        this.db = db;
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

//    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void executeJsQuery(String request, StreamObserver<ScreenshotListReply> respObserver) {
        try {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

            try (InputStream js = this.getClass().getClassLoader().getResourceAsStream("rethinkdbQueryParser.js");) {
                engine.eval(new InputStreamReader(js));
            }
            Invocable invocable = (Invocable) engine;

            Object qry = engine.eval("r.db('veidemann').table('crawl_log')");

            Object result = invocable.invokeFunction("qryToProto", qry);
            System.out.println("1: " + result);

//            respObserver.onNext(db.listScreenshots(request));
//            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
//            respObserver.onError(status.asException());
        }
    }
}
