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
package no.nb.nna.broprox.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.api.ReportGrpc;
import no.nb.nna.broprox.api.ReportProto.CrawlLogListReply;
import no.nb.nna.broprox.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.broprox.api.ReportProto.PageLogListReply;
import no.nb.nna.broprox.api.ReportProto.PageLogListRequest;
import no.nb.nna.broprox.api.ReportProto.ScreenshotListReply;
import no.nb.nna.broprox.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.broprox.commons.auth.AllowedRoles;
import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
