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
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.StatusGrpc;
import no.nb.nna.veidemann.api.StatusProto.ExecutionId;
import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsRequest;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;

/**
 *
 */
public class StatusService extends StatusGrpc.StatusImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(StatusService.class);

    private final ExecutionsAdapter db;

    public StatusService() {
        this.db = DbService.getInstance().getExecutionsAdapter();
    }

    @Override
    @AllowedRoles({Role.ADMIN, Role.CURATOR, Role.READONLY})
    public void getRunningExecutions(RunningExecutionsRequest request, StreamObserver<RunningExecutionsListReply> responseObserver) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                // Initially send an empty state in case the change feed is empty
                responseObserver.onNext(RunningExecutionsListReply.getDefaultInstance());
                try (ChangeFeed<RunningExecutionsListReply> c = db.getCrawlExecutionStatusStream(request);) {
                    c.stream().forEach(o -> responseObserver.onNext(o));
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    responseObserver.onError(status.asException());
                }
            }

        }).start();
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getExecution(ExecutionId request, StreamObserver<CrawlExecutionStatus> responseObserver) {
        handleGet(() -> db.getCrawlExecutionStatus(request.getId()), responseObserver);
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listExecutions(ListExecutionsRequest request, StreamObserver<ExecutionsListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlExecutionStatus(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void abortExecution(ExecutionId request, StreamObserver<CrawlExecutionStatus> responseObserver) {
        try {
            CrawlExecutionStatus status = db.setCrawlExecutionStateAborted(request.getId());

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getJobExecution(ExecutionId request, StreamObserver<JobExecutionStatus> responseObserver) {
        handleGet(() -> db.getJobExecutionStatus(request.getId()), responseObserver);
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listJobExecutions(ListJobExecutionsRequest request, StreamObserver<JobExecutionsListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listJobExecutionStatus(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void abortJobExecution(ExecutionId request, StreamObserver<JobExecutionStatus> responseObserver) {
        try {
            JobExecutionStatus status = db.setJobExecutionStateAborted(request.getId());

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }
}
