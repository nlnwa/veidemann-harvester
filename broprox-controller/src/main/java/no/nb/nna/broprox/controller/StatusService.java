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
import no.nb.nna.broprox.api.StatusGrpc;
import no.nb.nna.broprox.api.StatusProto.ExecutionsListReply;
import no.nb.nna.broprox.api.StatusProto.ExecutionsRequest;
import no.nb.nna.broprox.commons.ChangeFeed;
import no.nb.nna.broprox.commons.DbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StatusService extends StatusGrpc.StatusImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(StatusService.class);

    private final DbAdapter db;

    public StatusService(DbAdapter db) {
        this.db = db;
    }

    @Override
    public void getRunningExecutions(ExecutionsRequest request, StreamObserver<ExecutionsListReply> respObserver) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try (ChangeFeed<ExecutionsListReply> c = db.getExecutionStatusStream(request);) {
                    c.stream().forEach(o -> respObserver.onNext(o));
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                    respObserver.onError(status.asException());
                }
            }

        }).start();
    }

}
