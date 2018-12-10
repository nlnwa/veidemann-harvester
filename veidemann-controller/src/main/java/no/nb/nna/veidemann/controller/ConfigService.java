/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.GetRequest;
import no.nb.nna.veidemann.api.config.v1.LabelKeysResponse;
import no.nb.nna.veidemann.api.config.v1.ListCountResponse;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;

public class ConfigService extends ConfigGrpc.ConfigImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigService.class);

    private final ConfigAdapter db;

    private final Settings settings;

    public ConfigService(Settings settings) {
        this.settings = settings;
        this.db = DbService.getInstance().getConfigAdapter();
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getConfigObject(GetRequest request, StreamObserver<ConfigObject> responseObserver) {
        handleGet(() -> db.getConfigObject(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listConfigObjects(ListRequest request, StreamObserver<ConfigObject> responseObserver) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try (ChangeFeed<ConfigObject> c = db.listConfigObjects(request);) {
                    c.stream().forEach(o -> responseObserver.onNext(o));
                    responseObserver.onCompleted();
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
    public void countConfigObjects(ListRequest request, StreamObserver<ListCountResponse> responseObserver) {
        handleGet(() -> db.countConfigObjects(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveConfigObject(ConfigObject request, StreamObserver<ConfigObject> responseObserver) {
        try {
            responseObserver.onNext(db.saveConfigObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void updateConfigObjects(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        try {
            responseObserver.onNext(db.updateConfigObjects(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteConfigObject(ConfigObject request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            responseObserver.onNext(db.deleteConfigObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getLabelKeys(GetRequest request, StreamObserver<LabelKeysResponse> responseObserver) {
        handleGet(() -> db.getLabelKeys(request), responseObserver);
    }
}
