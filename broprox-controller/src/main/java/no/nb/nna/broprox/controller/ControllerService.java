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

import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.api.ControllerGrpc;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.MessagesProto.CrawlEntity;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListRequest;

/**
 *
 */
public class ControllerService extends ControllerGrpc.ControllerImplBase {

    private final DbAdapter db;

    public ControllerService(DbAdapter db) {
        this.db = db;
    }

    @Override
    public void saveEntity(CrawlEntity request, StreamObserver<CrawlEntity> respObserver) {
        respObserver.onNext(db.saveCrawlEntity(request));
        respObserver.onCompleted();
    }

    @Override
    public void listCrawlEntities(CrawlEntityListRequest request, StreamObserver<CrawlEntityListReply> respObserver) {
        respObserver.onNext(db.listCrawlEntities(request));
        respObserver.onCompleted();
    }

    @Override
    public void saveBrowserScript(BrowserScript request, StreamObserver<BrowserScript> respObserver) {
        respObserver.onNext(db.saveBrowserScript(request));
        respObserver.onCompleted();
    }

    @Override
    public void listBrowserScripts(BrowserScriptListRequest request, StreamObserver<BrowserScriptListReply> respObserver) {
        BrowserScriptListReply.Builder builder = BrowserScriptListReply.newBuilder();
        db.getBrowserScripts(request.getType()).forEach(bs -> builder.addScript(bs));
        respObserver.onNext(builder.build());
        respObserver.onCompleted();
    }

}
