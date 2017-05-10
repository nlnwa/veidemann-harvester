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

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.api.ControllerGrpc;
import no.nb.nna.broprox.api.ControllerProto;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.model.ConfigProto;
import org.netpreserve.commons.uri.UriConfigs;
import org.netpreserve.commons.uri.UriFormat;

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
        try {
            respObserver.onNext(db.saveCrawlEntity(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listCrawlEntities(ListRequest request, StreamObserver<CrawlEntityListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlEntities(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteEntity(CrawlEntity request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlEntity(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteCrawlScheduleConfig(ConfigProto.CrawlScheduleConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlScheduleConfig(request));
            respObserver.onCompleted();

        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveCrawlScheduleConfig(ConfigProto.CrawlScheduleConfig request,
            StreamObserver<ConfigProto.CrawlScheduleConfig> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlScheduleConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listCrawlScheduleConfigs(ListRequest request,
            StreamObserver<ControllerProto.CrawlScheduleConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlScheduleConfigs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteCrawlConfig(ConfigProto.CrawlConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveCrawlConfig(ConfigProto.CrawlConfig request, StreamObserver<ConfigProto.CrawlConfig> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listCrawlConfigs(ListRequest request,
            StreamObserver<ControllerProto.CrawlConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlConfigs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteCrawlJob(ConfigProto.CrawlJob request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlJob(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveCrawlJob(ConfigProto.CrawlJob request, StreamObserver<ConfigProto.CrawlJob> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlJob(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listCrawlJobs(CrawlJobListRequest request, StreamObserver<ControllerProto.CrawlJobListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlJobs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteSeed(ConfigProto.Seed request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteSeed(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveSeed(ConfigProto.Seed request, StreamObserver<ConfigProto.Seed> respObserver) {
        try {
            // If scope is not set, apply default scope
            if (request.getScope().getSurtPrefix().isEmpty()) {
                UriFormat f = UriConfigs.SURT_KEY_FORMAT.ignorePort(true).ignorePath(true).ignoreQuery(true);
                String scope = UriConfigs.SURT_KEY.buildUri(request.getUri()).toCustomString(f);
                scope = scope.substring(0, scope.length() - 1);
                request = request.toBuilder().setScope(request.getScope().toBuilder().setSurtPrefix(scope)).build();
            }

            respObserver.onNext(db.saveSeed(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listSeeds(SeedListRequest request, StreamObserver<ControllerProto.SeedListReply> respObserver) {
        try {
            respObserver.onNext(db.listSeeds(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deleteBrowserConfig(ConfigProto.BrowserConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteBrowserConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveBrowserConfig(ConfigProto.BrowserConfig request,
            StreamObserver<ConfigProto.BrowserConfig> respObserver) {
        try {
            respObserver.onNext(db.saveBrowserConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listBrowserConfigs(ListRequest request,
            StreamObserver<ControllerProto.BrowserConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listBrowserConfigs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void deletePolitenessConfig(ConfigProto.PolitenessConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deletePolitenessConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void savePolitenessConfig(ConfigProto.PolitenessConfig request,
            StreamObserver<ConfigProto.PolitenessConfig> respObserver) {
        try {
            respObserver.onNext(db.savePolitenessConfig(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listPolitenessConfigs(ListRequest request,
            StreamObserver<ControllerProto.PolitenessConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listPolitenessConfigs(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void saveBrowserScript(BrowserScript request, StreamObserver<BrowserScript> respObserver) {
        try {
            respObserver.onNext(db.saveBrowserScript(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

    @Override
    public void listBrowserScripts(BrowserScriptListRequest request, StreamObserver<BrowserScriptListReply> respObserver) {
        try {
            respObserver.onNext(db.listBrowserScripts(request));
            respObserver.onCompleted();
        } catch (Exception e) {
            respObserver.onError(e);
        }
    }

}
