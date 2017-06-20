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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.api.ControllerGrpc;
import no.nb.nna.broprox.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.broprox.api.ControllerProto.RunCrawlReply;
import no.nb.nna.broprox.api.ControllerProto.RunCrawlRequest;
import no.nb.nna.broprox.api.ControllerProto.SeedListReply;
import no.nb.nna.broprox.api.ControllerProto.SeedListRequest;
import no.nb.nna.broprox.commons.util.CrawlScopes;
import no.nb.nna.broprox.controller.scheduler.FrontierClient;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.BrowserConfig;
import no.nb.nna.broprox.model.ConfigProto.BrowserScript;
import no.nb.nna.broprox.model.ConfigProto.CrawlConfig;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import no.nb.nna.broprox.model.ConfigProto.CrawlJob;
import no.nb.nna.broprox.model.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.broprox.model.ConfigProto.PolitenessConfig;
import no.nb.nna.broprox.model.ConfigProto.Seed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ControllerService extends ControllerGrpc.ControllerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerService.class);

    private final DbAdapter db;

    private final FrontierClient frontierClient;

    public ControllerService(DbAdapter db, FrontierClient frontierClient) {
        this.db = db;
        this.frontierClient = frontierClient;
    }

    @Override
    public void saveEntity(CrawlEntity request, StreamObserver<CrawlEntity> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlEntity(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listCrawlEntities(ListRequest request, StreamObserver<CrawlEntityListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlEntities(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteEntity(CrawlEntity request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlEntity(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteCrawlScheduleConfig(CrawlScheduleConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlScheduleConfig(request));
            respObserver.onCompleted();

        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveCrawlScheduleConfig(CrawlScheduleConfig request, StreamObserver<CrawlScheduleConfig> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlScheduleConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listCrawlScheduleConfigs(ListRequest request,
            StreamObserver<CrawlScheduleConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlScheduleConfigs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteCrawlConfig(CrawlConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveCrawlConfig(CrawlConfig request, StreamObserver<CrawlConfig> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listCrawlConfigs(ListRequest request, StreamObserver<CrawlConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlConfigs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteCrawlJob(CrawlJob request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlJob(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveCrawlJob(CrawlJob request, StreamObserver<CrawlJob> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlJob(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listCrawlJobs(CrawlJobListRequest request, StreamObserver<CrawlJobListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlJobs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteSeed(Seed request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteSeed(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveSeed(Seed request, StreamObserver<Seed> respObserver) {
        try {
            // If scope is not set, apply default scope
            if (request.getScope().getSurtPrefix().isEmpty()) {
                String scope = CrawlScopes.generateDomainScope(request.getMeta().getName());
                request = request.toBuilder().setScope(request.getScope().toBuilder().setSurtPrefix(scope)).build();
            }

            respObserver.onNext(db.saveSeed(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listSeeds(SeedListRequest request, StreamObserver<SeedListReply> respObserver) {
        try {
            respObserver.onNext(db.listSeeds(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteBrowserConfig(BrowserConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteBrowserConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveBrowserConfig(BrowserConfig request,
            StreamObserver<BrowserConfig> respObserver) {
        try {
            respObserver.onNext(db.saveBrowserConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listBrowserConfigs(ListRequest request,
            StreamObserver<BrowserConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listBrowserConfigs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deletePolitenessConfig(PolitenessConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deletePolitenessConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void savePolitenessConfig(PolitenessConfig request, StreamObserver<PolitenessConfig> respObserver) {
        try {
            respObserver.onNext(db.savePolitenessConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listPolitenessConfigs(ListRequest request, StreamObserver<PolitenessConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listPolitenessConfigs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void saveBrowserScript(BrowserScript request, StreamObserver<BrowserScript> respObserver) {
        try {
            respObserver.onNext(db.saveBrowserScript(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void listBrowserScripts(BrowserScriptListRequest request,
            StreamObserver<BrowserScriptListReply> respObserver) {
        try {
            respObserver.onNext(db.listBrowserScripts(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void deleteBrowserScript(BrowserScript request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteBrowserScript(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void runCrawl(RunCrawlRequest request, StreamObserver<RunCrawlReply> respObserver) {
        try {
            RunCrawlReply.Builder reply = RunCrawlReply.newBuilder();

            CrawlJobListRequest jobRequest = CrawlJobListRequest.newBuilder()
                    .setId(request.getJobId())
                    .setExpand(true)
                    .build();
            SeedListRequest seedRequest;

            for (CrawlJob job : db.listCrawlJobs(jobRequest).getValueList()) {
                LOG.info("Job '{}' starting", job.getMeta().getName());

                if (request.getSeedId().isEmpty()) {
                    seedRequest = SeedListRequest.newBuilder()
                            .setCrawlJobId(job.getId())
                            .build();
                } else {
                    seedRequest = SeedListRequest.newBuilder()
                            .setId(request.getSeedId())
                            .build();
                }

                for (Seed seed : db.listSeeds(seedRequest).getValueList()) {
                    if (!seed.getDisabled()) {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Start harvest of: {}", seed.getMeta().getName());
                            reply.addSeedExecutionId(frontierClient.crawlSeed(job, seed).getId());
                        }
                    }
                }
                LOG.info("All seeds for job '{}' started", job.getMeta().getName());
            }

            respObserver.onNext(reply.build());
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

}
