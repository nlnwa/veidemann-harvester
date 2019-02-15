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

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.OpenIdConnectIssuerReply;
import no.nb.nna.veidemann.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleList;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.RunCrawlReply;
import no.nb.nna.veidemann.api.ControllerProto.RunCrawlRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.CrawlScopes;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.crawlSeed;
import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;

/**
 *
 */
public class ControllerService extends ControllerGrpc.ControllerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerService.class);

    private final ConfigAdapter db;

    private final Settings settings;

    public ControllerService(Settings settings) {
        this.settings = settings;
        this.db = DbService.getInstance().getConfigAdapter();
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getCrawlEntity(GetRequest request, StreamObserver<CrawlEntity> responseObserver) {
        handleGet(() -> db.getCrawlEntity(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveEntity(CrawlEntity request, StreamObserver<CrawlEntity> responseObserver) {
        try {
            responseObserver.onNext(db.saveCrawlEntity(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listCrawlEntities(ListRequest request, StreamObserver<CrawlEntityListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlEntities(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void deleteEntity(CrawlEntity request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteCrawlEntity(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getCrawlScheduleConfig(GetRequest request, StreamObserver<CrawlScheduleConfig> responseObserver) {
        handleGet(() -> db.getCrawlScheduleConfig(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteCrawlScheduleConfig(CrawlScheduleConfig request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteCrawlScheduleConfig(request));
            responseObserver.onCompleted();

        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveCrawlScheduleConfig(CrawlScheduleConfig request, StreamObserver<CrawlScheduleConfig> responseObserver) {
        try {
            responseObserver.onNext(db.saveCrawlScheduleConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listCrawlScheduleConfigs(ListRequest request,
                                         StreamObserver<CrawlScheduleConfigListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlScheduleConfigs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getCrawlConfig(GetRequest request, StreamObserver<CrawlConfig> responseObserver) {
        handleGet(() -> db.getCrawlConfig(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteCrawlConfig(CrawlConfig request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteCrawlConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveCrawlConfig(CrawlConfig request, StreamObserver<CrawlConfig> responseObserver) {
        try {
            responseObserver.onNext(db.saveCrawlConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listCrawlConfigs(ListRequest request, StreamObserver<CrawlConfigListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlConfigs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void getCrawlJob(GetRequest request, StreamObserver<CrawlJob> responseObserver) {
        handleGet(() -> db.getCrawlJob(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteCrawlJob(CrawlJob request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteCrawlJob(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveCrawlJob(CrawlJob request, StreamObserver<CrawlJob> responseObserver) {
        try {
            responseObserver.onNext(db.saveCrawlJob(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void listCrawlJobs(ListRequest request, StreamObserver<CrawlJobListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlJobs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getSeed(GetRequest request, StreamObserver<Seed> responseObserver) {
        handleGet(() -> db.getSeed(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void deleteSeed(Seed request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteSeed(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveSeed(Seed request, StreamObserver<Seed> responseObserver) {
        try {
            // If scope is not set, apply default scope
            if (request.getScope().getSurtPrefix().isEmpty()) {
                String scope = CrawlScopes.generateDomainScope(request.getMeta().getName());
                request = request.toBuilder().setScope(request.getScope().toBuilder().setSurtPrefix(scope)).build();
            }

            responseObserver.onNext(db.saveSeed(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listSeeds(SeedListRequest request, StreamObserver<SeedListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listSeeds(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getBrowserConfig(GetRequest request, StreamObserver<BrowserConfig> responseObserver) {
        handleGet(() -> db.getBrowserConfig(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteBrowserConfig(BrowserConfig request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteBrowserConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveBrowserConfig(BrowserConfig request,
                                  StreamObserver<BrowserConfig> responseObserver) {
        try {
            responseObserver.onNext(db.saveBrowserConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listBrowserConfigs(ListRequest request,
                                   StreamObserver<BrowserConfigListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listBrowserConfigs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getPolitenessConfig(GetRequest request, StreamObserver<PolitenessConfig> responseObserver) {
        handleGet(() -> db.getPolitenessConfig(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deletePolitenessConfig(PolitenessConfig request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deletePolitenessConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void savePolitenessConfig(PolitenessConfig request, StreamObserver<PolitenessConfig> responseObserver) {
        try {
            responseObserver.onNext(db.savePolitenessConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listPolitenessConfigs(ListRequest request, StreamObserver<PolitenessConfigListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listPolitenessConfigs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getBrowserScript(GetRequest request, StreamObserver<BrowserScript> responseObserver) {
        handleGet(() -> db.getBrowserScript(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveBrowserScript(BrowserScript request, StreamObserver<BrowserScript> responseObserver) {
        try {
            responseObserver.onNext(db.saveBrowserScript(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listBrowserScripts(ListRequest request,
                                   StreamObserver<BrowserScriptListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listBrowserScripts(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteBrowserScript(BrowserScript request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteBrowserScript(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getCrawlHostGroupConfig(GetRequest request, StreamObserver<CrawlHostGroupConfig> responseObserver) {
        handleGet(() -> db.getCrawlHostGroupConfig(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveCrawlHostGroupConfig(CrawlHostGroupConfig request, StreamObserver<CrawlHostGroupConfig> responseObserver) {
        try {
            responseObserver.onNext(db.saveCrawlHostGroupConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listCrawlHostGroupConfigs(ListRequest request,
                                          StreamObserver<CrawlHostGroupConfigListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listCrawlHostGroupConfigs(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void deleteCrawlHostGroupConfig(CrawlHostGroupConfig request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteCrawlHostGroupConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveLogConfig(LogLevels request, StreamObserver<LogLevels> responseObserver) {
        try {
            responseObserver.onNext(db.saveLogConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getLogConfig(Empty request, StreamObserver<LogLevels> responseObserver) {
        try {
            responseObserver.onNext(db.getLogConfig());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void runCrawl(RunCrawlRequest request, StreamObserver<RunCrawlReply> responseObserver) {
        try {
            ConfigRef jobRequest = ConfigRef.newBuilder()
                    .setKind(Kind.crawlJob)
                    .setId(request.getJobId())
                    .build();

            ConfigObject job = db.getConfigObject(jobRequest);
            LOG.info("Job '{}' starting", job.getMeta().getName());

            JobExecutionStatus jobExecutionStatus = DbService.getInstance().getExecutionsAdapter()
                    .createJobExecutionStatus(job.getId());

            RunCrawlReply reply = RunCrawlReply.newBuilder().setJobExecutionId(jobExecutionStatus.getId()).build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            if (!request.getSeedId().isEmpty()) {
                ConfigObject seed = db.getConfigObject(ConfigRef.newBuilder()
                        .setKind(Kind.seed)
                        .setId(request.getSeedId())
                        .build());
                crawlSeed(job, seed, jobExecutionStatus);
            } else {
                no.nb.nna.veidemann.api.config.v1.ListRequest.Builder seedRequest = no.nb.nna.veidemann.api.config.v1.ListRequest.newBuilder()
                        .setKind(Kind.seed);
                seedRequest.getQueryMaskBuilder().addPaths(Kind.seed.name() + ".jobRef");
                seedRequest.getQueryTemplateBuilder().getSeedBuilder().addJobRefBuilder().setKind(Kind.crawlJob).setId(job.getId());

                try (ChangeFeed<ConfigObject> seeds = db.listConfigObjects(seedRequest.build())) {
                    seeds.stream().forEach(s -> crawlSeed(job, s, jobExecutionStatus));
                }
            }
            LOG.info("All seeds for job '{}' started", job.getMeta().getName());
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void listRoleMappings(RoleMappingsListRequest request, StreamObserver<RoleMappingsListReply> responseObserver) {
        try {
            responseObserver.onNext(db.listRoleMappings(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveRoleMapping(RoleMapping request, StreamObserver<RoleMapping> responseObserver) {
        try {
            responseObserver.onNext(db.saveRoleMapping(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteRoleMapping(RoleMapping request, StreamObserver<Empty> responseObserver) {
        try {
            responseObserver.onNext(db.deleteRoleMapping(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public void getRolesForActiveUser(Empty request, StreamObserver<RoleList> responseObserver) {
        try {
            Collection<ConfigProto.Role> roles = RolesContextKey.roles()
                    .stream()
                    .map(r -> ConfigProto.Role.valueOf(r.name()))
                    .collect(Collectors.toSet());
            if (roles == null) {
                responseObserver.onNext(RoleList.newBuilder().build());
            } else {
                responseObserver.onNext(RoleList.newBuilder().addAllRole(roles).build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public void getOpenIdConnectIssuer(Empty request, StreamObserver<OpenIdConnectIssuerReply> responseObserver) {
        try {
            LOG.debug("OpenIdConnectIssuer requested. Returning '{}'", settings.getOpenIdConnectIssuer());
            responseObserver.onNext(OpenIdConnectIssuerReply.newBuilder()
                    .setOpenIdConnectIssuer(settings.getOpenIdConnectIssuer()).build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

}
