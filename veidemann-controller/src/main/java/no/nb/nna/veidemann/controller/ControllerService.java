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
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ControllerProto.AbortCrawlRequest;
import no.nb.nna.veidemann.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.veidemann.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.veidemann.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleList;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.RunCrawlReply;
import no.nb.nna.veidemann.api.ControllerProto.RunCrawlRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.util.CrawlScopes;
import no.nb.nna.veidemann.controller.scheduler.FrontierClient;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
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
    @AllowedRoles({Role.ADMIN})
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
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveCrawlHostGroupConfig(CrawlHostGroupConfig request, StreamObserver<CrawlHostGroupConfig> respObserver) {
        try {
            respObserver.onNext(db.saveCrawlHostGroupConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listCrawlHostGroupConfigs(ListRequest request,
            StreamObserver<CrawlHostGroupConfigListReply> respObserver) {
        try {
            respObserver.onNext(db.listCrawlHostGroupConfigs(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void deleteCrawlHostGroupConfig(CrawlHostGroupConfig request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteCrawlHostGroupConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void saveLogConfig(LogLevels request, StreamObserver<LogLevels> respObserver) {
        try {
            respObserver.onNext(db.saveLogConfig(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getLogConfig(Empty request, StreamObserver<LogLevels> respObserver) {
        try {
            respObserver.onNext(db.getLogConfig());
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
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

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void abortCrawl(AbortCrawlRequest request, StreamObserver<Empty> respObserver) {
        try {
            db.setExecutionStateAborted(request.getExecutionId());

            respObserver.onNext(Empty.getDefaultInstance());
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void listRoleMappings(RoleMappingsListRequest request, StreamObserver<RoleMappingsListReply> respObserver) {
        try {
            respObserver.onNext(db.listRoleMappings(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void saveRoleMapping(RoleMapping request, StreamObserver<RoleMapping> respObserver) {
        try {
            respObserver.onNext(db.saveRoleMapping(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteRoleMapping(RoleMapping request, StreamObserver<Empty> respObserver) {
        try {
            respObserver.onNext(db.deleteRoleMapping(request));
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

    @Override
    public void getRolesForActiveUser(Empty request, StreamObserver<RoleList> respObserver) {
        try {
            Collection<Role> roles = RolesContextKey.roles();
            if (roles == null) {
                respObserver.onNext(RoleList.newBuilder().build());
            } else {
                respObserver.onNext(RoleList.newBuilder().addAllRole(roles).build());
            }
            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }
}