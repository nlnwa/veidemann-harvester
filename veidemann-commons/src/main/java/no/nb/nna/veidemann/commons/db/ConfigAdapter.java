package no.nb.nna.veidemann.commons.db;

import com.google.protobuf.Empty;
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
import no.nb.nna.veidemann.api.ControllerProto.BrowserConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlHostGroupConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.veidemann.api.ControllerProto.CrawlScheduleConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.GetRequest;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.api.ControllerProto.PolitenessConfigListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListReply;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;

public interface ConfigAdapter {
    CrawlEntity getCrawlEntity(GetRequest req) throws DbException;

    CrawlEntity saveCrawlEntity(CrawlEntity msg) throws DbException;

    CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException;

    Empty deleteCrawlEntity(CrawlEntity entity) throws DbException;

    Seed getSeed(GetRequest req) throws DbException;

    SeedListReply listSeeds(SeedListRequest request) throws DbException;

    Seed saveSeed(Seed seed) throws DbException;

    Empty deleteSeed(Seed seed) throws DbException;

    CrawlJob getCrawlJob(GetRequest req) throws DbException;

    CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException;

    CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException;

    Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException;

    CrawlConfig getCrawlConfig(GetRequest req) throws DbException;

    CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException;

    CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException;

    CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException;

    CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException;

    PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException;

    PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    BrowserConfig getBrowserConfig(GetRequest req) throws DbException;

    BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException;

    BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException;

    Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException;

    BrowserScript getBrowserScript(GetRequest req) throws DbException;

    BrowserScript saveBrowserScript(BrowserScript script) throws DbException;

    Empty deleteBrowserScript(BrowserScript script) throws DbException;

    BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException;

    CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException;

    CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException;

    LogLevels getLogConfig() throws DbException;

    LogLevels saveLogConfig(LogLevels logLevels) throws DbException;

    RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException;

    RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException;

    Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException;
}
