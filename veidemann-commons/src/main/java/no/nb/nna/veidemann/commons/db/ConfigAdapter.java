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
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.LabelKeysResponse;
import no.nb.nna.veidemann.api.config.v1.ListCountResponse;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;

public interface ConfigAdapter {
    ConfigObject getConfigObject(ConfigRef request) throws DbException;

    ChangeFeed<ConfigObject> listConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbException;

    ListCountResponse countConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbException;

    ConfigObject saveConfigObject(ConfigObject object) throws DbException;

    UpdateResponse updateConfigObjects(UpdateRequest request) throws DbException;

    DeleteResponse deleteConfigObject(ConfigObject object) throws DbException;

    LabelKeysResponse getLabelKeys(GetLabelKeysRequest request) throws DbException;

    @Deprecated
    CrawlEntity getCrawlEntity(GetRequest req) throws DbException;

    @Deprecated
    CrawlEntity saveCrawlEntity(CrawlEntity msg) throws DbException;

    @Deprecated
    CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException;

    @Deprecated
    Empty deleteCrawlEntity(CrawlEntity entity) throws DbException;

    @Deprecated
    Seed getSeed(GetRequest req) throws DbException;

    @Deprecated
    SeedListReply listSeeds(SeedListRequest request) throws DbException;

    @Deprecated
    Seed saveSeed(Seed seed) throws DbException;

    @Deprecated
    Empty deleteSeed(Seed seed) throws DbException;

    @Deprecated
    CrawlJob getCrawlJob(GetRequest req) throws DbException;

    @Deprecated
    CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException;

    @Deprecated
    CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException;

    @Deprecated
    Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException;

    @Deprecated
    CrawlConfig getCrawlConfig(GetRequest req) throws DbException;

    @Deprecated
    CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException;

    @Deprecated
    CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    @Deprecated
    Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException;

    @Deprecated
    CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException;

    @Deprecated
    CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException;

    @Deprecated
    CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    @Deprecated
    Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException;

    @Deprecated
    PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException;

    @Deprecated
    PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException;

    @Deprecated
    PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    @Deprecated
    Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException;

    @Deprecated
    BrowserConfig getBrowserConfig(GetRequest req) throws DbException;

    @Deprecated
    BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException;

    @Deprecated
    BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException;

    @Deprecated
    Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException;

    @Deprecated
    BrowserScript getBrowserScript(GetRequest req) throws DbException;

    @Deprecated
    BrowserScript saveBrowserScript(BrowserScript script) throws DbException;

    @Deprecated
    Empty deleteBrowserScript(BrowserScript script) throws DbException;

    @Deprecated
    BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException;

    @Deprecated
    CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException;

    @Deprecated
    CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    @Deprecated
    Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException;

    @Deprecated
    CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException;

    LogLevels getLogConfig() throws DbException;

    LogLevels saveLogConfig(LogLevels logLevels) throws DbException;

    @Deprecated
    RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException;

    @Deprecated
    RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException;

    @Deprecated
    Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException;
}
