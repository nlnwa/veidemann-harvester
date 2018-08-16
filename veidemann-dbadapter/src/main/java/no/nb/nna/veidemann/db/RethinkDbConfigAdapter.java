package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
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
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RethinkDbConfigAdapter implements ConfigAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConfigAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbConfigAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public BrowserScript getBrowserScript(GetRequest req) throws DbException {
        return getMessage(req, BrowserScript.class, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public BrowserScript saveBrowserScript(BrowserScript script) throws DbException {
        return saveMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public Empty deleteBrowserScript(BrowserScript script) throws DbException {
        checkDependencies(script, TABLES.BROWSER_CONFIGS, BrowserConfig.getDefaultInstance(), "script_id");
        return deleteConfigMessage(script, TABLES.BROWSER_SCRIPTS);
    }

    @Override
    public BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.BROWSER_SCRIPTS);
        return queryBuilder.executeList(conn, BrowserScriptListReply.newBuilder()).build();
    }

    @Override
    public CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlHostGroupConfig.class, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        return saveMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        return deleteConfigMessage(crawlHostGroupConfig, TABLES.CRAWL_HOST_GROUP_CONFIGS);
    }

    @Override
    public CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_HOST_GROUP_CONFIGS);
        return queryBuilder.executeList(conn, CrawlHostGroupConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlEntity getCrawlEntity(GetRequest req) throws DbException {
        return getMessage(req, CrawlEntity.class, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) throws DbException {
        return saveMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public Empty deleteCrawlEntity(CrawlEntity entity) throws DbException {
        checkDependencies(entity, TABLES.SEEDS, Seed.getDefaultInstance(), "entity_id");
        return deleteConfigMessage(entity, TABLES.CRAWL_ENTITIES);
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_ENTITIES);
        return queryBuilder.executeList(conn, CrawlEntityListReply.newBuilder()).build();
    }

    @Override
    public Seed getSeed(GetRequest req) throws DbException {
        return getMessage(req, Seed.class, TABLES.SEEDS);
    }

    @Override
    public SeedListReply listSeeds(SeedListRequest request) throws DbException {
        SeedListRequestQueryBuilder queryBuilder = new SeedListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public Seed saveSeed(Seed seed) throws DbException {
        return saveMessage(seed, TABLES.SEEDS);
    }

    @Override
    public Empty deleteSeed(Seed seed) throws DbException {
        return deleteConfigMessage(seed, TABLES.SEEDS);
    }

    @Override
    public CrawlJob getCrawlJob(GetRequest req) throws DbException {
        return getMessage(req, CrawlJob.class, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_JOBS);
        return queryBuilder.executeList(conn, CrawlJobListReply.newBuilder()).build();
    }

    @Override
    public CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException {
        if (crawlJob.getCrawlConfigId().isEmpty()) {
            throw new IllegalArgumentException("A crawl config is required for crawl jobs");
        }

        return saveMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException {
        checkDependencies(crawlJob, TABLES.SEEDS, Seed.getDefaultInstance(), "job_id");
        return deleteConfigMessage(crawlJob, TABLES.CRAWL_JOBS);
    }

    @Override
    public CrawlConfig getCrawlConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlConfig.class, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_CONFIGS);
        return queryBuilder.executeList(conn, CrawlConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        return saveMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        checkDependencies(crawlConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "crawl_config_id");
        return deleteConfigMessage(crawlConfig, TABLES.CRAWL_CONFIGS);
    }

    @Override
    public CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlScheduleConfig.class, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.CRAWL_SCHEDULE_CONFIGS);
        return queryBuilder.executeList(conn, CrawlScheduleConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        return saveMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        checkDependencies(crawlScheduleConfig, TABLES.CRAWL_JOBS, CrawlJob.getDefaultInstance(), "schedule_id");
        return deleteConfigMessage(crawlScheduleConfig, TABLES.CRAWL_SCHEDULE_CONFIGS);
    }

    @Override
    public PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException {
        return getMessage(req, PolitenessConfig.class, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.POLITENESS_CONFIGS);
        return queryBuilder.executeList(conn, PolitenessConfigListReply.newBuilder()).build();
    }

    @Override
    public PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        return saveMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        checkDependencies(politenessConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "politeness_id");
        return deleteConfigMessage(politenessConfig, TABLES.POLITENESS_CONFIGS);
    }

    @Override
    public BrowserConfig getBrowserConfig(GetRequest req) throws DbException {
        return getMessage(req, BrowserConfig.class, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, TABLES.BROWSER_CONFIGS);
        return queryBuilder.executeList(conn, BrowserConfigListReply.newBuilder()).build();
    }

    @Override
    public BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException {
        return saveMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException {
        checkDependencies(browserConfig, TABLES.CRAWL_CONFIGS, CrawlConfig.getDefaultInstance(), "browser_config_id");
        return deleteConfigMessage(browserConfig, TABLES.BROWSER_CONFIGS);
    }

    @Override
    public RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException {
        RoleMappingsListRequestQueryBuilder queryBuilder = new RoleMappingsListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException {
        Map<String, Object> doc = ProtoUtils.protoToRethink(roleMapping);
        return conn.executeInsert("save-rolemapping",
                r.table(TABLES.ROLE_MAPPINGS.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                RoleMapping.class
        );
    }

    @Override
    public Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException {
        return deleteConfigMessage(roleMapping, TABLES.ROLE_MAPPINGS);
    }

    @Override
    public LogLevels getLogConfig() throws DbException {
        Map<String, Object> response = conn.exec("get-logconfig",
                r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                        .get("log_levels")
                        .pluck("logLevel")
        );

        return ProtoUtils.rethinkToProto(response, LogLevels.class);
    }

    @Override
    public LogLevels saveLogConfig(LogLevels logLevels) throws DbException {
        Map<String, Object> doc = ProtoUtils.protoToRethink(logLevels);
        doc.put("id", "log_levels");
        return conn.executeInsert("save-logconfig",
                r.table(RethinkDbAdapter.TABLES.SYSTEM.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                LogLevels.class
        );
    }

    public <T extends Message> T getMessage(GetRequest req, Class<T> type, TABLES table) throws DbException {
        Map<String, Object> response = conn.exec("db-get" + type.getSimpleName(),
                r.table(table.name)
                        .get(req.getId())
        );

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, type);
    }

    public <T extends Message> T saveMessage(T msg, TABLES table) throws DbException {
        FieldDescriptor metaField = msg.getDescriptorForType().findFieldByName("meta");
        Map rMap = ProtoUtils.protoToRethink(msg);

        if (metaField == null) {
            return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table.name)
                            .insert(rMap)
                            .optArg("conflict", "replace"),
                    (Class<T>) msg.getClass()
            );
        } else {
            // Check that name is set if this is a new object
            if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
                throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                        + " object, but meta.name is not set.");
            }

            rMap.put("meta", updateMeta((Map) rMap.get("meta")));

            return conn.executeInsert("db-save" + msg.getClass().getSimpleName(),
                    r.table(table.name)
                            .insert(rMap)
                            // A rethink function which copies created and createby from old doc,
                            // and copies name if not existent in new doc
                            .optArg("conflict", (id, old_doc, new_doc) -> new_doc.merge(
                                    r.hashMap("meta", r.hashMap()
                                            .with("name", r.branch(new_doc.g("meta").hasFields("name"),
                                                    new_doc.g("meta").g("name"), old_doc.g("meta").g("name")))
                                            .with("created", old_doc.g("meta").g("created"))
                                            .with("createdBy", old_doc.g("meta").g("createdBy"))
                                    ))),
                    (Class<T>) msg.getClass()
            );
        }
    }

    public <T extends Message> Empty deleteConfigMessage(T entity, TABLES table) throws DbException {
        Descriptors.FieldDescriptor idDescriptor = entity.getDescriptorForType().findFieldByName("id");

        conn.exec("db-delete" + entity.getClass().getSimpleName(),
                r.table(table.name)
                        .get(entity.getField(idDescriptor))
                        .delete()
        );

        return Empty.getDefaultInstance();
    }

    private Map updateMeta(Map meta) {
        if (meta == null) {
            meta = r.hashMap();
        }

        String user = EmailContextKey.email();
        if (user == null || user.isEmpty()) {
            user = "anonymous";
        }

        if (!meta.containsKey("created")) {
            meta.put("created", r.now());
            meta.put("createdBy", user);
        }

        meta.put("lastModified", r.now());
        if (!meta.containsKey("lastModifiedBy")) {
            meta.put("lastModifiedBy", user);
        }

        return meta;
    }

    /**
     * Check references to Config object.
     *
     * @param messageToCheck     the config message which other objects might refer.
     * @param dependentTable     the table containing objects which might have a dependency to the object to check.
     * @param dependentMessage   the object type in the table containing objects which might have a dependency to the
     *                           object to check.
     * @param dependentFieldName the field name in the dependent message which might contain reference to the id field
     *                           in the object to check.
     * @throws IllegalStateException if there are dependencies.
     */
    private void checkDependencies(Message messageToCheck, TABLES dependentTable,
                                   Message dependentMessage, String dependentFieldName) throws DbException {

        Descriptors.FieldDescriptor messageIdField = messageToCheck.getDescriptorForType().findFieldByName("id");
        Descriptors.FieldDescriptor dependentField = dependentMessage.getDescriptorForType()
                .findFieldByName(dependentFieldName);

        ListRequestQueryBuilder qry = new ListRequestQueryBuilder(ListRequest.getDefaultInstance(), dependentTable);
        if (dependentField.isRepeated()) {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).contains(messageToCheck.getField(messageIdField)));
        } else {
            qry.addFilter(j -> j.g(dependentField.getJsonName()).eq(messageToCheck.getField(messageIdField)));
        }

        long dependencyCount = qry.executeCount(conn);
        if (dependencyCount > 0) {
            throw new IllegalStateException("Can't delete " + messageToCheck.getClass().getSimpleName()
                    + ", there are " + dependencyCount + " " + dependentMessage.getClass().getSimpleName()
                    + "(s) referring it");
        }
    }

}
