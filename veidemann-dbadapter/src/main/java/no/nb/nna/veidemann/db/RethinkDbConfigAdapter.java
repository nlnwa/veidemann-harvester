package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Update;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.LogLevels;
import no.nb.nna.veidemann.api.ConfigProto.Meta;
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
import no.nb.nna.veidemann.api.config.v1.ConfigObject.SpecCase;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.LabelKeysResponse;
import no.nb.nna.veidemann.api.config.v1.ListCountResponse;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RethinkDbConfigAdapter implements ConfigAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RethinkDbConfigAdapter.class);

    static final RethinkDB r = RethinkDB.r;

    private final RethinkDbConnection conn;

    public RethinkDbConfigAdapter(RethinkDbConnection conn) {
        this.conn = conn;
    }

    @Override
    public ConfigObject getConfigObject(ConfigRef request) throws DbException {
        final Tables table = getTableForKind(request.getKind());

        Map<String, Object> response = conn.exec("db-getConfigObject",
                r.table(table.name)
                        .get(request.getId())
        );

        if (response == null) {
            return null;
        }

        return ProtoUtils.rethinkToProto(response, ConfigObject.class);
    }

    @Override
    public ChangeFeed<ConfigObject> listConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListConfigObjectQueryBuilder q = new ListConfigObjectQueryBuilder(request);

        Cursor<Map> res = conn.exec("db-listConfigObjects", q.getListQuery());

        return new ChangeFeedBase<ConfigObject>(res) {
            @Override
            protected Function<Map<String, Object>, ConfigObject> mapper() {
                return co -> {
                    ConfigObject res = ProtoUtils.rethinkToProto(co, ConfigObject.class);
                    return res;
                };
            }
        };
    }

    @Override
    public ListCountResponse countConfigObjects(no.nb.nna.veidemann.api.config.v1.ListRequest request) throws DbQueryException, DbConnectionException {
        ListConfigObjectQueryBuilder q = new ListConfigObjectQueryBuilder(request);
        long res = conn.exec("db-countConfigObjects", q.getCountQuery());
        return ListCountResponse.newBuilder().setCount(res).build();
    }

    @Override
    public ConfigObject saveConfigObject(ConfigObject object) throws DbException {
        object = ensureKindAndApiVersion(object);

        return storeConfigObject(object);
    }

    @Override
    public UpdateResponse updateConfigObjects(UpdateRequest request) throws DbQueryException, DbConnectionException {
        UpdateConfigObjectQueryBuilder q = new UpdateConfigObjectQueryBuilder(request);

        Map res = conn.exec("db-updateConfigObjects", q.getUpdateQuery());
        if ((long) res.get("inserted") != 0 || (long) res.get("errors") != 0 || (long) res.get("deleted") != 0) {
            throw new DbQueryException("Only replaced or unchanged expected from an update query. Got: " + res);
        }
        return UpdateResponse.newBuilder().setUpdated((long) res.get("replaced")).build();
    }

    @Override
    public DeleteResponse deleteConfigObject(ConfigObject object) throws DbException {
        final Tables table = getTableForKind(object.getKind());

        switch (object.getKind()) {
            case browserScript:
                checkDependencies(object, Kind.browserConfig, "browserConfig.scriptRef");
                break;
            case crawlEntity:
                checkDependencies(object, Kind.seed, "seed.entityRef");
                break;
            case crawlJob:
                checkDependencies(object, Kind.seed, "seed.jobRef");
                break;
            case crawlScheduleConfig:
                checkDependencies(object, Kind.crawlJob, "crawlJob.scheduleRef");
                break;
            case politenessConfig:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.politenessRef");
                break;
            case browserConfig:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.browserConfigRef");
                break;
            case crawlConfig:
                checkDependencies(object, Kind.crawlJob, "crawlJob.crawlConfigRef");
            case collection:
                checkDependencies(object, Kind.crawlConfig, "crawlConfig.collectionRef");
                break;
        }

        Map<String, Object> response = conn.exec("db-deleteConfigObject",
                r.table(table.name)
                        .get(object.getId())
                        .delete()
        );
        return DeleteResponse.newBuilder().setDeleted((long) response.get("deleted") == 1).build();
    }

    @Override
    public LabelKeysResponse getLabelKeys(GetLabelKeysRequest request) throws DbQueryException, DbConnectionException {
        Tables table = getTableForKind(request.getKind());

        try (Cursor<String> res = conn.exec("db-getLabelKeys",
                r.table(table.name)
                        .distinct().optArg("index", "kind_label_key")
                        .filter(k1 -> k1.nth(0).eq(request.getKind().name()))
                        .map(k2 -> k2.nth(1))
        )) {
            return LabelKeysResponse.newBuilder().addAllKey(res).build();
        }
    }

    public static ConfigObject ensureKindAndApiVersion(ConfigObject co) {
        if (co.getApiVersion().isEmpty()) {
            throw new IllegalArgumentException("apiVersion can not be empty");
        }

        if (co.getKind() == Kind.undefined) {
            if (co.getSpecCase() == SpecCase.SPEC_NOT_SET) {
                throw new IllegalArgumentException("Missing kind");
            } else {
                // Set kind from spec
                co = co.toBuilder().setKind(Kind.forNumber(co.getSpecCase().getNumber())).build();
            }
        }

        if (co.getSpecCase() != SpecCase.SPEC_NOT_SET && co.getKind().getNumber() != co.getSpecCase().getNumber()) {
            throw new IllegalArgumentException("Mismatch between kind and spec: " + co.getKind() + " != " + co.getSpecCase());
        }
        return co;
    }

    private Meta metaFromV1(no.nb.nna.veidemann.api.config.v1.Meta meta) {
        Meta.Builder m = Meta.newBuilder()
                .setName(meta.getName())
                .setDescription(meta.getDescription());
        meta.getLabelList().forEach(l -> m.addLabelBuilder().setKey(l.getKey()).setValue(l.getValue()));
        return m.build();
    }

    @Override
    public BrowserScript getBrowserScript(GetRequest req) throws DbException {
        return getMessage(req, BrowserScript.class, Kind.browserScript);
    }

    @Override
    public BrowserScript saveBrowserScript(BrowserScript script) throws DbException {
        return saveMessage(script, Kind.browserScript);
    }

    @Override
    public Empty deleteBrowserScript(BrowserScript script) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.browserScript)
                .setId(script.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public BrowserScriptListReply listBrowserScripts(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.browserScript);
        return queryBuilder.executeList(conn, BrowserScriptListReply.newBuilder()).build();
    }

    @Override
    public CrawlHostGroupConfig getCrawlHostGroupConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlHostGroupConfig.class, Kind.crawlHostGroupConfig);
    }

    @Override
    public CrawlHostGroupConfig saveCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        return saveMessage(crawlHostGroupConfig, Kind.crawlHostGroupConfig);
    }

    @Override
    public Empty deleteCrawlHostGroupConfig(CrawlHostGroupConfig crawlHostGroupConfig) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlHostGroupConfig)
                .setId(crawlHostGroupConfig.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public CrawlHostGroupConfigListReply listCrawlHostGroupConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.crawlHostGroupConfig);
        return queryBuilder.executeList(conn, CrawlHostGroupConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlEntity getCrawlEntity(GetRequest req) throws DbException {
        return getMessage(req, CrawlEntity.class, Kind.crawlEntity);
    }

    @Override
    public CrawlEntity saveCrawlEntity(CrawlEntity entity) throws DbException {
        return saveMessage(entity, Kind.crawlEntity);
    }

    @Override
    public Empty deleteCrawlEntity(CrawlEntity entity) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlEntity)
                .setId(entity.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public CrawlEntityListReply listCrawlEntities(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.crawlEntity);
        return queryBuilder.executeList(conn, CrawlEntityListReply.newBuilder()).build();
    }

    @Override
    public Seed getSeed(GetRequest req) throws DbException {
        return getMessage(req, Seed.class, Kind.seed);
    }

    @Override
    public SeedListReply listSeeds(SeedListRequest request) throws DbException {
        SeedListRequestQueryBuilder queryBuilder = new SeedListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public Seed saveSeed(Seed seed) throws DbException {
        return saveMessage(seed, Kind.seed);
    }

    @Override
    public Empty deleteSeed(Seed seed) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.seed)
                .setId(seed.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public CrawlJob getCrawlJob(GetRequest req) throws DbException {
        return getMessage(req, CrawlJob.class, Kind.crawlJob);
    }

    @Override
    public CrawlJobListReply listCrawlJobs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.crawlJob);
        return queryBuilder.executeList(conn, CrawlJobListReply.newBuilder()).build();
    }

    @Override
    public CrawlJob saveCrawlJob(CrawlJob crawlJob) throws DbException {
        if (crawlJob.getCrawlConfigId().isEmpty()) {
            throw new IllegalArgumentException("A crawl config is required for crawl jobs");
        }

        return saveMessage(crawlJob, Kind.crawlJob);
    }

    @Override
    public Empty deleteCrawlJob(CrawlJob crawlJob) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlJob)
                .setId(crawlJob.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public CrawlConfig getCrawlConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlConfig.class, Kind.crawlConfig);
    }

    @Override
    public CrawlConfigListReply listCrawlConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.crawlConfig);
        return queryBuilder.executeList(conn, CrawlConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlConfig saveCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        return saveMessage(crawlConfig, Kind.crawlConfig);
    }

    @Override
    public Empty deleteCrawlConfig(CrawlConfig crawlConfig) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlConfig)
                .setId(crawlConfig.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public CrawlScheduleConfig getCrawlScheduleConfig(GetRequest req) throws DbException {
        return getMessage(req, CrawlScheduleConfig.class, Kind.crawlScheduleConfig);
    }

    @Override
    public CrawlScheduleConfigListReply listCrawlScheduleConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.crawlScheduleConfig);
        return queryBuilder.executeList(conn, CrawlScheduleConfigListReply.newBuilder()).build();
    }

    @Override
    public CrawlScheduleConfig saveCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        return saveMessage(crawlScheduleConfig, Kind.crawlScheduleConfig);
    }

    @Override
    public Empty deleteCrawlScheduleConfig(CrawlScheduleConfig crawlScheduleConfig) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlScheduleConfig)
                .setId(crawlScheduleConfig.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public PolitenessConfig getPolitenessConfig(GetRequest req) throws DbException {
        return getMessage(req, PolitenessConfig.class, Kind.politenessConfig);
    }

    @Override
    public PolitenessConfigListReply listPolitenessConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.politenessConfig);
        return queryBuilder.executeList(conn, PolitenessConfigListReply.newBuilder()).build();
    }

    @Override
    public PolitenessConfig savePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        return saveMessage(politenessConfig, Kind.politenessConfig);
    }

    @Override
    public Empty deletePolitenessConfig(PolitenessConfig politenessConfig) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.politenessConfig)
                .setId(politenessConfig.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public BrowserConfig getBrowserConfig(GetRequest req) throws DbException {
        return getMessage(req, BrowserConfig.class, Kind.browserConfig);
    }

    @Override
    public BrowserConfigListReply listBrowserConfigs(ListRequest request) throws DbException {
        ListRequestQueryBuilder queryBuilder = new ListRequestQueryBuilder(request, Kind.browserConfig);
        return queryBuilder.executeList(conn, BrowserConfigListReply.newBuilder()).build();
    }

    @Override
    public BrowserConfig saveBrowserConfig(BrowserConfig browserConfig) throws DbException {
        return saveMessage(browserConfig, Kind.browserConfig);
    }

    @Override
    public Empty deleteBrowserConfig(BrowserConfig browserConfig) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.browserConfig)
                .setId(browserConfig.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public RoleMappingsListReply listRoleMappings(RoleMappingsListRequest request) throws DbException {
        RoleMappingsListRequestQueryBuilder queryBuilder = new RoleMappingsListRequestQueryBuilder(request);
        return queryBuilder.executeList(conn).build();
    }

    @Override
    public RoleMapping saveRoleMapping(RoleMapping roleMapping) throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.roleMapping)
                .setId(roleMapping.getId());
        co.getMetaBuilder().setName("Role mapping");
        switch (roleMapping.getEmailOrGroupCase()) {
            case EMAIL:
                co.getRoleMappingBuilder().setEmail(roleMapping.getEmail());
                break;
            case GROUP:
                co.getRoleMappingBuilder().setGroup(roleMapping.getGroup());
                break;
        }
        roleMapping.getRoleList().forEach(r -> co.getRoleMappingBuilder().addRole(Role.valueOf(r.name())));

        ConfigObject res = storeConfigObject(co.build());
        return roleMapping.toBuilder().setId(res.getId()).build();
    }

    @Override
    public Empty deleteRoleMapping(RoleMapping roleMapping) throws DbException {
        deleteConfigObject(ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.roleMapping)
                .setId(roleMapping.getId())
                .build());
        return Empty.getDefaultInstance();
    }

    @Override
    public LogLevels getLogConfig() throws DbException {
        Map<String, Object> response = conn.exec("get-logconfig",
                r.table(Tables.SYSTEM.name)
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
                r.table(Tables.SYSTEM.name)
                        .insert(doc)
                        .optArg("conflict", "replace"),
                LogLevels.class
        );
    }

    public <T extends Message> T getMessage(GetRequest req, Class<T> type, Kind kind) throws DbException {
        final Tables table = getTableForKind(kind);

        Map<String, Object> response = conn.exec("db-get" + type.getSimpleName(),
                r.table(table.name)
                        .get(req.getId())
        );

        if (response == null) {
            return null;
        }

        response = convertV1ToOldApi(response);

        return ProtoUtils.rethinkToProto(response, type);
    }

    public static Map<String, Object> convertOldToV1Api(Kind kind, Map<String, Object> old) {
        String[] fieldNames = old.keySet().toArray(new String[0]);
        MapObject spec = r.hashMap();
        for (String key : fieldNames) {
            switch (key) {
                case "id":
                case "meta":
                case "kind":
                case "apiVersion":
                    break;
                case "sleepAfterPageloadMs":
                    spec.put("maxInactivityTimeMs", old.remove(key));
                    break;
                case "entityId":
                    spec.put("entityRef", r.hashMap("kind", Kind.crawlEntity.name()).with("id", old.remove(key)));
                    break;
                case "jobId":
                    refToId(spec, old, "job", Kind.crawlJob);
                    break;
                case "scheduleId":
                    refToId(spec, old, "schedule", Kind.crawlScheduleConfig);
                    break;
                case "crawlConfigId":
                    refToId(spec, old, "crawlConfig", Kind.crawlConfig);
                    break;
                case "collectionId":
                    refToId(spec, old, "collection", Kind.collection);
                    break;
                case "browserConfigId":
                    refToId(spec, old, "browserConfig", Kind.browserConfig);
                    break;
                case "politenessId":
                    refToId(spec, old, "politeness", Kind.politenessConfig);
                    break;
                case "scriptId":
                    refToId(spec, old, "script", Kind.browserScript);
                    break;
                case "extra":
                    Map extra = (Map) old.remove(key);
                    if (extra.containsKey("createSnapshot")) {
                        extra.put("createScreenshot", extra.remove("createSnapshot"));
                        spec.put(key, extra);
                    }
                    break;
                default:
                    spec.put(key, old.remove(key));
                    break;
            }
        }
        old.put("apiVersion", "v1");
        old.put("kind", kind.name());
        old.put(kind.name(), spec);
        return old;
    }

    private static void refToId(MapObject spec, Map<String, Object> old, String name, Kind kind) {
        Object oldVal = old.remove(name + "Id");
        if (oldVal instanceof List) {
            List refs = r.array();
            for (String id : (List<String>) oldVal) {
                refs.add(r.hashMap("kind", kind.name()).with("id", id));
            }
            spec.put(name + "Ref", refs);
        } else {
            spec.put(name + "Ref", r.hashMap("kind", kind.name()).with("id", oldVal));
        }
    }

    public static Map<String, Object> convertV1ToOldApi(Map<String, Object> v1) {
        Kind kind = Kind.valueOf((String) v1.get("kind"));
        if (v1.containsKey(kind.name())) {
            v1.putAll((Map) v1.remove(kind.name()));
        }

        switch (kind) {
            case seed:
                idToRef(v1, "entity");
                idToRef(v1, "job");
                break;
            case crawlJob:
                idToRef(v1, "schedule");
                idToRef(v1, "crawlConfig");
                break;
            case crawlConfig:
                idToRef(v1, "collection");
                idToRef(v1, "browserConfig");
                idToRef(v1, "politeness");
                if (v1.containsKey("extra") && ((Map) v1.get("extra")).containsKey("createScreenshot")) {
                    ((Map) v1.get("extra")).put("createSnapshot", ((Map) v1.get("extra")).remove("createScreenshot"));
                }
                break;
            case browserConfig:
                idToRef(v1, "script");
                if (v1.containsKey("maxInactivityTimeMs")) {
                    v1.put("sleepAfterPageloadMs", v1.remove("maxInactivityTimeMs"));
                }
                break;
        }
        v1.remove("apiVersion");
        v1.remove("kind");
        return v1;
    }

    private static void idToRef(Map<String, Object> v1, String name) {
        if (v1.containsKey(name + "Ref")) {
            Object oldRef = v1.remove(name + "Ref");
            if (oldRef instanceof List) {
                List refs = r.array();
                for (Map item : (List<Map>) oldRef) {
                    refs.add(item.get("id"));
                }
                v1.put(name + "Id", refs);
            } else {
                v1.put(name + "Id", ((Map) oldRef).get("id"));
            }
        }
    }

    public <T extends Message> T saveMessage(T msg, Kind kind) throws DbException {
        final Tables table = getTableForKind(kind);

        if (msg.getDescriptorForType().findFieldByName("meta") == null) {
            throw new IllegalArgumentException("Message must be a config message");
        }
        Map<String, Object> rMap = ProtoUtils.protoToRethink(msg);

        rMap = convertOldToV1Api(kind, rMap);

        // Check that name is set if this is a new object
        if (!rMap.containsKey("id") && (!rMap.containsKey("meta") || !((Map) rMap.get("meta")).containsKey("name"))) {
            throw new IllegalArgumentException("Trying to store a new " + msg.getClass().getSimpleName()
                    + " object, but meta.name is not set.");
        }

        rMap.put("meta", updateMeta((Map) rMap.get("meta")));

        Map<String, Object> response = executeInsertOrUpdate("db-save" + msg.getClass().getSimpleName(),
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
                                )))
        );

        response = convertV1ToOldApi(response);

        return ProtoUtils.rethinkToProto(response, (Class<T>) msg.getClass());
    }

    private Map<String, Object> executeInsertOrUpdate(String operationName, ReqlExpr qry) throws DbException {
        if (qry instanceof Insert) {
            qry = ((Insert) qry).optArg("return_changes", "always");
        } else if (qry instanceof Update) {
            qry = ((Update) qry).optArg("return_changes", "always");
        }

        Map<String, Object> response = conn.exec(operationName, qry);
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        Map newDoc = changes.get(0).get("new_val");
        return newDoc;
    }

    static Tables getTableForKind(Kind kind) {
        switch (kind) {
            case undefined:
                throw new IllegalArgumentException("Missing kind");
            case seed:
                return Tables.SEEDS;
            case crawlEntity:
                return Tables.CRAWL_ENTITIES;
            default:
                return Tables.CONFIG;
        }
    }

    private ConfigObject storeConfigObject(ConfigObject msg) throws DbException {
        final Tables table = getTableForKind(msg.getKind());

        FieldDescriptor metaField = msg.getDescriptorForType().findFieldByName("meta");
        Map rMap = ProtoUtils.protoToRethink(msg);

        if (metaField == null) {
            throw new IllegalArgumentException("Missing meta");
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
                    ConfigObject.class
            );
        }
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
        meta.put("lastModifiedBy", user);

        return meta;
    }

    /**
     * Check references to Config object.
     *
     * @param messageToCheck     the config message which other objects might refer.
     * @param dependentKind      the config message kind which might have a dependency to the object to check.
     * @param dependentFieldName the field name in the dependent message which might contain reference to the id field
     *                           in the object to check.
     * @throws IllegalStateException if there are dependencies.
     */
    private void checkDependencies(ConfigObject messageToCheck, Kind dependentKind,
                                   String dependentFieldName) throws DbException {

        no.nb.nna.veidemann.api.config.v1.ListRequest.Builder request = no.nb.nna.veidemann.api.config.v1.ListRequest.newBuilder()
                .setKind(dependentKind);
        FieldMasks.setValue(dependentFieldName, request.getQueryTemplateBuilder(),
                ConfigRef.newBuilder().setKind(messageToCheck.getKind()).setId(messageToCheck.getId()).build()
        );
        request.getQueryMaskBuilder().addPaths(dependentFieldName);

        long dependencyCount = conn.exec("db-checkDependency",
                new ListConfigObjectQueryBuilder(request.build()).getCountQuery());

        if (dependencyCount > 0) {
            throw new DbQueryException("Can't delete " + messageToCheck.getKind()
                    + ", there are " + dependencyCount + " " + dependentKind
                    + "(s) referring it");
        }
    }

}
