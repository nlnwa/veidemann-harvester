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
package no.nb.nna.veidemann.db;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ReportProto.Filter;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.commons.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public abstract class ConfigListQueryBuilder<T extends Message> {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigListQueryBuilder.class);

    private final T request;

    private ReqlExpr listQry;

    private int page;

    private int pageSize;

    private List<String> id;

    final Tables table;

    final Kind kind;

    private final String orderByIndex;

    private final boolean descending;

    public ConfigListQueryBuilder(T request, Kind kind, String orderByIndex, boolean descending) {
        this.request = Objects.requireNonNull(request, "The request cannot be null");
        this.table = RethinkDbConfigAdapter.getTableForKind(kind);
        this.kind = kind;
        this.orderByIndex = orderByIndex == null ? "" : orderByIndex;
        this.descending = descending;
    }

    public ConfigListQueryBuilder(T request, Tables table, String orderByIndex, boolean descending) {
        this.request = Objects.requireNonNull(request, "The request cannot be null");
        this.table = Objects.requireNonNull(table);
        this.kind = Kind.undefined;
        this.orderByIndex = orderByIndex == null ? "" : orderByIndex;
        this.descending = descending;
    }

    public ConfigListQueryBuilder(T request, Kind kind) {
        this(request, kind, "name", false);
    }

    public ConfigListQueryBuilder(T request, Tables table) {
        this(request, table, "name", false);
    }

    /**
     * Build a query for a list of objects.
     *
     * @param id the object id
     */
    void buildIdQuery(List<String> id) {
        this.id = id.stream().filter(i -> !i.isEmpty()).collect(Collectors.toList());
        listQry = addKindFilter(r.table(table.name).getAll(this.id.toArray()));
    }

    /**
     * Build a query for a single object.
     *
     * @param id the object id
     */
    void buildIdQuery(String id) {
        if (id != null && !id.isEmpty()) {
            this.id = Collections.singletonList(id);
        }
        listQry = addKindFilter(r.table(table.name).getAll(this.id.toArray()));
    }

    /**
     * Build a query for a name request.
     * <p>
     * The name parameter accepts regular expressions.
     *
     * @param name the name regex
     */
    void buildNameQuery(String name) {
        if (name.isEmpty()) {
            return;
        }

        final String qry = "(?i)" + name;

        LOG.debug("Adding name query: {qry={}}", qry);

        if ("name".equals(orderByIndex)) {
            addQuery(r.table(table.name)
                    .orderBy().optArg("index", "name")
                    .filter(doc -> doc.g("meta").g("name").match(qry)));
        } else {
            addQuery(r.table(table.name)
                    .filter(doc -> doc.g("meta").g("name").match(qry)));
        }
    }

    /**
     * Build a query for a label selector.
     *
     * @param selector
     */
    void buildSelectorQuery(List<String> selector) {
        selector.forEach(q -> {
            String key;
            String value;

            int sepIdx = q.indexOf(':');
            if (sepIdx == -1) {
                key = "";
                value = q.toLowerCase();
            } else {
                key = q.substring(0, sepIdx).toLowerCase();
                value = q.substring(sepIdx + 1).toLowerCase();
            }

            LOG.debug("Adding selector: {key={}, value={}}", key, value);

            if (!key.isEmpty() && !value.isEmpty() && !value.endsWith("*")) {
                // Exact match
                List indexKey = r.array(key, value);
                addQuery(r.table(table.name).between(indexKey, indexKey).optArg("right_bound", "closed").optArg("index", "label"));
            } else if (!key.isEmpty()) {
                // Exact match on key, value ends with '*' or is empty

                List startSpan = r.array(key);
                List endSpan = r.array(key);

                if (value.endsWith("*")) {
                    String prefix = value.substring(0, value.length() - 1);
                    startSpan.add(prefix);
                    endSpan.add(prefix + Character.toString(Character.MAX_VALUE));
                } else if (value.isEmpty()) {
                    startSpan.add(r.minval());
                    endSpan.add(r.maxval());
                } else {
                    startSpan.add(value);
                    endSpan.add(value);
                }

                addQuery(r.table(table.name).between(startSpan, endSpan).optArg("index", "label"));
            } else {
                // Key is empty
                if (value.endsWith("*")) {
                    String prefix = value.toLowerCase().substring(0, value.length() - 1);
                    String startSpan = prefix;
                    String endSpan = prefix + Character.toString(Character.MAX_VALUE);
                    addQuery(r.table(table.name).between(startSpan, endSpan).optArg("index", "label_value"));
                } else if (!value.isEmpty()) {
                    addQuery(r.table(table.name).between(value, value).optArg("right_bound", "closed").optArg("index", "label_value"));
                }
            }
        });
    }

    public ReqlExpr addQuery(ReqlExpr qry) {
        qry = addKindFilter(qry);
        if (listQry == null) {
            listQry = qry;
        } else {
            listQry = listQry.innerJoin(qry, (l, r) -> l.g("id").eq(r.g("id"))).zip();
        }
        return listQry;
    }

    /**
     * Build a query returning all values.
     */
    void buildAllQuery() {
        listQry = addKindFilter(r.table(table.name));
    }

    void addFilter(ReqlFunction1... filter) {
        if (listQry == null) {
            if (orderByIndex.isEmpty()) {
                listQry = addKindFilter(r.table(table.name));
            } else {
                listQry = addKindFilter(r.table(table.name).orderBy().optArg("index", orderByIndex));
            }
        }

        Arrays.stream(filter).forEach(f -> {
            listQry = listQry.filter(f);
        });
    }

    void addFilter(List<Filter> filter) {
        filter.forEach(f -> {
            switch (f.getOp()) {
                case EQ:
                    addFilter(row -> row.g(f.getFieldName()).eq(f.getValue()));
                    break;
                case NE:
                    addFilter(row -> row.g(f.getFieldName()).ne(f.getValue()));
                    break;
                case MATCH:
                    addFilter(row -> row.g(f.getFieldName()).match(f.getValue()));
                    break;
                case LT:
                    addFilter(row -> row.g(f.getFieldName()).lt(f.getValue()));
                    break;
                case GT:
                    addFilter(row -> row.g(f.getFieldName()).gt(f.getValue()));
                    break;
            }
        });
    }

    ReqlExpr getListQry() {
        return listQry;
    }

    void setListQry(ReqlExpr listQry) {
        this.listQry = listQry;
    }

    void setPaging(int pageSize, int page) {
        this.pageSize = pageSize == 0 ? 30 : pageSize;
        this.page = page;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public T getRequest() {
        return request;
    }

    public long executeCount(RethinkDbConnection conn) throws DbException {
        if (listQry == null) {
            if (orderByIndex.isEmpty()) {
                listQry = addKindFilter(r.table(table.name));
            } else {
                listQry = addKindFilter(r.table(table.name).orderBy().optArg("index", orderByIndex));
            }
        }

        return conn.exec("db-countConfigObjects", listQry.count());
    }

    public <R extends Message.Builder> R executeList(RethinkDbConnection conn, R resultBuilder) throws DbException {
        if (listQry == null) {
            if (orderByIndex.isEmpty()) {
                listQry = addKindFilter(r.table(table.name));
            } else if (descending) {
                listQry = addKindFilter(r.table(table.name).orderBy().optArg("index", r.desc(orderByIndex)));
            } else {
                listQry = addKindFilter(r.table(table.name).orderBy().optArg("index", orderByIndex));
            }
        }

        ReqlExpr qry = listQry.skip(page * pageSize).limit(pageSize);

        Object res = conn.exec("db-listConfigObjects", qry);

        Descriptors.Descriptor resDescr = resultBuilder.getDescriptorForType();
        Descriptors.FieldDescriptor pageSizeField = resDescr.findFieldByName("page_size");
        Descriptors.FieldDescriptor pageField = resDescr.findFieldByName("page");
        Descriptors.FieldDescriptor countField = resDescr.findFieldByName("count");
        Descriptors.FieldDescriptor valueField = resDescr.findFieldByName("value");

        long count = 0L;
        if (res instanceof Cursor) {
            if (pageSize > 0) {
                // Set the count for the total resultset
                count = executeCount(conn);
            }

            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                resultBuilder.addRepeatedField(valueField, convertObject(entity));
                if (pageSize == 0) {
                    // No paging, so separate query for count is not necessary.
                    count++;
                }
            }
        } else if (res != null) {
            count = 1L;
            resultBuilder.addRepeatedField(valueField, convertObject((Map) res));
        }

        resultBuilder
                .setField(pageSizeField, pageSize)
                .setField(pageField, page)
                .setField(countField, count);

        return (R) resultBuilder;
    }

    private Message convertObject(Map<String, Object> entity) {
        Message.Builder b;
        if (kind != Kind.undefined) {
            entity = RethinkDbConfigAdapter.convertV1ToOldApi(kind, entity);
        }
        switch (kind) {
            case crawlConfig:
                b = CrawlConfig.newBuilder();
                break;
            case browserConfig:
                b = BrowserConfig.newBuilder();
                break;
            case browserScript:
                b = BrowserScript.newBuilder();
                break;
            case crawlEntity:
                b = CrawlEntity.newBuilder();
                break;
            case crawlHostGroupConfig:
                b = CrawlHostGroupConfig.newBuilder();
                break;
            case crawlJob:
                b = CrawlJob.newBuilder();
                break;
            case crawlScheduleConfig:
                b = CrawlScheduleConfig.newBuilder();
                break;
            case politenessConfig:
                b = PolitenessConfig.newBuilder();
                break;
            case seed:
                b = Seed.newBuilder();
                break;
            case roleMapping:
                b = RoleMapping.newBuilder();
                break;
            default:
                b = table.schema.newBuilderForType();
        }
        return ProtoUtils.rethinkToProto(entity, b);
    }

    private ReqlExpr addKindFilter(ReqlExpr q) {
        if (kind == Kind.undefined) {
            return q;
        } else {
            return q.filter(r.hashMap("kind", kind.name()));
        }
    }
}
