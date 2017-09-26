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
package no.nb.nna.broprox.db;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.model.ConfigProto;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;

/**
 *
 */
public abstract class ConfigListQueryBuilder<T extends Message> {

    private final T request;

    private ReqlExpr listQry;

    private ReqlExpr countQry;

    private int page;

    private int pageSize;

    private String id;

    final RethinkDbAdapter.TABLES table;

    public ConfigListQueryBuilder(T request, RethinkDbAdapter.TABLES table) {
        this.request = Objects.requireNonNull(request, "The request cannot be null");
        this.table = Objects.requireNonNull(table);
    }

    /**
     * Build a query for a single object.
     * <p>
     * When this method returns, {@link #getListQry()} will return the generated query.
     * <p>
     * In case of a id query the {@link #getCountQry()} should not be used.
     *
     * @param id the object id
     */
    void buildIdQuery(String id) {
        listQry = r.table(table.name).get(id);
        this.id = id;
    }

    /**
     * Build a query for a name request.
     * <p>
     * The name parameter accepts regular expressions.
     * <p>
     * When this method returns, both {@link #getListQry()} and {@link #getCountQry()} will return the generated
     * queries.
     *
     * @param name the name regex
     */
    void buildNameQuery(String name) {
        final String qry = "(?i)" + name;
        countQry = r.table(table.name)
                .orderBy().optArg("index", "name")
                .filter(doc -> doc.g("meta").g("name").match(qry));
        listQry = countQry;
    }

    /**
     * Build a query for a label selector.
     * <p>
     * When this method returns, both {@link #getListQry()} and {@link #getCountQry()} will return the generated
     * queries.
     *
     * @param selector
     */
    void buildSelectorQuery(ConfigProto.Selector selector) {
        List<List> exactQry = r.array();
        List<ReqlExpr> spanQry = r.array();

        for (int i = 0; i < selector.getLabelCount(); i++) {
            ConfigProto.Label label = selector.getLabel(i);
            if (!label.getKey().isEmpty() && !label.getValue().isEmpty() && !label.getValue().endsWith("*")) {
                exactQry.add(r.array(label.getKey().toLowerCase(), label.getValue().toLowerCase()));
            } else if (!label.getKey().isEmpty()) {
                List startSpan = r.array();
                List endSpan = r.array();

                startSpan.add(label.getKey().toLowerCase());
                endSpan.add(label.getKey().toLowerCase());

                if (label.getValue().endsWith("*")) {
                    String prefix = label.getValue().toLowerCase().substring(0, label.getValue().length() - 1);
                    startSpan.add(prefix);
                    endSpan.add(prefix + Character.toString(Character.MAX_VALUE));
                } else if (label.getValue().isEmpty()) {
                    startSpan.add(r.minval());
                    endSpan.add(r.maxval());
                } else {
                    startSpan.add(label.getValue().toLowerCase());
                    endSpan.add(label.getValue().toLowerCase());
                }

                spanQry.add(r.table(table.name).between(startSpan, endSpan).optArg("index", "label").g("id"));
            } else {
                Object startSpan;
                Object endSpan;
                if (label.getValue().endsWith("*")) {
                    String prefix = label.getValue().toLowerCase().substring(0, label.getValue().length() - 1);
                    startSpan = prefix;
                    endSpan = prefix + Character.toString(Character.MAX_VALUE);
                    spanQry.add(r.table(table.name).between(startSpan, endSpan).optArg("index", "label_value").g("id"));
                } else if (label.getValue().isEmpty()) {
                    spanQry.add(r.table(table.name).getAll().optArg("index", "label_value").g("id"));
                } else {
                    spanQry.add(r.table(table.name).getAll(label.getValue().toLowerCase())
                            .optArg("index", "label_value").g("id"));
                }
            }
        }

        countQry = r.table(table.name).getAll(r.args(
                r.table(table.name).getAll(exactQry.toArray()).optArg("index", "label")
                        .pluck("id").group("id").count().ungroup()
                        .filter(row -> row.g("reduction").eq(selector.getLabelCount()))
                        .g("group").union(spanQry.toArray()).distinct()));
        listQry = countQry;
    }

    /**
     * Build a query returning all values.
     */
    void buildAllQuery() {
        countQry = r.table(table.name);
        listQry = countQry.orderBy().optArg("index", "name");
    }

    void addFilter(ReqlFunction1... filter) {
        Arrays.stream(filter).forEach(f -> {
            countQry = countQry.filter(f);
            listQry = listQry.filter(f);
        });
    }

    ReqlExpr getListQry() {
        return listQry;
    }

    void setListQry(ReqlExpr listQry) {
        this.listQry = listQry;
    }

    ReqlExpr getCountQry() {
        return countQry;
    }

    void setCountQry(ReqlExpr countQry) {
        this.countQry = countQry;
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

    public long executeCount(OpenTracingWrapper otw, RethinkDbAdapter db) {
        if (id != null) {
            if (otw.map("db-countConfigObjects", db::executeRequest, listQry) == null) {
                return 0L;
            } else {
                return 1L;
            }
        } else {
            return otw.map("db-countConfigObjects", db::executeRequest, countQry.count());
        }
    }

    public <R extends Message.Builder> R executeList(OpenTracingWrapper otw, RethinkDbAdapter db, R resultBuilder) {
        if (id == null) {
            listQry = listQry.skip(page * pageSize).limit(pageSize);
        }

        Object res = otw.map("db-listConfigObjects", db::executeRequest, listQry);

        Descriptors.Descriptor resDescr = resultBuilder.getDescriptorForType();
        Descriptors.FieldDescriptor pageSizeField = resDescr.findFieldByName("page_size");
        Descriptors.FieldDescriptor pageField = resDescr.findFieldByName("page");
        Descriptors.FieldDescriptor countField = resDescr.findFieldByName("count");
        Descriptors.FieldDescriptor valueField = resDescr.findFieldByName("value");

        long count = 0;
        if (res instanceof Cursor) {
            if (pageSize > 0) {
                // Set the count for the total resultset
                count = executeCount(otw, db);
            }

            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                resultBuilder.addRepeatedField(valueField,
                        ProtoUtils.rethinkToProto(entity, table.schema.newBuilderForType()));
                if (pageSize == 0) {
                    // No paging, so separate query for count is not necessary.
                    count++;
                }
            }
        } else {
            count = 1L;
            resultBuilder.addRepeatedField(valueField,
                    ProtoUtils.rethinkToProto((Map<String, Object>) res, table.schema.newBuilderForType()));
        }

        resultBuilder
                .setField(pageSizeField, pageSize)
                .setField(pageField, page)
                .setField(countField, count);

        return (R) resultBuilder;
    }

}
