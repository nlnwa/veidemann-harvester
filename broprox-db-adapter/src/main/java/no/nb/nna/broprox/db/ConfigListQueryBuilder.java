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
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.net.Cursor;
import no.nb.nna.broprox.commons.OpenTracingWrapper;

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

    private final RethinkDbAdapter.TABLES table;

    public ConfigListQueryBuilder(T request, RethinkDbAdapter.TABLES table) {
        this.request = Objects.requireNonNull(request, "The request cannot be null");
        this.table = Objects.requireNonNull(table);
    }

    /**
     * Build a query for a single object.
     * <p>
     * When this method returns, {@link #getListQry()} will return the generated query.
     *
     * @param id the object id
     */
    void buildIdQuery(String id) {
        listQry = r.table(table.name).get(id);
        this.id = id;
    }

    /**
     * Build a query for a name prefix request.
     * <p>
     * When this method returns, both {@link #getListQry()} and {@link #getCountQry()} will return the generated query.
     *
     * @param prefix the name prefix
     */
    void buildNamePrefixQuery(String prefix) {
        prefix = prefix.toLowerCase();
        countQry = r.table(table.name)
                .between(prefix, prefix + Character.toString(Character.MAX_VALUE))
                .optArg("index", "name");
        listQry = countQry.orderBy().optArg("index", "name");
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
        this.pageSize = pageSize;
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
            return 1L;
        } else {
            return otw.map("db-countConfigObjects", db::executeRequest, countQry.count());
        }
    }

    public <R extends Message.Builder> R executeList(OpenTracingWrapper otw, RethinkDbAdapter db, R resultBuilder) {
        long count = executeCount(otw, db);

        if (pageSize > 0) {
            listQry = listQry.skip(page * pageSize).limit(pageSize);
        }

        Object res = otw.map("db-listConfigObjects", db::executeRequest, listQry);
        Descriptors.Descriptor resDescr = resultBuilder.getDescriptorForType();
        Descriptors.FieldDescriptor pageSizeField = resDescr.findFieldByName("page_size");
        Descriptors.FieldDescriptor pageField = resDescr.findFieldByName("page");
        Descriptors.FieldDescriptor countField = resDescr.findFieldByName("count");
        Descriptors.FieldDescriptor valueField = resDescr.findFieldByName("value");

        resultBuilder
                .setField(pageSizeField, pageSize)
                .setField(pageField, page)
                .setField(countField, count);

        if (res instanceof Cursor) {
            Cursor<Map<String, Object>> cursor = (Cursor) res;
            for (Map<String, Object> entity : cursor) {
                resultBuilder.addRepeatedField(valueField,
                        ProtoUtils.rethinkToProto(entity, table.schema.newBuilderForType()));
            }
        } else {
            resultBuilder.addRepeatedField(
                    valueField, ProtoUtils.rethinkToProto((Map<String, Object>) res, table.schema.newBuilderForType()));
        }

        return (R) resultBuilder;
    }

}
