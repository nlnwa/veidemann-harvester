/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.rethinkdb.RethinkDB.r;

public class ListConfigObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListConfigObjectQueryBuilder.class);

    private ReqlExpr q;
    private final ListRequest request;
    final Tables table;

    public ListConfigObjectQueryBuilder(ListRequest request) {
        this.request = request;
        table = RethinkDbConfigAdapter.getTableForKind(request.getKind());

        q = r.table(table.name);

        if (request.getLabelSelectorCount() > 0) {
            buildSelectorQuery(request.getLabelSelectorList());
        }

        if (!request.getOrderByPath().isEmpty()) {
            if (request.getOrderDescending()) {
                q = q.orderBy().optArg("index",
                        r.desc(FieldMasks.getSortIndexForPath(request.getOrderByPath())));
            } else {
                q = q.orderBy().optArg("index",
                        r.asc(FieldMasks.getSortIndexForPath(request.getOrderByPath())));
            }
        }

        if (request.getIdCount() > 0) {
            if (q instanceof Table) {
                q = ((Table) q).getAll(request.getIdList().toArray());
            } else {
                q = q.filter(row -> r.expr(request.getIdList().toArray()).contains(row.g("id")));
            }
        }

        switch (request.getKind()) {
            case seed:
            case crawlEntity:
                break;
            default:
                q = q.filter(r.hashMap("kind", request.getKind().name()));
        }

        if (!request.getNameRegex().isEmpty()) {
            final String qry = "(?i)" + request.getNameRegex();
            q = q.filter(doc -> doc.g("meta").g("name").match(qry));
        }

        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            FieldMasks fm = FieldMasks.createForFieldMaskProto(request.getQueryMask());
            q = q.filter(fm.buildFilterQuery(request.getQueryTemplate()));
        }
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            FieldMasks fm = FieldMasks.createForFieldMaskProto(request.getReturnedFieldsMask());
            query = query.pluck(fm.createPluckQuery());
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    public ReqlExpr getCountQuery() {
        return q.count();
    }

    public ReqlExpr getSelectForUpdateQuery() {
        ReqlExpr query = q;

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    /**
     * Build a query for a label selector.
     *
     * @param selector
     */
    void buildSelectorQuery(List<String> selector) {
        selector.forEach(s -> {
            String key;
            String value;

            int sepIdx = s.indexOf(':');
            if (sepIdx == -1) {
                key = "";
                value = s.toLowerCase();
            } else {
                key = s.substring(0, sepIdx).toLowerCase();
                value = s.substring(sepIdx + 1).toLowerCase();
            }

            LOG.debug("Adding selector: {key={}, value={}}", key, value);

            if (!key.isEmpty() && !value.isEmpty() && !value.endsWith("*")) {
                // Exact match
                List indexKey = r.array(key, value);
                q = q.between(indexKey, indexKey).optArg("right_bound", "closed").optArg("index", "label");
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

                q = q.between(startSpan, endSpan).optArg("index", "label");
            } else {
                // Key is empty
                if (value.endsWith("*")) {
                    String prefix = value.toLowerCase().substring(0, value.length() - 1);
                    String startSpan = prefix;
                    String endSpan = prefix + Character.toString(Character.MAX_VALUE);
                    q = q.between(startSpan, endSpan).optArg("index", "label_value");
                } else if (!value.isEmpty()) {
                    q = q.between(value, value).optArg("right_bound", "closed").optArg("index", "label_value");
                }
            }
        });
    }

}
