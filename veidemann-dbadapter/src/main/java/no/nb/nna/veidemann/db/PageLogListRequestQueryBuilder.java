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

import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.veidemann.api.ReportProto.PageLogListReply;
import no.nb.nna.veidemann.api.ReportProto.PageLogListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public class PageLogListRequestQueryBuilder extends ConfigListQueryBuilder<PageLogListRequest> {

    public PageLogListRequestQueryBuilder(PageLogListRequest request) {
        super(request, TABLES.PAGE_LOG);
        setPaging(request.getPageSize(), request.getPage());

        if (request.getWarcIdCount() > 0) {
            buildIdQuery(request.getWarcIdList());
        } else if (!request.getExecutionId().isEmpty()) {
            buildExecutionIdQuery(request.getExecutionId());
        } else {
            buildAllQuery();
        }

        addFilter(request.getFilterList());
    }

    public PageLogListReply.Builder executeList(RethinkDbConnection conn) throws DbException {
        return executeList(conn, PageLogListReply.newBuilder());
    }

    void buildExecutionIdQuery(String name) {
        ReqlExpr qry = r.table(table.name)
                .getAll(name)
                .optArg("index", "executionId");
        setListQry(qry);
    }
}
