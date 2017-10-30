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

import com.rethinkdb.gen.ast.ReqlExpr;
import no.nb.nna.broprox.api.ReportProto.PageLogListReply;
import no.nb.nna.broprox.api.ReportProto.PageLogListRequest;
import no.nb.nna.broprox.api.ReportProto.ScreenshotListReply;
import no.nb.nna.broprox.api.ReportProto.ScreenshotListRequest;
import no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;

/**
 *
 */
public class ScreenshotListRequestQueryBuilder extends ConfigListQueryBuilder<ScreenshotListRequest> {

    public ScreenshotListRequestQueryBuilder(ScreenshotListRequest request) {
        super(request, TABLES.SCREENSHOT);
        setPaging(request.getPageSize(), request.getPage());

        switch (request.getQryCase()) {
            case ID:
                buildIdQuery(request.getId());
                break;
            case EXECUTION_ID:
                buildAllQuery();
                addFilter(row -> row.g("executionId").equals(request.getExecutionId()));
                break;
            case URI:
                buildAllQuery();
                addFilter(row -> row.g("uri").equals(request.getUri()));
                break;
            default:
                buildAllQuery();
                break;
        }

    }

    public ScreenshotListReply.Builder executeList(RethinkDbAdapter db) {
        return executeList(db, ScreenshotListReply.newBuilder());
    }

    void buildExecutionIdQuery(String name) {
        ReqlExpr qry = r.table(table.name)
                .getAll(name)
                .optArg("index", "executionId");
        setListQry(qry);
        setCountQry(qry);
    }
}
