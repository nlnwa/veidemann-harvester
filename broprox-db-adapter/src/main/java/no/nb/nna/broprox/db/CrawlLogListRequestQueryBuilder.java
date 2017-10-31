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
import no.nb.nna.broprox.api.ReportProto.CrawlLogListReply;
import no.nb.nna.broprox.api.ReportProto.CrawlLogListRequest;
import no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;

import static no.nb.nna.broprox.db.RethinkDbAdapter.r;

/**
 *
 */
public class CrawlLogListRequestQueryBuilder extends ConfigListQueryBuilder<CrawlLogListRequest> {

    public CrawlLogListRequestQueryBuilder(CrawlLogListRequest request) {
        super(request, TABLES.CRAWL_LOG);
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

    public CrawlLogListReply.Builder executeList(RethinkDbAdapter db) {
        return executeList(db, CrawlLogListReply.newBuilder());
    }

    void buildExecutionIdQuery(String name) {
        ReqlExpr qry = r.table(table.name)
                .getAll(name)
                .optArg("index", "executionId");
        setListQry(qry);
        setCountQry(qry);
    }
}
