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


import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.commons.db.DbException;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public class CrawlExecutionsListRequestQueryBuilder extends ConfigListQueryBuilder<ListExecutionsRequest> {

    public CrawlExecutionsListRequestQueryBuilder(ListExecutionsRequest request) {
        super(request, Tables.EXECUTIONS, "startTime", true);
        setPaging(request.getPageSize(), request.getPage());

        if (request.getIdCount() > 0) {
            buildIdQuery(request.getIdList());
        } else {
            if (!request.getStateList().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getJobId()).optArg("index", "state"));
            }

            if (!request.getJobExecutionId().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getJobExecutionId()).optArg("index", "jobExecutionId"));
            }

            if (!request.getJobId().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getJobId()).optArg("index", "jobId"));
            }

            if (!request.getSeedId().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getSeedId()).optArg("index", "seedId"));
            }
        }
    }

    public ExecutionsListReply.Builder executeList(RethinkDbConnection conn) throws DbException {
        return executeList(conn, ExecutionsListReply.newBuilder());
    }

}
