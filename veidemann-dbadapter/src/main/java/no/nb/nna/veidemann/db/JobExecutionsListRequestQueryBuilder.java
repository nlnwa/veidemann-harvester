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


import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.db.RethinkDbAdapter.TABLES;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public class JobExecutionsListRequestQueryBuilder extends ConfigListQueryBuilder<ListJobExecutionsRequest> {

    public JobExecutionsListRequestQueryBuilder(ListJobExecutionsRequest request) {
        super(request, TABLES.JOB_EXECUTIONS, "startTime", true);
        setPaging(request.getPageSize(), request.getPage());

        if (request.getIdCount() > 0) {
            buildIdQuery(request.getIdList());
        } else {
            if (!request.getStateList().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getJobId()).optArg("index", "state"));
            }

            if (!request.getJobId().isEmpty()) {
                addQuery(r.table(table.name).getAll(request.getJobId()).optArg("index", "jobId"));
            }
        }
    }

    public JobExecutionsListReply.Builder executeList(RethinkDbAdapter db) throws DbException {
        return executeList(db, JobExecutionsListReply.newBuilder());
    }

}
