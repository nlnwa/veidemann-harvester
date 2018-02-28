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


import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;

import static no.nb.nna.veidemann.db.RethinkDbAdapter.r;

/**
 *
 */
public class SeedListRequestQueryBuilder extends ConfigListQueryBuilder<SeedListRequest> {

    public SeedListRequestQueryBuilder(SeedListRequest request) {
        super(request, RethinkDbAdapter.TABLES.SEEDS);
        setPaging(request.getPageSize(), request.getPage());

        buildNameQuery(request.getName());
        buildSelectorQuery(request.getLabelSelectorList());

        if (!request.getCrawlJobId().isEmpty()) {
            addQuery(r.table(table.name).getAll(request.getCrawlJobId()).optArg("index", "jobId"));
        }

        if (!request.getEntityId().isEmpty()) {
            addQuery(r.table(table.name).getAll(request.getEntityId()).optArg("index", "entityId"));
        }
    }

    public SeedListReply.Builder executeList(RethinkDbAdapter db) {
        return executeList(db, SeedListReply.newBuilder());
    }

}
