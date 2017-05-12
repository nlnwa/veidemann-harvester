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

import com.rethinkdb.RethinkDB;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListReply;
import no.nb.nna.broprox.api.ControllerProto.CrawlJobListRequest;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.db.RethinkDbAdapter.TABLES;

/**
 *
 */
public class CrawlJobListRequestQueryBuilder extends ConfigListQueryBuilder<CrawlJobListRequest> {

    static final RethinkDB r = RethinkDB.r;

    public CrawlJobListRequestQueryBuilder(CrawlJobListRequest request) {
        super(request, TABLES.CRAWL_JOBS);
        setPaging(request.getPageSize(), request.getPage());

        switch (request.getQryCase()) {
            case ID:
                buildIdQuery(request.getId());
                break;
            case NAME_PREFIX:
                buildNamePrefixQuery(request.getNamePrefix());
                break;
            case SELECTOR:
                buildSelectorQuery(request.getSelector());
                break;
            default:
                buildAllQuery();
                break;
        }

        if (request.getExpand()) {
            setListQry(getListQry().merge(cj -> r.hashMap()
                    .with("schedule", r.table(TABLES.CRAWL_SCHEDULE_CONFIGS.name).get(
                            cj.g("scheduleId").default_("")))
                    .with("crawlConfig", r.table(TABLES.CRAWL_CONFIGS.name).get(
                            cj.g("crawlConfigId").default_(""))
                            .merge(cc -> r.hashMap()
                            .with("browserConfig", r.table(TABLES.BROWSER_CONFIGS.name).get(
                                    cc.g("browserConfigId").default_("")))
                            .with("politeness", r.table(TABLES.POLITENESS_CONFIGS.name).get(
                                    cc.g("politenessId").default_(""))))
                            .without("browserConfigId", "politenessId")))
                    .without("scheduleId", "crawlConfigId"));
        }
    }

    public CrawlJobListReply.Builder executeList(OpenTracingWrapper otw, RethinkDbAdapter db) {
        return executeList(otw, db, CrawlJobListReply.newBuilder());
    }

}
