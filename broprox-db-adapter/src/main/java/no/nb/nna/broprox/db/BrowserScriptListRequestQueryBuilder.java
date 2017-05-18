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


import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListReply;
import no.nb.nna.broprox.api.ControllerProto.BrowserScriptListRequest;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;

/**
 *
 */
public class BrowserScriptListRequestQueryBuilder extends ConfigListQueryBuilder<BrowserScriptListRequest> {

    public BrowserScriptListRequestQueryBuilder(BrowserScriptListRequest request) {
        super(request, RethinkDbAdapter.TABLES.BROWSER_SCRIPTS);
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
            case TYPE:
                buildAllQuery();
                addFilter((row -> row.g("type").eq(request.getType().name())));
                break;
            default:
                buildAllQuery();
                break;
        }
    }

    public BrowserScriptListReply.Builder executeList(OpenTracingWrapper otw, RethinkDbAdapter db) {
        return executeList(otw, db, BrowserScriptListReply.newBuilder());
    }

}
