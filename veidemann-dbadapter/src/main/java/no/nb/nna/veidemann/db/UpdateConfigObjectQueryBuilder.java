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
import com.rethinkdb.gen.ast.ReqlFunction1;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class UpdateConfigObjectQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateConfigObjectQueryBuilder.class);

    ReqlExpr q;

    public UpdateConfigObjectQueryBuilder(UpdateRequest request) {
        ListConfigObjectQueryBuilder l = new ListConfigObjectQueryBuilder(request.getListRequest());
        q = l.getSelectForUpdateQuery();

        ReqlFunction1 updateDoc;
        if (request.hasUpdateMask()) {
            updateDoc = FieldMasks.createForFieldMaskProto(request.getUpdateMask())
                    .buildUpdateQuery(request.getListRequest().getKind(), request.getUpdateTemplate());
        } else {
            updateDoc = FieldMasks.CONFIG_OBJECT_DEF
                    .buildUpdateQuery(request.getListRequest().getKind(), request.getUpdateTemplate());
        }

        String user;
        if (EmailContextKey.email() == null || EmailContextKey.email().isEmpty()) {
            user = "anonymous";
        } else {
            user = EmailContextKey.email();
        }

        q = q.merge(updateDoc)
                .forEach(doc -> r.table(l.table.name)
                        .insert(doc)
                        .optArg("conflict",
                                (id, old_doc, new_doc) -> r.branch(
                                        old_doc.eq(new_doc),
                                        old_doc,
                                        new_doc.merge(
                                                r.hashMap("meta", r.hashMap()
                                                        .with("lastModified", r.now())
                                                        .with("lastModifiedBy", user)
                                                ))))
                );
    }

    public ReqlExpr getUpdateQuery() {
        return q;
    }

}
