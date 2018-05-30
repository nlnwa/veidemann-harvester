/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.controller.query;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.proto.TermType;
import org.json.simple.parser.JSONParser;

public class RethinkPreparsedTerm extends ReqlAst {
    private final String ast;

    protected RethinkPreparsedTerm(String ast) {
        // Superclass requires a term type, but it is not used in this class
        super(TermType.DATUM, null, null);
        this.ast = ast;
    }

    @Override
    protected Object build() {
        try {
            return new JSONParser().parse(ast);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RethinkPreparsedTerm{");
        sb.append("ast='").append(ast).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
