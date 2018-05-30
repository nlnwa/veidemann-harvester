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

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class QueryEngine {
    private static final QueryEngine instance = new QueryEngine();

    private final ScriptEngine engine;

    public static QueryEngine getInstance() {
        return instance;
    }

    private QueryEngine() {
        engine = new ScriptEngineManager().getEngineByName("nashorn");

        try {
            try (InputStream js = this.getClass().getClassLoader().getResourceAsStream("rethinkdbQueryParser.js");) {
                engine.eval(new InputStreamReader(js));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ReqlAst parseQuery(String query) throws ScriptException {
        Object qry = engine.eval(query);

        Invocable invocable = (Invocable) engine;
        try {
            Object result = invocable.invokeFunction("qryToProto", qry);
            return new RethinkPreparsedTerm((String) result);
        } catch (NoSuchMethodException e) {
            throw new ScriptException(e);
        }
    }
}
