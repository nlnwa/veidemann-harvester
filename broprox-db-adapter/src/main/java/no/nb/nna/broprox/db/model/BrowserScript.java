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
package no.nb.nna.broprox.db.model;

import java.util.Map;

import no.nb.nna.broprox.db.DbObject;

/**
 *
 */
public interface BrowserScript extends DbObject<BrowserScript> {
    public enum Type {
        BEHAVIOR,
        EXTRACT_OUTLINKS,
        LOGIN
    }

    BrowserScript withId(String id);

    String getId();

    BrowserScript withType(Type type);

    Type getType();

    BrowserScript withName(String name);

    String getName();
    
    BrowserScript withScript(String script);

    String getScript();

    BrowserScript withUrlRegexp(String regexp);

    String getUrlRegexp();

    BrowserScript withParameters(Map<String, String> parameters);

    Map<String, String> getParameters();
}
