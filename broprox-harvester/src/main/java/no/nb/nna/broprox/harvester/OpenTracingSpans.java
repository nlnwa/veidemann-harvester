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
package no.nb.nna.broprox.harvester;

import java.util.HashMap;
import java.util.Map;

import io.opentracing.Span;

/**
 *
 */
public class OpenTracingSpans {

    private static final Map<String, Span> spans = new HashMap<>();

    private OpenTracingSpans() {
    }

    public static void register(String executionId, Span span) {
        spans.put(executionId, span);
    }

    public static Span get(String executionId) {
        return spans.get(executionId);
    }

    public static void remove(String executionId) {
        spans.remove(executionId);
    }
}
