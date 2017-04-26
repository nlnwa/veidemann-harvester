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

package no.nb.nna.broprox.commons;

import io.grpc.Context;
import io.opentracing.Span;

/**
 * A {@link io.grpc.Context} key for the parent OpenTracing trace state.
 *
 * Can be used to get the parent span, or to set the parent span for a scoped unit of work.
 */
public class OpenTracingParentContextKey {

    public static final String KEY_NAME = "io.opentracing.parent-span";
    private static final Context.Key<Span> key = Context.key(KEY_NAME);

    /**
     * @return the parent span for the current request
     */
    public static Span parentSpan() {
        return key.get();
    }

    /**
     * @return the OpenTracing context key
     */
    public static Context.Key<Span> getKey() {
        return key;
    }
}