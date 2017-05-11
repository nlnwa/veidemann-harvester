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
package no.nb.nna.broprox.commons.opentracing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.grpc.Context;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.OpenTracingContextKey;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class OpenTracingWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTracingWrapper.class);

    private static final ContextContainer EMPTY_CONTAINER = new ContextContainer(null, null, null);

    private final String component;

    private final String spanKind;

    private final Map<String, String> tags = new HashMap<>();

    Span parentSpan;

    SpanContext parentSpanContext;

    String parentReferenceType = References.CHILD_OF;

    boolean spanFromGrpcContext = true;

    boolean newGrpcContextForSpan = true;

    boolean traceEnabled = true;

    public OpenTracingWrapper(String component, String spanKind) {
        this.component = component;
        this.spanKind = spanKind;
    }

    public OpenTracingWrapper(String component) {
        this(component, null);
    }

    public OpenTracingWrapper addTag(String key, String value) {
        tags.put(key, value);
        return this;
    }

    public OpenTracingWrapper setParentSpan(Span parentSpan) {
        this.parentSpan = parentSpan;
        return this;
    }

    public OpenTracingWrapper setParentSpan(SpanContext parentSpanContext) {
        this.parentSpanContext = parentSpanContext;
        return this;
    }

    public OpenTracingWrapper setParentReferenceType(String parentReferenceType) {
        this.parentReferenceType = parentReferenceType;
        return this;
    }

    public OpenTracingWrapper setExtractParentSpanFromGrpcContext(boolean spanFromGrpcContext) {
        this.spanFromGrpcContext = spanFromGrpcContext;
        return this;
    }

    public OpenTracingWrapper setCreateNewGrpcContextForSpan(boolean newGrpcContextForSpan) {
        this.newGrpcContextForSpan = newGrpcContextForSpan;
        return this;
    }

    public OpenTracingWrapper setTraceEnabled(boolean traceEnabled) {
        this.traceEnabled = traceEnabled;
        return this;
    }

    private ContextContainer prepareSpan(String operationName) {
        if (!traceEnabled) {
            return EMPTY_CONTAINER;
        }

        if (parentSpan == null && parentSpanContext == null) {
            if (spanFromGrpcContext) {
                parentSpan = OpenTracingContextKey.activeSpan();
            }
        }

        Tracer.SpanBuilder spanBuilder = GlobalTracer.get()
                .buildSpan(operationName)
                .withTag(Tags.COMPONENT.getKey(), component);

        if (spanKind != null) {
            spanBuilder.withTag(Tags.SPAN_KIND.getKey(), spanKind);
        }

        tags.forEach((k, v) -> spanBuilder.withTag(k, v));

        if (parentSpan != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentSpan.context());
        } else if (parentSpanContext != null) {
            spanBuilder.asChildOf(parentSpanContext);
        }

        Span span = spanBuilder.start();

        Context currentContext = null;
        Context prevContext = null;

        if (newGrpcContextForSpan) {
            currentContext = Context.current()
                    .withValues(OpenTracingContextKey.getKey(), span, OpenTracingParentContextKey.getKey(), parentSpan);
            prevContext = currentContext.attach();
        }

        return new ContextContainer(currentContext, prevContext, span);
    }

    private void finishSpan(ContextContainer context) {
        if (!traceEnabled) {
            return;
        }

        try {
            if (newGrpcContextForSpan) {
                context.currentContext.detach(context.prevContext);
            }
            context.currentSpan.finish();
        } catch (Exception e) {
            LOG.error("Could not finish span", e);
        }
    }

    public <T, R> R map(String operationName, Function<? super T, ? extends R> func, T param) {
        ContextContainer context = prepareSpan(operationName);
        try {
            return func.apply(param);
        } finally {
            finishSpan(context);
        }
    }

    public <T, U, R> R map(String operationName, BiFunction<? super T, ? super U, ? extends R> func, T par1, U par2) {
        ContextContainer context = prepareSpan(operationName);
        try {
            return func.apply(par1, par2);
        } finally {
            finishSpan(context);
        }
    }

    public <T> T run(String operationName, Supplier<? extends T> func) {
        ContextContainer context = prepareSpan(operationName);
        try {
            return func.get();
        } finally {
            finishSpan(context);
        }
    }

    public <T> void run(String operationName, Consumer<? super T> func, T param) {
        ContextContainer context = prepareSpan(operationName);
        try {
            func.accept(param);
        } finally {
            finishSpan(context);
        }
    }

    public <T, U> void run(String operationName, BiConsumer<? super T, ? super U> func, T par1, U par2) {
        ContextContainer context = prepareSpan(operationName);
        try {
            func.accept(par1, par2);
        } finally {
            finishSpan(context);
        }
    }

    public <T> void run(String operationName, Runnable func) {
        ContextContainer context = prepareSpan(operationName);
        try {
            func.run();
        } finally {
            finishSpan(context);
        }
    }

    public <V> V call(String operationName, Callable<? extends V> func) throws Exception {
        ContextContainer context = prepareSpan(operationName);
        try {
            return func.call();
        } finally {
            finishSpan(context);
        }
    }

    private static class ContextContainer {

        final Context currentContext;

        final Context prevContext;

        final Span currentSpan;

        public ContextContainer(Context currentContext, Context prevContext, Span currentSpan) {
            this.currentContext = currentContext;
            this.prevContext = prevContext;
            this.currentSpan = currentSpan;
        }

    }
}
