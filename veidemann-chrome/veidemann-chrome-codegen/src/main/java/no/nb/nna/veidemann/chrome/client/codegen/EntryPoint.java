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
package no.nb.nna.veidemann.chrome.client.codegen;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.opentracing.Tracer;
import javax.lang.model.element.Modifier;

import static no.nb.nna.veidemann.chrome.client.codegen.Protocol.uncap;

/**
 * Generates the ChromeDebugProtocol class.
 */
public class EntryPoint {
    static final ClassName type = ClassName.get(Codegen.PACKAGE, "ChromeDebugProtocol");

    static final FieldSpec protocolClient = FieldSpec.builder(Codegen.CLIENT_CLASS, "protocolClient", Modifier.FINAL)
            .build();

    static final FieldSpec tracer = FieldSpec.builder(Tracer.class, "tracer", Modifier.FINAL)
            .build();

    static final FieldSpec withActiveSpanOnly = FieldSpec
            .builder(boolean.class, "withActiveSpanOnly", Modifier.FINAL)
            .build();

    final TypeName sessionListType = ParameterizedTypeName.get(ClassName.get(List.class), Session.type);

    final TypeName contextIdListType = ParameterizedTypeName.get(ClassName.get(List.class), ClassName.get(String.class));

    final FieldSpec sessions = FieldSpec
            .builder(sessionListType, "sessions", Modifier.PRIVATE, Modifier.FINAL)
            .initializer("new $T<>()", ArrayList.class)
            .build();

    final FieldSpec timeout = FieldSpec
            .builder(long.class, "TIMEOUT", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer("5000").build();

    final FieldSpec host = FieldSpec.builder(String.class, "host", Modifier.FINAL).build();

    final FieldSpec port = FieldSpec.builder(int.class, "port", Modifier.FINAL).build();

    final FieldSpec closed = FieldSpec.builder(AtomicBoolean.class, "closed", Modifier.FINAL)
            .initializer("new $T(false)", AtomicBoolean.class).build();

    final CodeBlock timeoutGet = CodeBlock.of("get($N, $T.MILLISECONDS)", timeout, TimeUnit.class);

    final List<Domain> domains;

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public EntryPoint(List<Domain> domains, File outdir) {
        this.domains = domains;
        this.outdir = outdir;

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .addSuperinterface(Closeable.class)
                .addField(timeout)
                .addField(host)
                .addField(port)
                .addField(protocolClient)
                .addField(sessions)
                .addField(closed)
                .addField(tracer)
                .addField(withActiveSpanOnly);
    }

    static void generate(List<Domain> domains, File outdir) throws IOException {
        EntryPoint e = new EntryPoint(domains, outdir);
        e.genConstructors();
        e.genNewSessionMethod();
        e.genCloseMethod();
        e.genOnSessionClosedMethod();
        e.genToStringAndVersionMethods();
        e.genGetOpenContextsMethod();

        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, e.classBuilder.build()).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genConstructors() {

        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(host.type, host.name, Modifier.FINAL)
                .addParameter(port.type, port.name, Modifier.FINAL)
                .addParameter(tracer.type, tracer.name, Modifier.FINAL)
                .addParameter(withActiveSpanOnly.type, withActiveSpanOnly.name, Modifier.FINAL)
                .addStatement("this.$1N = $1N", host)
                .addStatement("this.$1N = $1N", port)
                .addStatement("this.$1N = $1N", tracer)
                .addStatement("this.$1N = $1N", withActiveSpanOnly)
                .addStatement("$N = new Cdp($N, $N, $N, $N)", protocolClient, host, port, tracer, withActiveSpanOnly);

        for (Domain domain : domains) {
            if ("Target".equals(domain.domain) || "Browser".equals(domain.domain)) {
                FieldSpec field = FieldSpec
                        .builder(domain.className, uncap(domain.domain), Modifier.FINAL)
                        .build();
                classBuilder.addField(field);
                constructor.addStatement("$N = new $T($N)", field, field.type, protocolClient);
            }
        }

        classBuilder.addMethod(constructor.build());

        constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(host.type, host.name, Modifier.FINAL)
                .addParameter(port.type, port.name, Modifier.FINAL)
                .addParameter(tracer.type, tracer.name, Modifier.FINAL)
                .addStatement("this($N, $N, $N, true)", host, port, tracer);

        classBuilder.addMethod(constructor.build());

        constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(host.type, host.name, Modifier.FINAL)
                .addParameter(port.type, port.name, Modifier.FINAL)
                .addStatement("this($N, $N, null, true)", host, port);

        classBuilder.addMethod(constructor.build());
    }

    void genNewSessionMethod() {
        ParameterSpec clientWidth = ParameterSpec.builder(int.class, "clientWidth", Modifier.FINAL).build();
        ParameterSpec clientHeight = ParameterSpec.builder(int.class, "clientHeight", Modifier.FINAL).build();
        classBuilder.addMethod(MethodSpec.methodBuilder("newSession")
                .addException(IOException.class)
                .addModifiers(Modifier.PUBLIC, Modifier.SYNCHRONIZED)
                .addParameter(clientWidth)
                .addParameter(clientHeight)
                .returns(Session.type)
                .beginControlFlow("if ($N.get())", closed)
                .addStatement("throw new $T($S)", IOException.class, "Client is closed")
                .endControlFlow()
                .addStatement("$1T s = new $1T(this, $2N, $3N, $4N, $5N)",
                        Session.type, host, port, clientWidth, clientHeight)
                .addStatement("$N.add(s)", sessions)
                .addStatement("return s")
                .build());
    }

    void genOnSessionClosedMethod() {
        ParameterSpec session = ParameterSpec.builder(Session.type, "session", Modifier.FINAL).build();
        classBuilder.addMethod(MethodSpec.methodBuilder("onSessionClosed")
                .addModifiers(Modifier.SYNCHRONIZED)
                .addParameter(session)
                .addStatement("$N.remove($N)", sessions, session)
                .build());
    }

    void genCloseMethod() {
        classBuilder.addMethod(MethodSpec.methodBuilder("close")
                .addModifiers(Modifier.PUBLIC, Modifier.SYNCHRONIZED)
                .addAnnotation(Override.class)
                .addStatement("$N.set(true)", closed)
                .beginControlFlow("while (!$N.isEmpty())", sessions)
                .addStatement("$N.get(0).close()", sessions)
                .endControlFlow()
                .addStatement("$N.close()", protocolClient)
                .build());
    }

    void genToStringAndVersionMethods() {
        MethodSpec version = MethodSpec.methodBuilder("version")
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S", "Chrome/" + Codegen.CHROME_VERSION)
                .build();
        classBuilder.addMethod(version);

        classBuilder.addMethod(MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S + $N()", "Chrome Debug Protocol ", version)
                .build());
    }

    void genGetOpenContextsMethod() {
        MethodSpec getOpenContexts = MethodSpec.methodBuilder("getOpenContexts")
                .addModifiers(Modifier.PUBLIC, Modifier.SYNCHRONIZED)
                .addException(IOException.class)
                .returns(contextIdListType)
                .addJavadoc("Get a list of context ids opened by this client.\n\n"
                        + "@return List of context ids\n")
                .beginControlFlow("if ($N.get())", closed)
                .addStatement("throw new $T($S)", IOException.class, "Client is closed")
                .endControlFlow()
                .addStatement("$T contextIds = new $T<>()", contextIdListType, ArrayList.class)
                .beginControlFlow("for ($T s : $N)", Session.type, sessions)
                .addStatement("contextIds.add(s.contextId)")
                .endControlFlow()
                .addStatement("return contextIds")
                .build();
        classBuilder.addMethod(getOpenContexts);
    }

}