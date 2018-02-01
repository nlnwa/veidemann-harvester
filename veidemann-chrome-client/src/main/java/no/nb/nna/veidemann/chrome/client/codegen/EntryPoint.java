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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.MaxActiveSessionsExceededException;
import no.nb.nna.veidemann.chrome.client.ws.ClientClosedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static javax.lang.model.element.Modifier.PUBLIC;
import static no.nb.nna.veidemann.chrome.client.codegen.Protocol.INDENT;

/**
 * Generates the ChromeDebugProtocol class.
 */
public class EntryPoint {
    static final ClassName type = ClassName.get(Codegen.PACKAGE, "ChromeDebugProtocol");

    final FieldSpec logger = FieldSpec
            .builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.of("$T.getLogger($T.class)", LoggerFactory.class, type)).build();

    static final FieldSpec protocolClient = FieldSpec.builder(Codegen.CLIENT_CLASS, "protocolClient", Modifier.FINAL)
            .build();

    final TypeName sessionListType = ParameterizedTypeName.get(ClassName.get(List.class), Session.type);

    final TypeName contextIdListType = ParameterizedTypeName.get(ClassName.get(List.class), ClassName.get(String.class));

    final FieldSpec sessions = FieldSpec
            .builder(sessionListType, "sessions", Modifier.PRIVATE, Modifier.FINAL)
            .initializer("new $T<>()", ArrayList.class)
            .build();

    final FieldSpec config = FieldSpec.builder(ChromeDebugProtocolConfig.class, "config", Modifier.FINAL).build();

    final FieldSpec closed = FieldSpec.builder(AtomicBoolean.class, "closed", Modifier.FINAL)
            .initializer("new $T(false)", AtomicBoolean.class).build();

    final List<Domain> domains;

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public EntryPoint(List<Domain> domains, File outdir) {
        this.domains = domains;
        this.outdir = outdir;

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .addSuperinterface(Closeable.class)
                .addSuperinterface(ClientClosedListener.class)
                .addField(logger)
                .addField(config)
                .addField(protocolClient)
                .addField(sessions)
                .addField(closed);
    }

    static void generate(List<Domain> domains, File outdir) throws IOException {
        EntryPoint e = new EntryPoint(domains, outdir);
        e.genConstructors();
        e.genNewSessionMethod();
        e.genCloseMethod();
        e.genIsClosesMethod();
        e.genOnSessionClosedMethod();
        e.genClientClosedMethod();
        e.genToStringAndVersionMethods();
        e.genGetOpenContextsMethod();

        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, e.classBuilder.build()).indent(INDENT).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genConstructors() {

        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(config.type, config.name, Modifier.FINAL)
                .addStatement("this.$1N = $1N", config)
                .addStatement("$N = new Cdp($N)", protocolClient, config)
                .addStatement("$N.setClientClosedListener(this)", protocolClient);

        for (Domain domain : domains) {
            if ("Target".equals(domain.domain) || "Browser".equals(domain.domain)) {
                FieldSpec field = FieldSpec
                        .builder(domain.className, Codegen.uncap(domain.domain), Modifier.FINAL, Modifier.PRIVATE)
                        .build();
                classBuilder.addField(field);
                constructor.addStatement("$N = new $T(this, $N)", field, field.type, protocolClient);

                classBuilder.addMethod(MethodSpec.methodBuilder(Codegen.uncap(domain.domain))
                        .addModifiers(PUBLIC)
                        .returns(field.type)
                        .addException(ClientClosedException.class)
                        .beginControlFlow("if (isClosed())")
                        .addStatement("$N.info(\"Accessing $T on closed client. {}\", $N.getClosedReason())", logger, domain.className, protocolClient)
                        .addStatement("throw new $T($N.getClosedReason())", ClientClosedException.class, protocolClient)
                        .endControlFlow()
                        .addStatement("return $N", field)
                        .addJavadoc("Get the $N domain.\n<p>\n", domain.domain)
                        .addJavadoc(domain.description == null ? "" : domain.description.replace("$", "$$") + "\n")
                        .addJavadoc("\n@return the $N domain\n", domain.domain)
                        .build());
            }
        }

        classBuilder.addMethod(constructor.build());
    }

    void genNewSessionMethod() {
        ParameterSpec clientWidth = ParameterSpec.builder(int.class, "clientWidth", Modifier.FINAL).build();
        ParameterSpec clientHeight = ParameterSpec.builder(int.class, "clientHeight", Modifier.FINAL).build();
        classBuilder.addMethod(MethodSpec.methodBuilder("newSession")
                .addException(IOException.class)
                .addException(TimeoutException.class)
                .addException(ExecutionException.class)
                .addModifiers(Modifier.PUBLIC, Modifier.SYNCHRONIZED)
                .addParameter(clientWidth)
                .addParameter(clientHeight)
                .returns(Session.type)
                .beginControlFlow("if (isClosed())")
                .addStatement("$N.info(\"Creating new session on closed client. {}\", $N.getClosedReason())", logger, protocolClient)
                .addStatement("throw new $T($N.getClosedReason())", ClientClosedException.class, protocolClient)
                .endControlFlow()
                .beginControlFlow("if (target().getTargets().run().targetInfos().size() > $N.getMaxOpenSessions())", config)
                .addStatement("throw new $T($N.getMaxOpenSessions())", MaxActiveSessionsExceededException.class, config)
                .endControlFlow()
                .addStatement("$1T s = new $1T(this, $2N, $3N, $4N)",
                        Session.type, config, clientWidth, clientHeight)
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
                .addStatement("$N.onClose(\"Client is closed by user\")", protocolClient)
                .addStatement("$N.cleanup()", protocolClient)
                .build());
    }

    void genIsClosesMethod() {
        classBuilder.addMethod(MethodSpec.methodBuilder("isClosed")
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addStatement("return $N.get()", closed)
                .build());
    }

    void genClientClosedMethod() {
        classBuilder.addMethod(MethodSpec.methodBuilder("clientClosed")
                .addModifiers(Modifier.PUBLIC, Modifier.SYNCHRONIZED)
                .addAnnotation(Override.class)
                .addParameter(String.class, "reason")
                .addStatement("$N.set(true)", closed)
                .beginControlFlow("while (!$N.isEmpty())", sessions)
                .addStatement("$N.get(0).close(\"Client closed\")", sessions)
                .endControlFlow()
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
