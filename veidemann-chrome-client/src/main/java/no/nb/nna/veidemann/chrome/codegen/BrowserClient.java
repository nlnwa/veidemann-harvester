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
package no.nb.nna.veidemann.chrome.codegen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.BrowserClientBase;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.ws.CdpSession;
import no.nb.nna.veidemann.chrome.client.ws.ClientClosedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static javax.lang.model.element.Modifier.PUBLIC;

/**
 * Generates the BrowserClient class.
 */
public class BrowserClient {
    static final ClassName type = ClassName.get(Codegen.PACKAGE, "BrowserClient");

    final FieldSpec logger = FieldSpec
            .builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.of("$T.getLogger($T.class)", LoggerFactory.class, type)).build();

    static final FieldSpec protocolClient = FieldSpec.builder(Codegen.CLIENT_CLASS, "protocolClient", Modifier.FINAL)
            .build();

    final FieldSpec config = FieldSpec.builder(ChromeDebugProtocolConfig.class, "config", Modifier.FINAL).build();

    final FieldSpec closed = FieldSpec.builder(AtomicBoolean.class, "closed", Modifier.FINAL)
            .initializer("new $T(false)", AtomicBoolean.class).build();

    final List<Domain> domains;

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public BrowserClient(List<Domain> domains, File outdir) {
        this.domains = domains;
        this.outdir = outdir;

        final TypeName superClass = ParameterizedTypeName.get(ClassName.get(BrowserClientBase.class), PageSession.type);

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .superclass(superClass)
                .addSuperinterface(Closeable.class)
                .addSuperinterface(ClientClosedListener.class)
                .addField(logger)
                .addField(closed);
    }

    static void generate(List<Domain> domains, File outdir) throws IOException {
        BrowserClient e = new BrowserClient(domains, outdir);
        e.genInitMethod();
        e.genNewTargetMethod();
        e.genNewPageSessionMethod();
        e.genIsClosedMethod();
        e.genOnSessionClosedMethod();
        e.genClientClosedMethod();
        e.genToStringAndVersionMethods();

        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, e.classBuilder.build()).indent(Protocol.INDENT).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genInitMethod() {
        MethodSpec.Builder init = MethodSpec.methodBuilder("init")
                .addParameter(config.type, config.name, Modifier.FINAL)
                .addStatement("super.init($N)", config)
                .addStatement("$N.setClientClosedListener(this)", protocolClient);

        for (Domain domain : domains) {
            if ("Target".equals(domain.domain)
                    || "Browser".equals(domain.domain)
                    || "Security".equals(domain.domain)) {
                FieldSpec field = FieldSpec
                        .builder(domain.className, Codegen.uncap(domain.domain), Modifier.PRIVATE)
                        .build();
                classBuilder.addField(field);
                init.addStatement("$N = new $T($N)", field, field.type, protocolClient);

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

        init.beginControlFlow("try")
                .addStatement("target().onTargetCreated(t -> onTargetCreated(t.targetInfo()))")
                .addStatement("target().onTargetDestroyed(t -> onTargetDestroyed(t.targetId()))")
                .addStatement("target().onTargetInfoChanged(t -> onTargetInfoChanged(t.targetInfo()))")
                .addStatement("target().setDiscoverTargets(true).runAsync()")
                .nextControlFlow("catch (Exception e)")
                .addStatement("$N.error(e.getMessage(), e)", logger)
                .addStatement("throw new $T(e)", RuntimeException.class)
                .endControlFlow();

        classBuilder.addMethod(init.build());
    }

    void genNewTargetMethod() {
        ParameterSpec contextId = ParameterSpec.builder(String.class, "contextId", Modifier.FINAL).build();
        ParameterSpec clientWidth = ParameterSpec.builder(int.class, "clientWidth", Modifier.FINAL).build();
        ParameterSpec clientHeight = ParameterSpec.builder(int.class, "clientHeight", Modifier.FINAL).build();
        final TypeName returnType = ParameterizedTypeName.get(ClassName.get(CompletableFuture.class), ClassName.get("no.nb.nna.veidemann.chrome.client.TargetDomain", "CreateTargetResponse"));

        classBuilder.addMethod(MethodSpec.methodBuilder("newTarget")
                .addException(ClientClosedException.class)
                .addException(TimeoutException.class)
                .addException(ExecutionException.class)
                .addAnnotation(Override.class)
                .addParameter(contextId)
                .addParameter(clientWidth)
                .addParameter(clientHeight)
                .returns(returnType)
                .addStatement("return target().createTarget(\"about:blank\")\n.withWidth($N)\n.withHeight($N)\n" +
                                ".withBrowserContextId($N)\n.withEnableBeginFrameControl(false)\n.runAsync()",
                        clientWidth, clientHeight, contextId)
                .build());
    }

    void genNewPageSessionMethod() {
        ParameterSpec sessionClient = ParameterSpec.builder(CdpSession.class, "sessionClient", Modifier.FINAL).build();
        classBuilder.addMethod(MethodSpec.methodBuilder("newPageSession")
                .addAnnotation(Override.class)
                .addParameter(sessionClient)
                .returns(PageSession.type)
                .addStatement("return new $T(this, $N)", PageSession.type, sessionClient)
                .build());
    }

    void genOnSessionClosedMethod() {
        ParameterSpec session = ParameterSpec.builder(PageSession.type, "session", Modifier.FINAL).build();
        classBuilder.addMethod(MethodSpec.methodBuilder("onSessionClosed")
                .addModifiers(Modifier.SYNCHRONIZED)
                .addParameter(session)
                .build());
    }

    void genIsClosedMethod() {
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

}
