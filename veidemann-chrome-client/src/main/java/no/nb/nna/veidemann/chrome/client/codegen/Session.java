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
import com.squareup.javapoet.TypeSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static javax.lang.model.element.Modifier.PUBLIC;

/**
 *
 */
public class Session {

    static final ClassName type = ClassName.get(Codegen.PACKAGE, "Session");

    final FieldSpec entryPoint = FieldSpec.builder(EntryPoint.type, "chromeDebugProtocol", Modifier.FINAL).build();

    final FieldSpec sessionClient = FieldSpec
            .builder(Codegen.CLIENT_CLASS, "sessionClient", Modifier.PRIVATE, Modifier.FINAL).build();

    final FieldSpec logger = FieldSpec
            .builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.of("$T.getLogger($T.class)", LoggerFactory.class, type)).build();

    final FieldSpec timeout = FieldSpec
            .builder(long.class, "TIMEOUT", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer("5000").build();

    final FieldSpec contextId = FieldSpec.builder(String.class, "contextId", Modifier.FINAL).build();

    final FieldSpec targetId = FieldSpec.builder(String.class, "targetId", Modifier.FINAL).build();

    final CodeBlock timeoutGet = CodeBlock.of("get($N, $T.MILLISECONDS)", timeout, TimeUnit.class);

    final List<Domain> domains;

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public Session(List<Domain> domains, File outdir) {
        this.domains = domains;
        this.outdir = outdir;

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .addSuperinterface(Closeable.class)
                .addField(logger)
                .addField(timeout)
                .addField(entryPoint)
                .addField(sessionClient)
                .addField(contextId)
                .addField(targetId);

    }

    static void generate(List<Domain> domains, File outdir) throws IOException {
        Session s = new Session(domains, outdir);
        s.genConstructor();
        s.genCloseMethod();
        s.genToStringAndVersionMethods();

        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, s.classBuilder.build()).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genConstructor() {
        ParameterSpec host = ParameterSpec.builder(String.class, "host", Modifier.FINAL).build();
        ParameterSpec port = ParameterSpec.builder(int.class, "port", Modifier.FINAL).build();
        ParameterSpec clientWidth = ParameterSpec.builder(int.class, "clientWidth", Modifier.FINAL).build();
        ParameterSpec clientHeight = ParameterSpec.builder(int.class, "clientHeight", Modifier.FINAL).build();

        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addException(IOException.class)
                .addParameter(entryPoint.type, entryPoint.name, Modifier.FINAL)
                .addParameter(host)
                .addParameter(port)
                .addParameter(clientWidth)
                .addParameter(clientHeight)
                .addStatement("this.$1N = $1N", entryPoint)
                .beginControlFlow("try")
                .addStatement("$N = $N.target().createBrowserContext().$L.getBrowserContextId()",
                        contextId, entryPoint, timeoutGet)
                .addStatement("$N = $N.target().createTarget(\"about:blank\", $N, $N, $N, false).$L.getTargetId()", targetId,
                        entryPoint, clientWidth, clientHeight, contextId, timeoutGet)
                .endControlFlow()
                .beginControlFlow("catch ($T | $T | $T ex)",
                        InterruptedException.class, ExecutionException.class, TimeoutException.class)
                .addStatement("throw new $T(ex)", IOException.class)
                .endControlFlow()
                .addCode("\n")
                .addStatement("$N = $N.$N.createSessionClient($N)",
                        sessionClient,
                        entryPoint,
                        EntryPoint.protocolClient,
                        targetId)
                .addCode("\n");

        for (Domain domain : domains) {
            if (!"Target".equals(domain.domain) && !"Browser".equals(domain.domain)) {
                FieldSpec.Builder fieldBuilder = FieldSpec
                        .builder(domain.className, Protocol.uncap(domain.domain), Modifier.PRIVATE, Modifier.FINAL);
                if (domain.description != null) {
                    fieldBuilder.addJavadoc(domain.description + "\n");
                }

                FieldSpec field = fieldBuilder.build();
                classBuilder.addField(field);

                constructor.addStatement("$N = new $T($N)", field, field.type, sessionClient);

                classBuilder.addMethod(MethodSpec.methodBuilder(Protocol.uncap(domain.domain))
                        .addModifiers(PUBLIC)
                        .returns(field.type)
                        .addStatement("return $N", field)
                        .addJavadoc(domain.description == null ? "" : domain.description.replace("$", "$$") + "\n")
                        .build());
            }
        }

        constructor.addCode("\n")
                .addStatement("inspector.onTargetCrashed(c -> close(\"Session has crashed\"))");

        constructor.addCode("\n").addStatement("$N.debug($S, $N)", logger, "Browser session created: {}", contextId);

        classBuilder.addMethod(constructor.build());
    }

    void genCloseMethod() {
        classBuilder.addMethod(MethodSpec.methodBuilder("isClosed")
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addStatement("return $N.isClosed()", sessionClient)
                .build());

        classBuilder.addMethod(MethodSpec.methodBuilder("close")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addStatement("close(\"Session is closed by user\")")
                .build());

        classBuilder.addMethod(MethodSpec.methodBuilder("close")
                .addModifiers(Modifier.PRIVATE)
                .addParameter(String.class, "reason", Modifier.FINAL)
                .addStatement("$N.debug($S, $N)", logger, "Browser session closing: {}", contextId)
                .beginControlFlow("try")
                .addStatement("$N.close(reason)", sessionClient)
                .beginControlFlow("if ($N != null)", targetId)
                .addStatement("$N.target().closeTarget(targetId).$L", entryPoint, timeoutGet)
                .endControlFlow()
                .beginControlFlow("if ($N != null)", contextId)
                .beginControlFlow("if (!$N.target().disposeBrowserContext(contextId).$L.getSuccess())", entryPoint, timeoutGet)
                .addStatement("$N.info($S, $N)", logger, "Failed closing context {}", contextId)
                .endControlFlow()
                .endControlFlow()
                .endControlFlow()
                .beginControlFlow("catch ($T | $T | $T ex)",
                        InterruptedException.class, ExecutionException.class, TimeoutException.class)
                .addStatement("$N.error($S, $N, ex.toString(), ex)",
                        logger, "Failed closing browser session '{}': {}", contextId)
                .endControlFlow()
                .beginControlFlow("finally")
                .addStatement("$N.onSessionClosed(this)", entryPoint)
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

    public static CodeBlock createUrl(String protocol, ParameterSpec host, ParameterSpec port, String path) {
        return CodeBlock.of("$S + $N + $S + $N + $S", protocol + "://", host, ":", port, path);
    }

}
