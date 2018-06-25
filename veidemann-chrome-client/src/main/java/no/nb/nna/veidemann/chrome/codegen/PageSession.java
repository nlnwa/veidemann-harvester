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
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.BrowserClientBase.BrowserPage;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.SessionClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static javax.lang.model.element.Modifier.PUBLIC;
import static no.nb.nna.veidemann.chrome.codegen.BrowserClient.protocolClient;

/**
 *
 */
public class PageSession {

    static final ClassName type = ClassName.get(Codegen.PACKAGE, "PageSession");

    static final FieldSpec entryPoint = FieldSpec.builder(BrowserClient.type, "browser", Modifier.PROTECTED, Modifier.FINAL).build();

    final FieldSpec sessionClient = FieldSpec.builder(Codegen.CLIENT_CLASS, "sessionClient", Modifier.PROTECTED, Modifier.FINAL).build();

    final FieldSpec logger = FieldSpec
            .builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.of("$T.getLogger($T.class)", LoggerFactory.class, type)).build();

    final List<Domain> domains;

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public PageSession(List<Domain> domains, File outdir) {
        this.domains = domains;
        this.outdir = outdir;

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .addSuperinterface(BrowserPage.class)
                .addField(logger)
                .addField(entryPoint)
                .addField(sessionClient);

    }

    static void generate(List<Domain> domains, File outdir) throws IOException {
        PageSession s = new PageSession(domains, outdir);
        s.genConstructor();
        s.genCloseMethod();
        s.genToStringAndVersionMethods();

        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, s.classBuilder.build()).indent(Protocol.INDENT).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genConstructor() {
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addParameter(entryPoint.type, entryPoint.name, Modifier.FINAL)
                .addParameter(sessionClient.type, sessionClient.name, Modifier.FINAL)
                .addStatement("this.$1N = $1N", entryPoint)
                .addStatement("this.$1N = $1N", sessionClient)
                .addCode("\n");

        for (Domain domain : domains) {
            if (!"Target".equals(domain.domain) && !"Browser".equals(domain.domain)) {
                FieldSpec.Builder fieldBuilder = FieldSpec
                        .builder(domain.className, Codegen.uncap(domain.domain), Modifier.PRIVATE, Modifier.FINAL);
                if (domain.description != null) {
                    fieldBuilder.addJavadoc(domain.description + "\n");
                }

                FieldSpec field = fieldBuilder.build();
                classBuilder.addField(field);

                constructor.addStatement("$N = new $T($N)", field, field.type, sessionClient);

                classBuilder.addMethod(MethodSpec.methodBuilder(Codegen.uncap(domain.domain))
                        .addModifiers(PUBLIC)
                        .addException(ClientClosedException.class)
                        .addException(SessionClosedException.class)
                        .returns(field.type)
                        .beginControlFlow("if ($N.isClosed())", entryPoint)
                        .addStatement("$N.info(\"Accessing $T on closed client. {}\", $N.$N.getClosedReason())", logger, domain.className, entryPoint, protocolClient)
                        .addStatement("throw new $T($N.$N.getClosedReason())", ClientClosedException.class, entryPoint, protocolClient)
                        .endControlFlow()
                        .beginControlFlow("if ($N.isClosed())", sessionClient)
                        .addStatement("$N.info(\"Accessing $T on closed session. {}\", $N.getClosedReason())", logger, domain.className, sessionClient)
                        .addStatement("throw new $T($N.getClosedReason())", SessionClosedException.class, sessionClient)
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

    void genCloseMethod() {
        classBuilder.addMethod(MethodSpec.methodBuilder("close")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
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
