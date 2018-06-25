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
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolBase;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;

import static no.nb.nna.veidemann.chrome.codegen.Codegen.PACKAGE;

/**
 * Generates the BrowserClient class.
 */
public class EntryPoint {
    static final ClassName type = ClassName.get(PACKAGE, "ChromeDebugProtocol");

    final FieldSpec logger = FieldSpec
            .builder(Logger.class, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
            .initializer(CodeBlock.of("$T.getLogger($T.class)", LoggerFactory.class, type)).build();

    final FieldSpec config = FieldSpec.builder(ChromeDebugProtocolConfig.class, "config", Modifier.FINAL).build();

    final File outdir;

    final TypeSpec.Builder classBuilder;

    public EntryPoint(File outdir) {
        this.outdir = outdir;

        final TypeName superClass = ParameterizedTypeName.get(ClassName.get(ChromeDebugProtocolBase.class), BrowserClient.type);

        classBuilder = TypeSpec.classBuilder(type).addModifiers(Modifier.PUBLIC)
                .superclass(superClass)
                .addField(logger);
    }

    static void generate(File outdir) throws IOException {
        EntryPoint e = new EntryPoint(outdir);
        e.genConnectMethod();

        JavaFile javaFile = JavaFile.builder(PACKAGE, e.classBuilder.build()).indent(Protocol.INDENT).build();
        if (outdir == null) {
            javaFile.writeTo(System.out);
        } else {
            javaFile.writeTo(outdir);
        }
    }

    void genConnectMethod() {
        MethodSpec.Builder init = MethodSpec.methodBuilder("connect")
                .addModifiers(Modifier.PUBLIC)
                .returns(BrowserClient.type)
                .addParameter(config.type, config.name, Modifier.FINAL)
                .addStatement("return super.connect($N, new $T())", config, BrowserClient.type);

        classBuilder.addMethod(init.build());
    }
}
