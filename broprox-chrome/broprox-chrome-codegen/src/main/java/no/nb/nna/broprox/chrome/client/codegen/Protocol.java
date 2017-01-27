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
package no.nb.nna.broprox.chrome.client.codegen;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;
import javax.annotation.Generated;
import javax.lang.model.element.Modifier;

/**
 *
 */
public class Protocol {

    List<Domain> domains;

    static Pattern pattern = Pattern.compile("([A-Z]+)(.*)");

    void gencode(File outdir) throws IOException {
        AnnotationSpec generatedAnnotation = AnnotationSpec.builder(Generated.class)
                .addMember("value", "$S", Codegen.class.getCanonicalName()).addMember("date", "$S", Instant.now()
                        .toString()).build();
        for (Domain domain : domains) {
            domain.init(generatedAnnotation);
        }
        for (Domain domain : domains) {
            domain.buildType(this);
        }
        for (Domain domain : domains) {
            JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, domain.builder.build()).build();
            if (outdir == null) {
                javaFile.writeTo(System.out);
            } else {
                javaFile.writeTo(outdir);
            }
        }
        EntryPoint.generate(domains, outdir);
        Session.generate(domains, outdir);
    }

//    private void genEntrypoint(File outdir) throws IOException {
//        TypeSpec.Builder entrypoint = TypeSpec.classBuilder("ChromeDebugProtocol").addModifiers(Modifier.PUBLIC)
//                .addSuperinterface(Closeable.class)
//                .addField(Codegen.CLIENT_CLASS, "protocolClient", Modifier.PUBLIC, Modifier.FINAL);
//
//        entrypoint.addMethod(
//                MethodSpec.constructorBuilder().addParameter(String.class, "webSocketDebuggerUrl")
//                .addModifiers(Modifier.PUBLIC)
//                .addStatement("this(new $T(webSocketDebuggerUrl))", Codegen.CLIENT_CLASS).build());
//
//        entrypoint.addMethod(
//                MethodSpec.constructorBuilder().addParameter(URI.class, "webSocketDebuggerUrl")
//                .addModifiers(Modifier.PUBLIC)
//                .addStatement("this(new $T(webSocketDebuggerUrl))", Codegen.CLIENT_CLASS).build());
//
//        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
//                .addParameter(Codegen.CLIENT_CLASS, "protocolClient").addModifiers(Modifier.PRIVATE)
//                .addStatement("this.protocolClient = protocolClient");
//        for (Domain domain : domains) {
//            FieldSpec field = FieldSpec.builder(domain.className, uncap(domain.domain), Modifier.PUBLIC, Modifier.FINAL)
//                    .build();
//            entrypoint.addField(field);
//            constructor.addStatement("$N = new $T(protocolClient)", field, domain.className);
//        }
//        entrypoint.addMethod(constructor.build());
//
//        entrypoint.addMethod(MethodSpec.methodBuilder("close")
//                .addModifiers(Modifier.PUBLIC)
//                .addStatement("protocolClient.close()")
//                .build());
//
//        MethodSpec version = MethodSpec.methodBuilder("version")
//                .addModifiers(Modifier.PUBLIC)
//                .returns(String.class)
//                .addStatement("return $S", "Chrome/" + Codegen.CHROME_VERSION)
//                .build();
//        entrypoint.addMethod(version);
//
//        entrypoint.addMethod(MethodSpec.methodBuilder("toString")
//                .addAnnotation(Override.class)
//                .addModifiers(Modifier.PUBLIC)
//                .returns(String.class)
//                .addStatement("return $S + $N()", "Chrome Debug Protocol ", version)
//                .build());
//
//        TypeSpec type = entrypoint.build();
//
//        JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, type).build();
//        if (outdir == null) {
//            javaFile.writeTo(System.out);
//        } else {
//            javaFile.writeTo(outdir);
//        }
//    }

    public static String uncap(String name) {
        Matcher m = pattern.matcher(name);
        if (m.matches()) {
            return m.group(1).toLowerCase() + m.group(2);
        } else {
            return name;
        }
    }

    public Domain domain(String domainName) {
        for (Domain domain : domains) {
            if (domainName.equals(domain.domain)) {
                return domain;
            }
        }
        throw new IllegalArgumentException("No such domain: " + domainName);
    }

    public Parameter ref(String domainName, String typeName) {
        for (Domain domain : domains) {
            if (domainName.equals(domain.domain)) {
                return domain.ref(typeName);
            }
        }
        throw new IllegalStateException("Unresolved $ref: missing domain " + domainName);
    }

    public void merge(Protocol other) {
        domains.addAll(other.domains);
    }

}
