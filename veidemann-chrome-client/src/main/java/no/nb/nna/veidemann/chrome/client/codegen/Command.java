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

import com.google.gson.annotations.SerializedName;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolConfig;
import no.nb.nna.veidemann.chrome.client.ClientClosedException;
import no.nb.nna.veidemann.chrome.client.SessionClosedException;

import javax.lang.model.element.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static no.nb.nna.veidemann.chrome.client.codegen.EntryPoint.protocolClient;

/**
 *
 */
public class Command {

    public List<Parameter> parameters = Collections.emptyList();

    public List<Parameter> returns = Collections.emptyList();

    String name;

    String description;

    public void build(TypeSpec.Builder b, Protocol protocol, Domain domain) {
        MethodSpec.Builder methodSpec = MethodSpec.methodBuilder(name).addModifiers(Modifier.PUBLIC);
        StringBuilder javadoc = new StringBuilder();
        if (description != null) {
            javadoc.append(description.replace("$", "$$") + "\n<p>\n");
        }
        ClassName resultType;
        if (returns.isEmpty()) {
            resultType = ClassName.get(Void.class);
        } else {
            String responseClassDescription = "Response for " + Codegen.cap(name) + " request.";
            resultType = Codegen.buildImmutableResponse(b, name + "Response", responseClassDescription, returns, protocol, domain);
        }


        String returnTypeName = Codegen.cap(name) + "RequestBuilder";
        TypeName returnType = ClassName.get("", domain.javaName, returnTypeName);
        String builderClassDescription = "Builder for " + Codegen.cap(name) + " request.";
        TypeSpec.Builder returnClass = buildBuilderClass(returnTypeName, resultType, builderClassDescription, parameters, protocol, domain);
        b.addType(returnClass.build());

        methodSpec.returns(returnType);

        CodeBlock.Builder createCommandBuilder = CodeBlock.builder()
                .add("return new $T(", returnType)
                .add("$N.config", Session.entryPoint);
        for (Parameter param : parameters) {
            if (!param.optional) {
                TypeName type = param.typeName(protocol, domain);
                if (type != null) {
                    param.spec = ParameterSpec.builder(type, param.name).build();
                } else {
                    param.spec = ParameterSpec.builder(Object.class, param.name).build();
                }
                methodSpec.addParameter(param.spec);

                if (param.description != null) {
                    javadoc.append("@param ").append(param.name).append(" ").append(param.description.replace("$", "$$"))
                            .append("\n");
                }

                createCommandBuilder.add(", $N", param.name);
            }
        }
        methodSpec.addJavadoc(javadoc.toString())
                .addCode(createCommandBuilder.addStatement(")").build());

        b.addMethod(methodSpec.build());
    }

    private TypeSpec.Builder buildBuilderClass(String typeName, ClassName resultType, String description, List<Parameter> members, Protocol protocol, Domain domain) {
        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(typeName)
                .addModifiers(PUBLIC);

        StringBuilder fieldStrings = new StringBuilder();

        if (description != null) {
            typeSpec.addJavadoc(description.replace("$", "$$") + "\n");
        }

        // Constructor
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addParameter(ChromeDebugProtocolConfig.class, "cdpConfig", Modifier.FINAL)
                .addStatement("super($N)", "cdpConfig");

        for (Parameter member : members) {
            if (Objects.equals(member.name, "this")) {
                member.name = "this_";
            }
            FieldSpec.Builder field = FieldSpec.builder(member.typeName(protocol, domain), member.name, PRIVATE);
            if (member.name.equals("this_")) {
                field.addAnnotation(AnnotationSpec.builder(SerializedName.class)
                        .addMember("value", "$S", "this").build());
            }
            if (member.description != null) {
                field.addJavadoc(member.description.replace("$", "$$") + "\n");
            }

            FieldSpec fieldSpec = field.build();
            typeSpec.addField(fieldSpec);

            if (fieldStrings.length() > 0) {
                fieldStrings.append(", ");
            }
            fieldStrings.append(member.name + "=\" + " + member.name + " + \"");

            String memberDescription = member.description == null ? "" : member.description.replace("$", "$$") + "\n";

            if (member.optional) {
                // Add fluent withNNN methods for optional arguments
                typeSpec.addMethod(MethodSpec.methodBuilder("with" + Codegen.cap(member.name))
                        .addModifiers(PUBLIC)
                        .returns(ClassName.get("", typeName))
                        .addParameter(fieldSpec.type, fieldSpec.name, Modifier.FINAL)
                        .addStatement("this.$N = $N", fieldSpec, fieldSpec)
                        .addStatement("return this")
                        .addJavadoc(memberDescription + "\n")
                        .addJavadoc("@param " + member.name + " " + memberDescription)
                        .addJavadoc("@return this Builder for chaining\n")
                        .build());
            } else {
                // Add required arguments to constructor
                constructor.addParameter(fieldSpec.type, fieldSpec.name, Modifier.FINAL)
                        .addJavadoc("@param " + member.name + " " + memberDescription)
                        .addStatement("this.$1N = $1N", fieldSpec);
            }
        }

        typeSpec.superclass(ParameterizedTypeName.get(ClassName.get(Codegen.PACKAGE, "CommandBuilder"), resultType));
        typeSpec.addMethod(constructor.build());

        typeSpec.addMethod(buildRunAsync(resultType, protocol, domain));

        typeSpec.addMethod(MethodSpec.methodBuilder("toString")
                .addModifiers(PUBLIC)
                .returns(String.class)
                .addStatement("return \"" + typeName + "{" + fieldStrings + "}\"").build());

        return typeSpec;
    }

    private MethodSpec buildRunAsync(ClassName resultType, Protocol protocol, Domain domain) {
        TypeName returnType = ParameterizedTypeName.get(ClassName.get(CompletableFuture.class), resultType);
        MethodSpec.Builder methodSpec = MethodSpec.methodBuilder("runAsync").addModifiers(Modifier.PUBLIC);
        methodSpec.returns(returnType);

        methodSpec.addException(ClientClosedException.class)
                .addException(SessionClosedException.class)
                .beginControlFlow("if ($N.isClosed())", Session.entryPoint)
                .addStatement("$N.info(\"Accessing $T on closed client. {}\", $N.$N.getClosedReason())", domain.logger, domain.className, Session.entryPoint, protocolClient)
                .addStatement("throw new $T($N.$N.getClosedReason())", ClientClosedException.class, Session.entryPoint, protocolClient)
                .endControlFlow()
                .beginControlFlow("if ($N.isClosed())", domain.sessionClient)
                .addStatement("$N.info(\"Accessing $T on closed session. {}\", $N.getClosedReason())", domain.logger, domain.className, domain.sessionClient)
                .addStatement("throw new $T($N.getClosedReason())", SessionClosedException.class, domain.sessionClient)
                .endControlFlow();


        methodSpec.addJavadoc("Run request.\n<p>\n@return a {@link $T} from which the result might be retrieved\n", CompletableFuture.class);
        if (!parameters.isEmpty()) {
            methodSpec.addStatement("\n$T<String, Object> params = new $T<>()", Map.class, HashMap.class);
            for (Parameter param : parameters) {
                methodSpec.addStatement("params.put($S, $N)", param.name, param.name);
            }
        } else {
            methodSpec.addStatement("$T<String, Object> params = $T.EMPTY_MAP", Map.class, Collections.class);
        }

        methodSpec.addStatement("return $N.call($S, params, $T.class)",
                domain.sessionClient, domain.domain + "." + name, resultType);

        return methodSpec.build();
    }
}
