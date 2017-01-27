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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;

/**
 *
 */
public class Domain {

    public String domain;

    public String description;

    public String javaName;

    public ClassName className;

    public List<Parameter> types;

    public List<Command> commands;

    public List<Command> events;

    TypeSpec.Builder builder;

    void init(AnnotationSpec generatedAnnotation) {
        javaName = domain + "Domain";
        className = ClassName.get(Codegen.PACKAGE, javaName);
        builder = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC).addAnnotation(generatedAnnotation);
        builder.addField(Codegen.CLIENT_CLASS, "protocolClient", Modifier.PRIVATE);
        builder.addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC)
                .addParameter(Codegen.CLIENT_CLASS, "protocolClient").addStatement("this.protocolClient = protocolClient").build());
    }

    public void buildType(Protocol protocol) {
        for (Command command : commands) {
            command.build(builder, protocol, this);
        }

        if (events != null) {
            for (Command event : events) {
                ClassName struct = Codegen.buildStruct(builder, Codegen.cap(event.name), event.description, event.parameters, protocol, this);
                {
                    MethodSpec.Builder methodSpec = MethodSpec.methodBuilder("on" + Codegen.cap(event.name))
                            .addModifiers(Modifier.PUBLIC)
                            .addParameter(ParameterizedTypeName.get(ClassName.get(Consumer.class), struct), "listener")
                            .addStatement("protocolClient.addEventListener($S, listener, $T.class)", domain + "." + event.name, struct);
                    if (event.description != null) {
                        methodSpec.addJavadoc(event.description.replace("$", "$$") + "\n");
                    }
                    builder.addMethod(methodSpec.build());
                }
                {
                    MethodSpec.Builder methodSpec = MethodSpec.methodBuilder("on" + Codegen.cap(event.name))
                            .addModifiers(Modifier.PUBLIC)
                            .returns(ParameterizedTypeName.get(ClassName.get(CompletableFuture.class), struct))
                            .addStatement("return protocolClient.eventFuture($S, $T.class)", domain + "." + event.name, struct);
                    if (event.description != null) {
                        methodSpec.addJavadoc(event.description.replace("$", "$$") + "\n");
                    }
                    builder.addMethod(methodSpec.build());
                }
            }
        }
    }

    public Parameter ref(String id) {
        for (Parameter t : types) {
            if (id.equals(t.id)) {
                return t;
            }
        }
        throw new IllegalStateException("Unresolved $ref: " + id + " in " + domain);
    }

}
