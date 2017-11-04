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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
            javadoc.append(description.replace("$", "$$") + "\n");
        }
        ClassName resultType;
        if (returns.isEmpty()) {
            resultType = ClassName.get(Void.class);
        } else {
            resultType = Codegen.buildStruct(b, name, null, returns, protocol, domain);
        }
        TypeName returnType = ParameterizedTypeName.get(ClassName.get(CompletableFuture.class), resultType);
        methodSpec.returns(returnType);
        for (Parameter param : parameters) {
            TypeName type = param.typeName(protocol, domain);
            if (type != null) {
                param.spec = ParameterSpec.builder(type, param.name)
                        .addAnnotation(param.optional ? Nullable.class : NotNull.class).build();
            } else {
                param.spec = ParameterSpec.builder(Object.class, param.name).build();
            }
            methodSpec.addParameter(param.spec);
            if (param.description != null) {
                javadoc.append("@param ").append(param.name).append(" ").append(param.description.replace("$", "$$"))
                        .append("\n");
            }
        }
        methodSpec.addJavadoc(javadoc.toString());
        methodSpec.addStatement("$T<String,Object> params = new $T<>()", Map.class, HashMap.class);
        for (Parameter param : parameters) {
            methodSpec.addStatement("params.put($S, $N)", param.name, param.spec);
        }
        methodSpec.addStatement("return $N.call($S, params, $T.class)",
                domain.sessionClient, domain.domain + "." + name, resultType);
        b.addMethod(methodSpec.build());
    }

}
