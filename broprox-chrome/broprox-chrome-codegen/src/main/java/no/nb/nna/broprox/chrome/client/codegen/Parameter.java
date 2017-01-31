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
import java.util.Map;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

/**
 *
 */
public class Parameter {

    public String description;

    String id;

    String name;

    String type;

    Parameter items;

    String $ref;

    List<Parameter> properties;

    ClassName className;

    ParameterSpec spec;

    public boolean optional;

    public TypeName typeName(Protocol protocol, Domain domain) {
        if ($ref != null) {
            if ($ref.contains(".")) {
                String[] parts = $ref.split("\\.");
                Domain refDomain = protocol.domain(parts[0]);
                return refDomain.ref(parts[1]).typeName(protocol, refDomain);
            } else {
                return domain.ref($ref).typeName(protocol, domain);
            }
        }
        if (type == null) {
            return TypeName.get(Object.class);
        }
        switch (type) {
            case "string":
                return TypeName.get(String.class);
            case "integer":
                return TypeName.get(Integer.class);
            case "boolean":
                return TypeName.get(Boolean.class);
            case "number":
                return TypeName.get(Double.class);
            case "object":
                if (properties == null) {
                    return ParameterizedTypeName.get(Map.class, String.class, Object.class);
                }
                if (className == null) {
                    String typeName = Codegen.cap(Codegen.coalesce(id, name, "anonymous"));
                    className = ClassName.get(Codegen.PACKAGE, domain.javaName, typeName);
                    Codegen.buildStruct(domain.builder, typeName, description, properties, protocol, domain);
                }
                return className;
            case "array":
                return ParameterizedTypeName.get(ClassName.get(List.class), items.typeName(protocol, domain));
            default:
                return TypeName.get(Object.class);
        }
    }

}
