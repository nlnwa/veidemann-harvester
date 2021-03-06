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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;

import javax.annotation.Generated;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 *
 */
public class Protocol {

    List<Domain> domains;

    static final String INDENT = "    ";

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
            JavaFile javaFile = JavaFile.builder(Codegen.PACKAGE, domain.builder.build()).indent(INDENT).build();
            if (outdir == null) {
                javaFile.writeTo(System.out);
            } else {
                javaFile.writeTo(outdir);
            }
        }
        EntryPoint.generate(outdir);
        BrowserClient.generate(domains, outdir);
        PageSession.generate(domains, outdir);
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
