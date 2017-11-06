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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.squareup.javapoet.AnnotationSpec;
import javax.annotation.Generated;
import org.junit.Ignore;
import org.junit.Test;
//import static org.junit.Assert.*;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class ProtocolTest {

    String domainJson = "{\n"
            + "        \"domain\": \"Schema\",\n"
            + "        \"description\": \"Provides information about the protocol schema.\",\n"
            + "        \"types\": [\n"
            + "            {\n"
            + "                \"id\": \"Domain\",\n"
            + "                \"type\": \"object\",\n"
            + "                \"description\": \"Description of the protocol domain.\",\n"
            + "                \"exported\": true,\n"
            + "                \"properties\": [\n"
            + "                    { \"name\": \"name\", \"type\": \"string\", \"description\": \"Domain name.\" },\n"
            + "                    { \"name\": \"version\", \"type\": \"string\", \"description\": \"Domain version.\" }\n"
            + "                ]\n"
            + "            }\n"
            + "        ],\n"
            + "        \"commands\": [\n"
            + "            {\n"
            + "                \"name\": \"getDomains\",\n"
            + "                \"description\": \"Returns supported domains.\",\n"
            + "                \"handlers\": [\"browser\", \"renderer\"],\n"
            + "                \"returns\": [\n"
            + "                    { \"name\": \"domains\", \"type\": \"array\", \"items\": { \"$ref\": \"Domain\" }, \"description\": \"List of supported domains.\" }\n"
            + "                ]\n"
            + "            }\n"
            + "        ]\n"
            + "    }";

    @Test
    @Ignore
    public void testGenSession() throws IOException {
        List<Domain> domains = new ArrayList<>();
        Domain domain = Codegen.gson.fromJson(domainJson, Domain.class);
        domains.add(domain);

        AnnotationSpec generatedAnnotation = AnnotationSpec.builder(Generated.class)
                .addMember("value", "$S", Codegen.class.getCanonicalName()).addMember("date", "$S", Instant.now()
                        .toString()).build();
        for (Domain d : domains) {
            d.init(generatedAnnotation);
        }

        Session.generate(domains, null);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    @Test
    @Ignore
    public void testGenEntrypoint() throws IOException {
        List<Domain> domains = new ArrayList<>();
        Domain domain = Codegen.gson.fromJson(domainJson, Domain.class);
        domains.add(domain);

        AnnotationSpec generatedAnnotation = AnnotationSpec.builder(Generated.class)
                .addMember("value", "$S", Codegen.class.getCanonicalName()).addMember("date", "$S", Instant.now()
                        .toString()).build();
        for (Domain d : domains) {
            d.init(generatedAnnotation);
        }

        EntryPoint.generate(domains, null);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}
