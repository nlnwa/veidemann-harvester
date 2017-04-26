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
package no.nb.nna.broprox.commons;


import io.opentracing.tag.Tags;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class OpenTracingWrapperTest {

    public OpenTracingWrapperTest() {
    }

    /**
     * Test of map method, of class OpenTracingWrapper.
     */
    @Test
    public void testRun() {
        String operationName = "Test";
        OpenTracingWrapper instance = new OpenTracingWrapper("myComponent", Tags.SPAN_KIND_CLIENT);
        Object expResult = null;
        Object result;

        result = instance.map(operationName, (i, j) -> {
            return "Hello " + i + " " + j;
        }, "World", "Earth");
        assertThat(result).isEqualTo("Hello World Earth");

        result = instance.map(operationName, i -> {
            return "Hello " + i;
        }, "World");
        assertThat(result).isEqualTo("Hello World");

        result = instance.map(operationName, this::methodWithParameterAndResult, "World");
        assertThat(result).isEqualTo("Hello World");

        result = instance.run(operationName, this::methodWithoutParameter);
        assertThat(result).isEqualTo("Hello nobody");

        instance.run(operationName, this::methodWithoutResult, "World");

        instance.run(operationName, this::methodWithoutParameterOrResult);
    }

    private String methodWithoutParameter() {
        return "Hello nobody";
    }

    private void methodWithoutResult(String s) {
    }

    private void methodWithoutParameterOrResult() {
    }

    private String methodWithParameterAndResult(String s) {
        return "Hello " + s;
    }

}
