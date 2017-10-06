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
package no.nb.nna.broprox.commons.opentracing;

import com.uber.jaeger.Configuration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

/**
 *
 */
public class TracerFactory {
    private static final String JAEGER_SERVICE_NAME = "JAEGER_SERVICE_NAME";
    private TracerFactory() {
    }

    public static final void init(String serviceName) {
        String envServiceName = System.getenv(JAEGER_SERVICE_NAME);
        if (envServiceName == null) {
            System.setProperty(JAEGER_SERVICE_NAME, serviceName);
        }
        Configuration config = Configuration.fromEnv();
        //config.setStatsFactory(...); // optional if you want to get metrics about tracer behavior

        Tracer tracer = config.getTracer();

        // Register tracer as a global tracer
        GlobalTracer.register(tracer);
    }

}
