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

import java.io.IOException;

import brave.Tracer;
import brave.opentracing.BraveTracer;
import io.opentracing.util.GlobalTracer;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.Reporter;
import zipkin.reporter.Sender;
import zipkin.reporter.okhttp3.OkHttpSender;

/**
 *
 */
public class TracerFactory {

    private TracerFactory() {
    }

    public static final void init(String serviceName, String serverUri) {
        if (serverUri == null) {
            serverUri = "http://tracer:9411/api/v1/spans";
        }

        // Configure a reporter, which controls how often spans are sent
        //   (the dependency is io.zipkin.reporter:zipkin-sender-okhttp3)
        Sender sender = OkHttpSender.create(serverUri);
        AsyncReporter reporter = AsyncReporter.builder(sender).build();

        // Create a Zipkin tracer.
        Tracer tracer = Tracer.newBuilder()
                .localServiceName(serviceName)
                .reporter(reporter)
                .build();

        // Wrap the Zipkin tracer as an OpenTracing tracer and register it as a global tracer
        GlobalTracer.register(BraveTracer.wrap(tracer));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down tracer");
                    reporter.close();
                    try {
                        sender.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }

            });
    }

}
