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
package no.nb.nna.veidemann.robotsservice;

/**
 * Main class for launching the service.
 */
public final class Main {

    /**
     * Private constructor to avoid instantiation.
     */
    private Main() {
    }

    /**
     * Start the server.
     * <p>
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // This class intentionally doesn't do anything except for instanciating a ResourceResolverServer.
        // This is necessary to be able to replace the LogManager. The system property must be set before any other
        // logging is even loaded.
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
        System.setProperty("log4j2.disable.jmx", "true");

        new RobotsServer().start();
    }

}
