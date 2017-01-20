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
package no.nb.nna.broprox.harvester.proxy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.littleshoot.proxy.mitm.Authority;

/**
 * Authority with constants for the self signed root certificate.
 */
public class SelfSignedAuthority extends Authority {

    private static final String ALIAS = "BroproxCA";

    private static final char[] PASSWORD = "MyReallySecretPassword".toCharArray();

    private static final String COMMON_NAME = "Broprox Web Traffic Recorder";

    private static final String ORGANIZATION = "Broprox";

    private static final String ORGANIZATIONAL_UNIT_NAME = "Certificate Authority";

    private static final String CERT_ORGANIZATION = "Broprox";

    private static final String CERT_ORGANIZATIONAL_UNIT_NAME = "Certificate Authority";

    /**
     * Create a self signed authority.
     * <p>
     * If a certificate is found in the certificate directory it is used, otherwise a new one is created.
     * <p>
     * @param certificateDir where to store the certificate
     * @throws IOException is thrown if certificate directory could not be created
     */
    public SelfSignedAuthority(File certificateDir) throws IOException {

        super(certificateDir, ALIAS, PASSWORD, COMMON_NAME, ORGANIZATION,
                ORGANIZATIONAL_UNIT_NAME, CERT_ORGANIZATION, CERT_ORGANIZATIONAL_UNIT_NAME);

        Files.createDirectories(certificateDir.toPath());
    }

}
