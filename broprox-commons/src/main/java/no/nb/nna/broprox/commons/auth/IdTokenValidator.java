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
package no.nb.nna.broprox.commons.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.oauth2.sdk.GeneralException;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

public class IdTokenValidator {
    private static final Logger LOG = LoggerFactory.getLogger(IdTokenValidator.class);
    public static final String CLIENT_ID = "veidemann-api";

    OIDCProviderMetadata providerMetadata;


    public IdTokenValidator(String issuerUrl) {
        providerMetadata = providerDiscovery(issuerUrl);
    }

    private OIDCProviderMetadata providerDiscovery(String issuerUrl) {
        try {
            Issuer issuer = new Issuer(issuerUrl);
            return OIDCProviderMetadata.resolve(issuer);
        } catch (IOException | GeneralException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Could not connect to IDP", ex);
        }
    }

    public JWTClaimsSet verifyIdToken(String idToken) {
        ConfigurableJWTProcessor jwtProcessor = new DefaultJWTProcessor();
        try {
            JWKSource keySource = new RemoteJWKSet(providerMetadata.getJWKSetURI().toURL());
            JWSAlgorithm expectedJWSAlg = JWSAlgorithm.RS256;

            JWSKeySelector keySelector = new JWSVerificationKeySelector(expectedJWSAlg, keySource);
            jwtProcessor.setJWSKeySelector(keySelector);

            // Process the token
            SecurityContext ctx = null; // optional context parameter, not required here
            JWTClaimsSet claims = jwtProcessor.process(idToken, ctx);
            List<String> audience = claims.getAudience();
            LOG.debug("Audience: {}", audience);
            if (!audience.contains(CLIENT_ID)) {
                LOG.info("Jwt has wrong audience: {}", audience);
                return null;
            }
            return claims;
        } catch (JOSEException | java.text.ParseException | MalformedURLException | BadJOSEException ex) {
            LOG.debug(ex.getMessage(), ex);
            return null;
        }
    }
}
