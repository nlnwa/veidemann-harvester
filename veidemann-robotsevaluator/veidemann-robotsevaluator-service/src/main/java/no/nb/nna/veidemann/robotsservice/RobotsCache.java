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

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.cert.CertificateException;
import java.util.Objects;

import no.nb.nna.veidemann.robotsparser.RobotsTxt;
import no.nb.nna.veidemann.robotsparser.RobotsTxtParser;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.CacheEntry;
import org.cache2k.expiry.ExpiryPolicy;
import org.cache2k.integration.CacheLoader;
import org.netpreserve.commons.uri.Uri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static no.nb.nna.veidemann.commons.VeidemannHeaderConstants.COLLECTION_ID;
import static no.nb.nna.veidemann.commons.VeidemannHeaderConstants.EXECUTION_ID;
import static no.nb.nna.veidemann.commons.VeidemannHeaderConstants.JOB_EXECUTION_ID;

/**
 *
 */
public class RobotsCache {

    private static final Logger LOG = LoggerFactory.getLogger(RobotsCache.class);

    private final Cache<CacheKey, RobotsTxt> cache;

    private final RobotsTxtParser ROBOTS_TXT_PARSER = new RobotsTxtParser();

    private final OkHttpClient client;

    private static final RobotsTxt EMPTY_ROBOTS = new RobotsTxt();

    public RobotsCache(final String proxyHost, final int proxyPort) {
        client = getUnsafeOkHttpClient()
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)))
                .build();
        cache = new Cache2kBuilder<CacheKey, RobotsTxt>() {
        }
                .name("robotsCache")
                .entryCapacity(500000)
                .expiryPolicy(new ExpiryPolicy<CacheKey, RobotsTxt>() {
                    @Override
                    public long calculateExpiryTime(CacheKey key, RobotsTxt value,
                            long loadTime, CacheEntry<CacheKey, RobotsTxt> oldEntry) {
                        if (value == null) {
                            if (LOG.isErrorEnabled()) {
                                LOG.error("Loader returned null");
                            }
                            return NO_CACHE;
                        }
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Caching {}", key);
                        }
                        return loadTime + (1000L * key.ttlSeconds);
                    }

                })
                .loader(new CacheLoader<CacheKey, RobotsTxt>() {
                    @Override
                    public RobotsTxt load(CacheKey key) throws Exception {
                        String url = key.protocol + "://" + key.getDomain() + ":" + key.getPort() + "/robots.txt";

                        Request request = new Request.Builder()
                                .url(url)
                                .addHeader(EXECUTION_ID, key.executionId)
                                .addHeader(JOB_EXECUTION_ID, key.jobExecutionId)
                                .addHeader(COLLECTION_ID, key.collectionId)
                                .build();

                        try (Response response = client.newCall(request).execute();) {
                            if (response.isSuccessful()) {
                                LOG.debug("Found '{}'", url);
                                return ROBOTS_TXT_PARSER.parse(response.body().charStream());
                            } else {
                                LOG.debug("No '{}' found", url);
                            }
                        } catch (Exception e) {
                            LOG.debug("No '{}' found", url, e);
                        }
                        return EMPTY_ROBOTS;
                    }

                })
                .build();
    }

    public RobotsTxt get(final Uri uri, final int ttlSeconds, final String executionId, final String jobExecutionId, final String collectionId) {
        return cache.get(new CacheKey(uri, ttlSeconds, executionId, jobExecutionId, collectionId));
    }

    public static final class CacheKey {

        private final String protocol;

        private final String domain;

        private final int port;

        private final int ttlSeconds;

        private final String executionId;

        private final String jobExecutionId;

        private final String collectionId;

        public CacheKey(final Uri uri, final int ttlSeconds, final String executionId, final String jobExecutionId, final String collectionId) {
            this.protocol = uri.getScheme();
            this.domain = uri.getHost();
            this.port = uri.getDecodedPort();
            this.ttlSeconds = ttlSeconds;
            this.executionId = executionId;
            this.jobExecutionId = jobExecutionId;
            this.collectionId = collectionId;
        }

        public String getDomain() {
            return domain;
        }

        public String getProtocol() {
            return protocol;
        }

        public int getPort() {
            return port;
        }

        public int getTtlSeconds() {
            return ttlSeconds;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 73 * hash + Objects.hashCode(this.protocol);
            hash = 73 * hash + Objects.hashCode(this.domain);
            hash = 73 * hash + this.port;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final CacheKey other = (CacheKey) obj;
            if (this.port != other.port) {
                return false;
            }
            if (!Objects.equals(this.protocol, other.protocol)) {
                return false;
            }
            if (!Objects.equals(this.domain, other.domain)) {
                return false;
            }
            return true;
        }

    }

    private static OkHttpClient.Builder getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });

            return builder;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
