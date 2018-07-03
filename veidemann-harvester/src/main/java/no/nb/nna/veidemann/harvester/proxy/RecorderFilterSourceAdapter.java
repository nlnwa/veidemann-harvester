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
package no.nb.nna.veidemann.harvester.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.harvester.BrowserSessionRegistry;
import org.littleshoot.proxy.HostResolver;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Filters source which extracts the uri from the request independently of http or https style before creating the
 * recorder filter.
 */
public class RecorderFilterSourceAdapter extends HttpFiltersSourceAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RecorderFilterSourceAdapter.class);

    private static final AttributeKey<String> CONNECTED_URL = AttributeKey.valueOf("connected_url");

    private final int proxyId;

    private final DbAdapter db;

    private final ContentWriterClient contentWriterClient;

    private final BrowserSessionRegistry sessionRegistry;

    private final HostResolver hostResolver;

    public RecorderFilterSourceAdapter(final int proxyId, final DbAdapter db, final ContentWriterClient contentWriterClient,
                                       final BrowserSessionRegistry sessionRegistry, final HostResolver hostResolver) {
        this.proxyId = proxyId;
        this.db = db;
        this.contentWriterClient = contentWriterClient;
        this.sessionRegistry = sessionRegistry;
        this.hostResolver = hostResolver;
    }

    @Override
    public HttpFilters filterRequest(HttpRequest originalRequest, ChannelHandlerContext clientCtx) {
        String uri = originalRequest.uri();

        MDC.put("uri", uri);
        LOG.trace("Method: {}, Client Context: {}", originalRequest.method(), clientCtx);

        if (originalRequest.method() == HttpMethod.CONNECT) {
            if (clientCtx != null) {
                String prefix = "https://" + uri.replaceFirst(":443$", "");
                clientCtx.channel().attr(CONNECTED_URL).set(prefix);
            }
            return HttpFiltersAdapter.NOOP_FILTER;
        }

        String connectedUrl = clientCtx.channel().attr(CONNECTED_URL).get();
        if (connectedUrl == null) {
            return new RecorderFilter(proxyId, uri, originalRequest, clientCtx,
                    db, contentWriterClient, sessionRegistry, hostResolver);
        }
        return new RecorderFilter(proxyId, connectedUrl + uri, originalRequest, clientCtx,
                db, contentWriterClient, sessionRegistry, hostResolver);
    }

}
