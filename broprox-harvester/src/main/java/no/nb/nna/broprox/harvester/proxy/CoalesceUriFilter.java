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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import no.nb.nna.broprox.db.DbAdapter;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;

/**
 * Filter which extracts the uri from the request independently of http or https style.
 */
public class CoalesceUriFilter extends HttpFiltersSourceAdapter {

    private static final AttributeKey<String> CONNECTED_URL = AttributeKey.valueOf("connected_url");

    private final DbAdapter db;

    private final ContentWriterClient contentWriterClient;

    private final AlreadyCrawledCache cache;

    public CoalesceUriFilter(final DbAdapter db, final ContentWriterClient contentWriterClient, final AlreadyCrawledCache cache) {
        this.db = db;
        this.contentWriterClient = contentWriterClient;
        this.cache = cache;
    }

    @Override
    public HttpFilters filterRequest(HttpRequest originalRequest, ChannelHandlerContext clientCtx) {
        String uri = originalRequest.uri();
        if (originalRequest.method() == HttpMethod.CONNECT) {
            if (clientCtx != null) {
                String prefix = "https://" + uri.replaceFirst(":443$", "");
                clientCtx.channel().attr(CONNECTED_URL).set(prefix);
            }
            return HttpFiltersAdapter.NOOP_FILTER;
        }
        String connectedUrl = clientCtx.channel().attr(CONNECTED_URL).get();
        if (connectedUrl == null) {
            return new RecorderFilter(uri, originalRequest, clientCtx, db, contentWriterClient, cache);
        }
        originalRequest.setUri(uri);
        return new RecorderFilter(connectedUrl + uri, originalRequest, clientCtx, db, contentWriterClient, cache);
    }

}
