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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import no.nb.nna.broprox.db.CrawlLog;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.DbObjectFactory;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ProxyUtils;
import org.netpreserve.commons.uri.UriConfigs;

/**
 *
 */
public class RecorderFilter extends HttpFiltersAdapter {

    private final String uri;

    private final DbAdapter db;

    private final CrawlLog crawlLog;

    private final ContentInterceptor contentInterceptor;

    private final ContentWriterClient contentWriterClient;

    private long payloadSizeField;

    public RecorderFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx,
            final DbAdapter db, final ContentWriterClient contentWriterClient) {

        super(originalRequest.setUri(uri), ctx);
        this.uri = uri;
        this.db = db;
        this.contentWriterClient = contentWriterClient;

        this.crawlLog = DbObjectFactory.create(CrawlLog.class)
                .withRequestedUri(uri)
                .withSurt(UriConfigs.SURT_KEY.buildUri(uri).toString());
        this.contentInterceptor = new ContentInterceptor(db, ctx, contentWriterClient);
    }

    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) httpObject;
//            System.out.println(this.hashCode() + " :: PROXY URI: " + uri);
//            System.out.println("HEADERS: ");
//            for (Iterator<Map.Entry<CharSequence, CharSequence>> it = req.headers().iteratorCharSequence(); it.hasNext();) {
//                Map.Entry<CharSequence, CharSequence> e = it.next();
//                System.out.println("   " + e.getKey() + " = " + e.getValue());
//            }
            req.headers().set("Accept-Encoding", "identity");
            crawlLog.withFetchTimeStamp(OffsetDateTime.now().withOffsetSameInstant(ZoneOffset.UTC))
                    .withReferrer(req.headers().get("referer"))
                    .withDiscoveryPath(req.headers().get("Discovery-Path"));
        }

        return null;
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        if (httpObject instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) httpObject;
            crawlLog.withStatusCode(res.status().code())
                    .withContentType(res.headers().get("Content-Type"));
            contentInterceptor.addHeader(res.headers());

            try {
                payloadSizeField = res.headers().getInt("Content-Length");
            } catch (NullPointerException ex) {
                payloadSizeField = 0L;
            }

        } else if (httpObject instanceof HttpContent) {
            HttpContent res = (HttpContent) httpObject;
            contentInterceptor.addPayload(res.content());

            if (ProxyUtils.isLastChunk(httpObject)) {
                contentInterceptor.writeData(crawlLog);
            }
        } else {
            System.out.println(this.hashCode() + " :: RESP: " + httpObject.getClass());
        }

        return httpObject;
    }

}
