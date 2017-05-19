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

package no.nb.nna.broprox.integrationtests;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.QueuedUri;
import no.nb.nna.broprox.commons.BroproxHeaderConstants;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;

/**
 *
 */
public class LoggingProxy extends HttpFiltersSourceAdapter implements BroproxHeaderConstants {

    private static final AttributeKey<String> CONNECTED_URL = AttributeKey.valueOf("connected_url");

    public LoggingProxy() {
        System.out.println("new instance " + hashCode());
    }

    @Override
    public HttpFilters filterRequest(HttpRequest originalRequest, ChannelHandlerContext clientCtx) {
        System.out.println("    instance " + hashCode());
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
            return new LoggingProxyFilter(uri, originalRequest, clientCtx);
        }
        originalRequest.setUri(uri);
        return new LoggingProxyFilter(connectedUrl + uri, originalRequest, clientCtx);
    }

public class LoggingProxyFilter extends HttpFiltersAdapter {

    private final String uri;

    private long payloadSizeField;

    public LoggingProxyFilter(final String uri, final HttpRequest originalRequest, final ChannelHandlerContext ctx) {

        super(originalRequest.setUri(uri), ctx);
        this.uri = uri;
    }

    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) httpObject;
            System.out.println(this.hashCode() + " :: PROXY URI: " + uri);
            System.out.println("HEADERS: ");
            for (Iterator<Map.Entry<CharSequence, CharSequence>> it = req.headers().iteratorCharSequence(); it.hasNext();) {
                Map.Entry<CharSequence, CharSequence> e = it.next();
                System.out.println("   " + e.getKey() + " = " + e.getValue());
                if (ALL_EXECUTION_IDS.equals(e.getKey().toString())) {
                    System.out.println("YYY");
                    List<QueuedUri.IdSeq> allExId = ProtoUtils.jsonListToProto(e.getValue().toString(), QueuedUri.IdSeq.class);
                    System.out.println("XXXX " + allExId);
                }
            }
//            req.headers().set("Accept-Encoding", "identity");
//            crawlLog.withFetchTimeStamp(OffsetDateTime.now(ZoneOffset.UTC))
//                    .withReferrer(req.headers().get("referer"))
//                    .withDiscoveryPath(req.headers().get("Discovery-Path"));
//
//            req.headers()
//                    .remove("referer")
//                    .remove("Discovery-Path")
//                    .remove("executionId");
        }

//        return null;
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
    }

    @Override
    public HttpObject serverToProxyResponse(HttpObject httpObject) {
        if (httpObject instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) httpObject;
//            crawlLog.withStatusCode(res.status().code())
//                    .withContentType(res.headers().get("Content-Type"));
//            contentInterceptor.addHeader(res.headers());

            try {
                payloadSizeField = res.headers().getInt("Content-Length");
            } catch (NullPointerException ex) {
                payloadSizeField = 0L;
            }

        } else if (httpObject instanceof HttpContent) {
            HttpContent res = (HttpContent) httpObject;
//            contentInterceptor.addPayload(res.content());
//
//            if (ProxyUtils.isLastChunk(httpObject)) {
//                contentInterceptor.writeData(crawlLog);
//            }
        } else {
            System.out.println(this.hashCode() + " :: RESP: " + httpObject.getClass());
        }

        return httpObject;
    }

}

}
