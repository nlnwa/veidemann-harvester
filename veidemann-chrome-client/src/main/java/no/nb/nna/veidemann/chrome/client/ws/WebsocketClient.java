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
package no.nb.nna.veidemann.chrome.client.ws;

import java.net.URI;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WebsocketClient {

    private static final Logger LOG = LoggerFactory.getLogger(WebsocketClient.class);

    /**
     * Must be large enough to carry snapshot of web page.
     */
    private static final int MAX_FRAME_PAYLOAD_LENGTH = 8 * 1024 * 1024;

    private Channel channel;

    private EventLoopGroup workerGroup;

    Throwable closeReason = null;

    private final WebSocketCallback callback;

    private final URI uri;

    private boolean connected = false;

    private int connectionAttempts = 0;

    private int maxConnectionAttempts = 10;

    public WebsocketClient(WebSocketCallback callback, URI uri) {
        this.callback = callback;
        this.uri = uri;
        while (!connected && connectionAttempts++ < maxConnectionAttempts) {
            try {
                connect();
                connected = true;
            } catch (Throwable t) {
                LOG.error("Connection failed: {}", t.toString());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private void connect() {
        String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
        final String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
        final int port;
        if (uri.getPort() == -1) {
            if ("ws".equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("wss".equalsIgnoreCase(scheme)) {
                port = 443;
            } else {
                port = -1;
            }
        } else {
            port = uri.getPort();
        }

        if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
            System.err.println("Only WS(S) is supported.");
            return;
        }

        final boolean ssl = "wss".equalsIgnoreCase(scheme);
        final SslContext sslCtx;
        if (ssl) {
            try {
                sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } catch (SSLException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            sslCtx = null;
        }

        workerGroup = new NioEventLoopGroup();

        try {
            final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), MAX_FRAME_PAYLOAD_LENGTH);

            final ResponseHandler handler = new ResponseHandler();

            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline p = ch.pipeline();
                    if (sslCtx != null) {
                        p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                    }
                    p.addLast(
                            new HttpClientCodec(),
                            new HttpObjectAggregator(8192),
                            new WebSocketClientProtocolHandler(handshaker, false),
                            handler);
                }

            });

            channel = b.connect(uri.getHost(), port).sync().channel();
            channel.closeFuture().addListener(c -> {
                LOG.info("Closed {}", uri, closeReason);
                workerGroup.shutdownGracefully();
            });

            handler.handshakeFuture().sync();

        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

    }

    public void close() {
        if (!channel.isOpen()) {
            return;
        }
        channel.writeAndFlush(new CloseWebSocketFrame());
    }

    public void ping() {
        if (!channel.isActive()) {
            throw new IllegalStateException("closed", closeReason);
        }

        WebSocketFrame frame = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{8, 1, 8, 1}));
        channel.writeAndFlush(frame);
    }

    public void sendMessage(String msg) {
        int retryLimit = 5;
        int retryAttempts = 0;
        while (!channel.isActive() && retryAttempts < retryLimit) {
            LOG.info("WS channel closed, try to reopen.");
            close();
            connect();
            retryAttempts++;
        }

        if (!channel.isActive()) {
            throw new IllegalStateException("closed", closeReason);
        }

        WebSocketFrame frame = new TextWebSocketFrame(msg);
        channel.writeAndFlush(frame);
    }

    private class ResponseHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        private ChannelPromise handshakeFuture;

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                handshakeFuture.setSuccess();
                callback.onConnect();
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
            if (frame instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                callback.onMessageReceived(textFrame.text());
            } else if (frame instanceof CloseWebSocketFrame) {
                System.out.println("WebSocket Client received closing");
                channel.close();
                callback.onClose();
            } else {
                String message = "unsupported frame type: " + frame.getClass().getName();
                throw new UnsupportedOperationException(message);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            closeReason = cause;
            cause.printStackTrace();
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }

    }
}
