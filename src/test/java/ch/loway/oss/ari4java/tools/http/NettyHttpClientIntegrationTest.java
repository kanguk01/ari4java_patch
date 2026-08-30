package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.tools.HttpParam;
import ch.loway.oss.ari4java.tools.HttpResponseHandler;
import ch.loway.oss.ari4java.tools.RestException;
import ch.loway.oss.ari4java.tools.WsClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.junit.jupiter.api.RepeatedTest;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyHttpClientIntegrationTest {

    @RepeatedTest(5)
    void realSocketHandshakeDisconnectAndReconnectKeepOneHealthyReplacement() throws Exception {
        LocalWebSocketServer server = new LocalWebSocketServer();
        NettyHttpClient client = new NettyHttpClient();
        try {
            server.start();
            client.initialize("http://127.0.0.1:" + server.port(), "ora", "local-secret");
            client.reconnectDelays = new long[]{20L};
            client.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
            client.pingPeriod = 50;
            client.pingTimeUnit = TimeUnit.MILLISECONDS;
            client.pingIdleThresholdMillis = 0L;
            client.pongTimeout = 300L;
            client.pongTimeoutTimeUnit = TimeUnit.MILLISECONDS;
            RecordingCallback callback = new RecordingCallback();

            WsClient.WsClientConnection connection = client.connect(
                    callback,
                    "/events",
                    Collections.singletonList(HttpParam.build("app", "ora-local"))
            );

            assertTrue(callback.connectedTwice.await(5L, TimeUnit.SECONDS),
                    "The real WebSocket replacement did not finish its handshake");
            assertTrue(callback.disconnectedOnce.await(2L, TimeUnit.SECONDS),
                    "The first socket close was not reported");
            Thread.sleep(450L);

            assertEquals(2, server.connectionCount.get(),
                    "Only the forced replacement connection should exist");
            assertEquals(2, callback.connectedCount.get());
            assertEquals(1, callback.disconnectedCount.get());
            assertEquals(0, callback.failureCount.get());
            assertTrue(client.isWsConnected());

            connection.disconnect();
            awaitDisconnected(client);
            assertFalse(client.isWsConnected());
        } finally {
            client.destroy();
            server.close();
        }
    }

    private void awaitDisconnected(NettyHttpClient client) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (client.isWsConnected() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
    }

    private static final class RecordingCallback implements HttpResponseHandler {
        private final AtomicInteger connectedCount = new AtomicInteger();
        private final AtomicInteger disconnectedCount = new AtomicInteger();
        private final AtomicInteger failureCount = new AtomicInteger();
        private final AtomicLong lastResponseTime = new AtomicLong(System.currentTimeMillis());
        private final CountDownLatch connectedTwice = new CountDownLatch(2);
        private final CountDownLatch disconnectedOnce = new CountDownLatch(1);

        @Override
        public void onChReadyToWrite() {
            connectedCount.incrementAndGet();
            lastResponseTime.set(System.currentTimeMillis());
            connectedTwice.countDown();
        }

        @Override
        public void onResponseReceived() {
            lastResponseTime.set(System.currentTimeMillis());
        }

        @Override
        public void onDisconnect() {
            disconnectedCount.incrementAndGet();
            disconnectedOnce.countDown();
        }

        @Override
        public void onSuccess(String response) {
            lastResponseTime.set(System.currentTimeMillis());
        }

        @Override
        public void onSuccess(byte[] response) {
            lastResponseTime.set(System.currentTimeMillis());
        }

        @Override
        public void onFailure(Throwable error) {
            failureCount.incrementAndGet();
        }

        @Override
        public long getLastResponseTime() {
            return lastResponseTime.get();
        }

        @Override
        public Class<?> getType() {
            return String.class;
        }
    }

    private static final class LocalWebSocketServer implements AutoCloseable {
        private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final AtomicInteger connectionCount = new AtomicInteger();
        private Channel serverChannel;

        private void start() throws InterruptedException {
            WebSocketServerProtocolConfig protocolConfig = WebSocketServerProtocolConfig.newBuilder()
                    .websocketPath("/ari/events")
                    .checkStartsWith(true)
                    .handleCloseFrames(true)
                    .dropPongFrames(false)
                    .build();
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(new HttpServerCodec());
                            channel.pipeline().addLast(new HttpObjectAggregator(65_536));
                            channel.pipeline().addLast(new WebSocketServerProtocolHandler(protocolConfig));
                            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext context, Object event)
                                        throws Exception {
                                    if (event == WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                                        int currentConnection = connectionCount.incrementAndGet();
                                        if (currentConnection == 1) {
                                            context.executor().schedule(
                                                    () -> {
                                                        context.close();
                                                    }, 100L, TimeUnit.MILLISECONDS);
                                        }
                                    }
                                    super.userEventTriggered(context, event);
                                }
                            });
                        }
                    });
            serverChannel = bootstrap.bind("127.0.0.1", 0).sync().channel();
        }

        private int port() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        @Override
        public void close() {
            if (serverChannel != null) {
                serverChannel.close().awaitUninterruptibly();
            }
            workerGroup.shutdownGracefully().awaitUninterruptibly();
            bossGroup.shutdownGracefully().awaitUninterruptibly();
        }
    }
}
