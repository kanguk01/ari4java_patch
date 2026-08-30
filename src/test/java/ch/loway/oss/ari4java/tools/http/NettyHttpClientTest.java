package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.generated.actions.requests.AsteriskPingGetRequest;
import ch.loway.oss.ari4java.generated.ari_6_0_0.actions.requests.*;
import ch.loway.oss.ari4java.generated.models.AsteriskPing;
import ch.loway.oss.ari4java.tools.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class NettyHttpClientTest {

    private NettyHttpClient client;
    private ChannelFuture cf;

    private void setupTestClient(boolean init) throws URISyntaxException {
        client = new NettyHttpClient() {
            protected void initHttpBootstrap() {
                // for testing skip the bootstrapping
            }

            protected ChannelFuture httpConnect() {
                return cf;
            }
        };
        if (init) {
            client.initialize("http://localhost:8088/", "user", "p@ss");
        }
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.destroy();
        }
    }

    @Test
    public void testInitializeBadURL() throws URISyntaxException {
        setupTestClient(false);
        assertThrows(URISyntaxException.class, () ->
                client.initialize(":", "", ""));
    }

    @Test
    public void testInitializeInvalidURL() throws URISyntaxException {
        setupTestClient(false);
        assertThrows(IllegalArgumentException.class, () ->
                client.initialize("ws://localhost:8088/", "", ""));
    }

    @Test
    public void testBuildURL() throws Exception {
        setupTestClient(true);
        List<HttpParam> queryParams = new ArrayList<>();
        queryParams.add(HttpParam.build("a", "b/c"));
        queryParams.add(HttpParam.build("d", "e"));
        String url = client.buildURL("/channels", queryParams, false);
        assertEquals("/ari/channels?a=b%2Fc&d=e", url);
    }

    @Test
    public void testInitialize() throws Exception {
        NettyHttpClient client = new NettyHttpClient();
        client.initialize("http://localhost:8088/", "user", "p@ss");
        client.destroy();
        assertNotNull(client.baseUri);
    }

    @Test
    public void testHttpConnect() {
        Bootstrap bootstrap = mock(Bootstrap.class);
        NettyHttpClient client = new NettyHttpClient() {
            {
                initHttpBootstrap();
            }
            @Override
            protected void initHttpBootstrap() {
                httpBootstrap = bootstrap;
                try {
                    baseUri = new URI("http://localhost:8088/");
                } catch (URISyntaxException e) {
                    // oh well
                }
            }
        };
        when(bootstrap.connect(eq("localhost"), eq(8088))).thenReturn(mock(ChannelFuture.class));
        ChannelFuture future = client.httpConnect();
        assertNotNull(future, "Expected ChannelFuture");
        verify(bootstrap, times(1)).connect(anyString(), anyInt());
        client.destroy();
    }

    @Test
    public void testWsConnect() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        HttpResponseHandler callback = healthyCallback();

        WsClient.WsClientConnection connection = testClient.connect(callback, "/events", null);
        assertNotNull(connection, "Expected WsClientConnection");
        assertEquals(1, testClient.attempts.size());

        testClient.attempts.get(0).succeed();

        awaitCondition(testClient::isWsConnected);
        verify(callback, times(1)).onChReadyToWrite();
    }

    @Test
    public void staleHeartbeatMustNotReconnectReplacementConnection() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.pingPeriod = 500;
        testClient.pingTimeUnit = TimeUnit.MILLISECONDS;
        testClient.pingIdleThresholdMillis = 0L;
        testClient.pongTimeout = 80L;
        testClient.pongTimeoutTimeUnit = TimeUnit.MILLISECONDS;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;

        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        when(callback.getLastResponseTime()).thenReturn(0L);
        testClient.connect(callback, "/events", null);
        LifecycleTestClient.Attempt firstAttempt = testClient.attempts.get(0);
        firstAttempt.succeed();
        awaitCondition(testClient::isWsConnected);
        awaitPing(firstAttempt);

        long firstGeneration = testClient.currentWsGeneration();
        testClient.reconnectWs(new RestException("WS channel inactive"), firstGeneration);
        awaitCondition(() -> testClient.attempts.size() == 2);

        LifecycleTestClient.Attempt replacementAttempt = testClient.attempts.get(1);
        replacementAttempt.succeed();
        awaitCondition(testClient::isWsConnected);
        awaitPing(replacementAttempt);
        testClient.pong(testClient.currentWsGeneration());

        Thread.sleep(150L);
        assertEquals(2, testClient.attempts.size(),
                "A heartbeat owned by the previous connection must not reconnect the replacement connection");
        verify(callback, times(2)).onChReadyToWrite();
    }

    @Test
    public void noPongReconnectsCurrentConnectionOnce() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.pingPeriod = 500;
        testClient.pingTimeUnit = TimeUnit.MILLISECONDS;
        testClient.pingIdleThresholdMillis = 0L;
        testClient.pongTimeout = 30L;
        testClient.pongTimeoutTimeUnit = TimeUnit.MILLISECONDS;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;

        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        when(callback.getLastResponseTime()).thenReturn(0L);
        testClient.connect(callback, "/events", null);
        LifecycleTestClient.Attempt firstAttempt = testClient.attempts.get(0);
        firstAttempt.succeed();
        awaitPing(firstAttempt);

        awaitCondition(() -> testClient.attempts.size() == 2);
        assertEquals(2, testClient.attempts.size());
        verify(callback, times(1)).onDisconnect();
    }

    @Test
    public void overlappingReconnectSignalsScheduleOneReplacement() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.reconnectDelays = new long[]{40L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;

        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);
        testClient.attempts.get(0).succeed();
        awaitCondition(testClient::isWsConnected);
        long generation = testClient.currentWsGeneration();

        testClient.reconnectWs(new RestException("WS channel inactive"), generation);
        testClient.reconnectWs(new RestException("No Ping response from server"), generation);
        testClient.reconnectWs(new RestException("CloseWebSocketFrame received"), generation);

        awaitCondition(() -> testClient.attempts.size() == 2);
        Thread.sleep(80L);
        assertEquals(2, testClient.attempts.size());
        verify(callback, times(1)).onDisconnect();
    }

    @Test
    public void disconnectCallbackFailureDoesNotBlockReconnect() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();
        org.mockito.Mockito.doThrow(new IllegalStateException("listener failed"))
                .when(callback).onDisconnect();
        testClient.connect(callback, "/events", null);
        testClient.attempts.get(0).succeed();
        awaitCondition(testClient::isWsConnected);

        testClient.reconnectWs(
                new RestException("WS channel inactive"), testClient.currentWsGeneration());

        awaitCondition(() -> testClient.attempts.size() == 2);
        verify(callback, times(1)).onDisconnect();
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    @Test
    public void terminalReconnectFailureCallsBackOnce() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.setMaxReconnectCount(0);
        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);
        testClient.attempts.get(0).succeed();
        awaitCondition(testClient::isWsConnected);
        long generation = testClient.currentWsGeneration();

        RestException terminal = new RestException("terminal");
        testClient.reconnectWs(terminal, generation);
        testClient.reconnectWs(terminal, generation);

        awaitCondition(() -> !testClient.isWsConnected());
        verify(callback, times(1)).onFailure(terminal);
        assertEquals(1, testClient.attempts.size());
    }

    @Test
    public void failedReconnectAttemptsStopAtConfiguredLimit() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.setMaxReconnectCount(1);
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);

        RuntimeException firstFailure = new RuntimeException("first transport failure");
        testClient.attempts.get(0).failTransport(firstFailure);
        awaitCondition(() -> testClient.attempts.size() == 2);

        RuntimeException terminalFailure = new RuntimeException("replacement transport failure");
        testClient.attempts.get(1).failTransport(terminalFailure);

        awaitCondition(() -> !testClient.isWsConnected());
        verify(callback, times(1)).onFailure(terminalFailure);
        assertEquals(2, testClient.attempts.size());
    }

    @Test
    public void reconnectLimitRejectsValuesBelowInfiniteSentinel() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> testClient.setMaxReconnectCount(-2));

        assertEquals("Reconnect count must be -1 or greater", error.getMessage());
    }

    @Test
    public void transportFailureRetriesWithoutPrematureFailureCallback() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);

        testClient.attempts.get(0).failTransport(new RuntimeException("connect failed"));

        awaitCondition(() -> testClient.attempts.size() == 2);
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    @Test
    public void handshakeFailureRetriesWithoutPrematureFailureCallback() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);
        LifecycleTestClient.Attempt firstAttempt = testClient.attempts.get(0);

        firstAttempt.succeedTransport();
        firstAttempt.failHandshake(new RuntimeException("upgrade failed"));

        awaitCondition(() -> testClient.attempts.size() == 2);
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    @Test
    public void connectionTimeoutRetriesOnlyTheTimedOutGeneration() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.connectionTimeout = 30L;
        testClient.connectionTimeoutTimeUnit = TimeUnit.MILLISECONDS;
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();

        testClient.connect(callback, "/events", null);

        awaitCondition(() -> testClient.attempts.size() == 2);
        LifecycleTestClient.Attempt replacementAttempt = testClient.attempts.get(1);
        replacementAttempt.succeed();
        awaitCondition(testClient::isWsConnected);
        Thread.sleep(60L);
        assertEquals(2, testClient.attempts.size());
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    @Test
    public void successfulReconnectResetsRetryBudget() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        testClient.setMaxReconnectCount(1);
        testClient.reconnectDelays = new long[]{0L};
        testClient.reconnectDelayTimeUnit = TimeUnit.MILLISECONDS;
        HttpResponseHandler callback = healthyCallback();
        testClient.connect(callback, "/events", null);

        testClient.attempts.get(0).failTransport(new RuntimeException("first outage"));
        awaitCondition(() -> testClient.attempts.size() == 2);
        testClient.attempts.get(1).succeed();
        awaitCondition(testClient::isWsConnected);

        long recoveredGeneration = testClient.currentWsGeneration();
        testClient.reconnectWs(new RestException("second outage"), recoveredGeneration);

        awaitCondition(() -> testClient.attempts.size() == 3);
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    @Test
    public void intentionalDisconnectCancelsReconnectAndHeartbeatTasks() throws Exception {
        LifecycleTestClient testClient = new LifecycleTestClient();
        client = testClient;
        HttpResponseHandler callback = healthyCallback();
        WsClient.WsClientConnection connection = testClient.connect(callback, "/events", null);
        testClient.attempts.get(0).succeed();
        awaitCondition(testClient::isWsConnected);

        connection.disconnect();

        awaitCondition(() -> !testClient.isWsConnected());
        Thread.sleep(60L);
        assertEquals(1, testClient.attempts.size());
        verify(callback, never()).onFailure(any(Throwable.class));
    }

    private HttpResponseHandler healthyCallback() {
        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        when(callback.getLastResponseTime()).thenAnswer(invocation -> System.currentTimeMillis());
        return callback;
    }

    private void awaitPing(LifecycleTestClient.Attempt attempt) throws Exception {
        awaitCondition(() -> attempt.pingCount.get() > 0);
        assertEquals(PingWebSocketFrame.class, attempt.lastPingType.get());
    }

    private void awaitCondition(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met before timeout");
    }

    private static final class LifecycleTestClient extends NettyHttpClient {
        private final List<Attempt> attempts = new CopyOnWriteArrayList<>();

        private LifecycleTestClient() throws URISyntaxException {
            initialize("http://localhost:8088/", "user", "password");
        }

        @Override
        protected void initHttpBootstrap() {
            // HTTP is not used by WebSocket lifecycle tests.
        }

        @Override
        protected WsClientConnection connect(Bootstrap ignored, HttpResponseHandler callback) {
            Attempt attempt = new Attempt();
            attempts.add(attempt);
            ChannelHandlerContext handlerContext = mock(ChannelHandlerContext.class);
            when(handlerContext.newPromise()).thenReturn(attempt.handshakePromise);
            try {
                wsHandler.handlerAdded(handlerContext);
            } catch (Exception e) {
                throw new IllegalStateException("Could not initialize WebSocket handler", e);
            }

            Bootstrap bootstrap = mock(Bootstrap.class);
            when(bootstrap.connect(anyString(), anyInt())).thenReturn(attempt.transportPromise);
            return super.connect(bootstrap, callback);
        }

        @Override
        protected ChannelFuture writePing(Channel channel, WebSocketFrame frame) {
            for (Attempt attempt : attempts) {
                if (attempt.channel == channel) {
                    attempt.lastPingType.set(frame.getClass());
                    attempt.pingCount.incrementAndGet();
                    frame.release();
                    DefaultChannelPromise writePromise = new DefaultChannelPromise(channel);
                    writePromise.trySuccess();
                    return writePromise;
                }
            }
            throw new IllegalStateException("No WebSocket attempt owns the ping channel");
        }

        private static final class Attempt {
            private final EmbeddedChannel channel = new EmbeddedChannel();
            private final DefaultChannelPromise transportPromise = new DefaultChannelPromise(channel);
            private final DefaultChannelPromise handshakePromise = new DefaultChannelPromise(channel);
            private final AtomicInteger pingCount = new AtomicInteger();
            private final AtomicReference<Class<?>> lastPingType = new AtomicReference<>();

            private void succeed() {
                succeedTransport();
                handshakePromise.trySuccess();
                channel.runPendingTasks();
            }

            private void succeedTransport() {
                transportPromise.trySuccess();
                channel.runPendingTasks();
            }

            private void failTransport(Throwable cause) {
                transportPromise.tryFailure(cause);
                channel.runPendingTasks();
            }

            private void failHandshake(Throwable cause) {
                handshakePromise.tryFailure(cause);
                channel.runPendingTasks();
            }
        }
    }

    private void setupSync(NettyHttpClientHandler h) {
        cf = mock(ChannelFuture.class);
        when(cf.addListener(any())).thenReturn(cf);
        when(cf.syncUninterruptibly()).thenReturn(cf);
        Channel ch = mock(Channel.class);
        when(ch.closeFuture()).thenReturn(cf);
        when(cf.channel()).thenReturn(ch);
        ChannelPipeline p = mock(ChannelPipeline.class);
        when(p.get("http-handler")).thenReturn(h);
        when(ch.pipeline()).thenReturn(p);
    }

    @Test
    public void testHttpActionSync() throws Exception {
        setupTestClient(true);
        NettyHttpClientHandler h = new NettyHttpClientHandler();
        setupSync(h);
        h.responseStatus = HttpResponseStatus.OK;
        h.responseBytes = "testing".getBytes(ARIEncoder.ENCODING);
        String res = client.httpActionSync("", "GET", null, null, null);
        assertEquals("testing", res);
    }

    private EmbeddedChannel createTestChannel() {
        EmbeddedChannel channel = createTestChannel("http-handler", new NettyHttpClientHandler());
        cf = channel.closeFuture();
        ((DefaultChannelPromise) cf).setSuccess(null);
        return channel;
    }

    private EmbeddedChannel createTestChannel(String name, ChannelHandler handler) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast("http-codec", new HttpClientCodec());
        channel.pipeline().addLast("http-aggregator", new HttpObjectAggregator(NettyHttpClient.MAX_HTTP_REQUEST));
        channel.pipeline().addLast(name, handler);
        return channel;
    }

    private AsteriskPingGetRequest pingSetup(EmbeddedChannel channel) {
        channel.writeInbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(
                        "{\"ping\":\"pong\",\"timestamp\":\"2020-01-01T00:00:00.000+0000\",\"asterisk_id\":\"test_asterisk\"}",
                        ARIEncoder.ENCODING)));
        AsteriskPingGetRequest_impl_ari_6_0_0 req = new AsteriskPingGetRequest_impl_ari_6_0_0();
        req.setHttpClient(client);
        return req;
    }

    private void pingValidate(EmbeddedChannel channel, AsteriskPing res) {
        String data = ((ByteBuf) channel.readOutbound()).toString(ARIEncoder.ENCODING);
        String expecting = "GET /ari/asterisk/ping HTTP/1.1";
        assertTrue(data.startsWith(expecting), "HTTP Request Data does not start with " + expecting);
        assertTrue(data.contains("authorization: Basic dXNlcjpwQHNz"), "Expected HTTP Auth Header");
        assertEquals("pong", res.getPing());
        assertEquals("test_asterisk", res.getAsterisk_id());
    }

    @Test
    public void testHttpActionSyncPing() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        AsteriskPingGetRequest req = pingSetup(channel);
        AsteriskPing res = req.execute();
        pingValidate(channel, res);
    }

    @Test
    public void testHttpActionAsyncPing() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        AsteriskPingGetRequest req = pingSetup(channel);
        final boolean[] callback = {false};
        req.execute(new AriCallback<AsteriskPing>() {
            @Override
            public void onSuccess(AsteriskPing res) {
                pingValidate(channel, res);
                callback[0] = true;
            }

            @Override
            public void onFailure(RestException e) {
                fail(e.toString());
            }
        });
        channel.runPendingTasks();
        assertTrue(callback[0], "No onSuccess Callback");
    }

    @Test
    public void testHttpActionException() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        ApplicationsGetRequest_impl_ari_6_0_0 req = new ApplicationsGetRequest_impl_ari_6_0_0("test");
        req.setHttpClient(client);
        // when the response is JSON error then return the error from the server
        channel.writeInbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                Unpooled.copiedBuffer("{\"message\":\"a test error\"}", ARIEncoder.ENCODING)));
        boolean exception = false;
        try {
            req.execute();
        } catch (RestException e) {
            assertEquals("a test error", e.getMessage());
            exception = true;
        }
        assertTrue(exception, "Expecting an exception");
        // when the response is not JSON and there is an error definition from the API then return API definition
        channel.writeInbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                Unpooled.copiedBuffer("Not found", ARIEncoder.ENCODING)));
        exception = false;
        try {
            req.execute();
        } catch (RestException e) {
            assertEquals("Application does not exist.", e.getMessage());
            exception = true;
        }
        assertTrue(exception, "Expecting an exception");
    }

    @Test
    public void testBodyFieldSerialisation() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        channel.writeInbound(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer("[]", ARIEncoder.ENCODING)));
        AsteriskUpdateObjectPutRequest_impl_ari_6_0_0 req = new AsteriskUpdateObjectPutRequest_impl_ari_6_0_0(
                "cc", "ot", "id");
        req.setHttpClient(client);
        req.addFields("key1", "val1").addFields("key2", "val2").execute();
        validateBody(channel, "fields");
    }

    @Test
    public void testBodyVariableSerialisation() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        channel.writeInbound(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer("{}", ARIEncoder.ENCODING)));

        EndpointsSendMessagePutRequest_impl_ari_6_0_0 req = new EndpointsSendMessagePutRequest_impl_ari_6_0_0("to", "from");
        req.setHttpClient(client);
        req.addVariables("key1", "val1").addVariables("key2", "val2").execute();
        validateBody(channel, "variables");
    }

    @Test
    public void testBodyObjectSerialisation() throws Exception {
        setupTestClient(true);
        EmbeddedChannel channel = createTestChannel();
        channel.writeInbound(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer("{}", ARIEncoder.ENCODING)));

        Map<String, String> map = new HashMap<>();
        map.put("key1", "val1");
        map.put("key2", "val2");
        ApplicationsFilterPutRequest_impl_ari_6_0_0 req = new ApplicationsFilterPutRequest_impl_ari_6_0_0("app");
        req.setHttpClient(client);
        req.setFilter(map).execute();
        validateBody(channel, "filter");
    }

    private void validateBody(EmbeddedChannel channel, String field) {
        String expected = "{\"" + field + "\":{\"key1\":\"val1\",\"key2\":\"val2\"}}";
        if ("fields".equals(field)) {
            expected = "{\"fields\":[{\"attribute\":\"key1\",\"value\":\"val1\"},{\"attribute\":\"key2\",\"value\":\"val2\"}]}";
        }
        StringBuilder buffer = new StringBuilder();
        ByteBuf data = channel.readOutbound();
        while (data != null) {
            if (data.readableBytes() > 0) {
                buffer.append(data.toString(ARIEncoder.ENCODING));
            }
            data = channel.readOutbound();
        }
        String[] lines = buffer.toString().split("\n");
        assertEquals(expected, lines[lines.length - 1].trim());
    }

}
