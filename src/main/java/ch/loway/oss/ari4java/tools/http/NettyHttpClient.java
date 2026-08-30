package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.tools.HttpResponse;
import ch.loway.oss.ari4java.tools.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP and WebSocket client implementation based on netty.io.
 * <p>
 * Threading is handled by NioEventLoopGroup, which selects on multiple
 * sockets and provides threads to handle the events on the sockets.
 * <p>
 * Requires netty-all-4.0.12.Final.jar
 *
 * @author mwalton
 */
public class NettyHttpClient implements HttpClient, WsClient, WsClientAutoReconnect, NettyWsConnectionLifecycle {

    public static final int CONNECTION_TIMEOUT_SEC = 10;
    public static final int READ_TIMEOUT_SEC = 30;
    public static final int MAX_HTTP_REQUEST = 16 * 1024 * 1024; // 16MB
    public static final int MAX_HTTP_BIN_REQUEST = 150 * 1024 * 1024; // 150MB

    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private static final String HTTP_CODEC = "http-codec";
    private static final String HTTP_AGGREGATOR = "http-aggregator";
    private static final String HTTP_HANDLER = "http-handler";
    private static final long[] DEFAULT_RECONNECT_DELAYS = {2L, 5L, 10L};
    private static final byte[] WS_PING_PAYLOAD = "ari4j".getBytes(ARIEncoder.ENCODING);

    private final Logger logger = LoggerFactory.getLogger(NettyHttpClient.class);
    private final Object wsLifecycleLock = new Object();
    private final AtomicLong wsGenerationSequence = new AtomicLong();

    protected Bootstrap httpBootstrap;
    protected URI baseUri;
    private EventLoopGroup group;
    private EventLoopGroup shutDownGroup;
    protected String auth;

    private HttpResponseHandler wsCallback;
    private String wsEventsUrl;
    private List<HttpParam> wsEventsParamQuery;
    private volatile WsClientConnection wsClientConnection;
    private int reconnectCount = 0;
    private int maxReconnectCount = 10; // -1 = infinite reconnect attempts
    private volatile WsConnectionContext wsContext;
    protected volatile NettyWSClientHandler wsHandler;
    protected volatile ChannelFutureListener wsFuture;
    private static SslContext sslContext;

    private volatile boolean destroyed;
    private static volatile boolean autoReconnect = true;
    protected int pingPeriod = 5;
    protected TimeUnit pingTimeUnit = TimeUnit.MINUTES;
    protected long pingIdleThresholdMillis = 15_000L;
    protected long pongTimeout = 10L;
    protected TimeUnit pongTimeoutTimeUnit = TimeUnit.SECONDS;
    protected long connectionTimeout = CONNECTION_TIMEOUT_SEC;
    protected TimeUnit connectionTimeoutTimeUnit = TimeUnit.SECONDS;
    protected long[] reconnectDelays = DEFAULT_RECONNECT_DELAYS.clone();
    protected TimeUnit reconnectDelayTimeUnit = TimeUnit.SECONDS;

    private static final class WsConnectionContext {
        private final long generation;
        private final NettyWSClientHandler handler;
        private volatile ChannelFuture channelFuture;
        private volatile ChannelFutureListener connectListener;
        private volatile ScheduledFuture<?> connectionTimeoutTask;
        private volatile ScheduledFuture<?> pingTask;
        private volatile ScheduledFuture<?> pongTimeoutTask;
        private volatile ScheduledFuture<?> reconnectTask;
        private volatile long lastPongAt;
        private volatile boolean handshakeComplete;
        private final AtomicBoolean disconnectNotified = new AtomicBoolean(false);
        private boolean terminalFailureNotified;

        private WsConnectionContext(long generation, NettyWSClientHandler handler) {
            this.generation = generation;
            this.handler = handler;
        }
    }

    public NettyHttpClient() {
        group = new NioEventLoopGroup();
        shutDownGroup = new NioEventLoopGroup();
    }

    public void initialize(String baseUrl, String username, String password) throws URISyntaxException {
        if (!baseUrl.endsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        logger.debug("initialize url: {}, user: {}", baseUrl, username);
        baseUri = new URI(baseUrl);
        String protocol = baseUri.getScheme();
        if (!HTTP.equalsIgnoreCase(protocol) && !HTTPS.equalsIgnoreCase(protocol)) {
            logger.warn("Not http(s), protocol: {}", protocol);
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        }
        this.auth = "Basic " + Base64.encode(Unpooled.copiedBuffer((username + ":" + password), ARIEncoder.ENCODING)).toString(ARIEncoder.ENCODING);
        initHttpBootstrap();
    }

    protected void initHttpBootstrap() {
        if (httpBootstrap == null) {
            // Bootstrap is the factory for HTTP connections
            logger.debug("Bootstrap with\n" +
                            " connection timeout: {},\n" +
                            " read timeout: {},\n" +
                            " aggregator max-length: {}",
                    CONNECTION_TIMEOUT_SEC,
                    READ_TIMEOUT_SEC,
                    MAX_HTTP_REQUEST);
            httpBootstrap = new Bootstrap();
            bootstrapOptions(httpBootstrap);
            httpBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    addSSLIfRequired(pipeline, baseUri);
                    pipeline.addLast("read-timeout", new ReadTimeoutHandler(READ_TIMEOUT_SEC));
                    pipeline.addLast(HTTP_CODEC, new HttpClientCodec());
                    pipeline.addLast(HTTP_AGGREGATOR, new HttpObjectAggregator(MAX_HTTP_REQUEST));
                    pipeline.addLast(HTTP_HANDLER, new NettyHttpClientHandler());
                }
            });
        }
    }

    private void bootstrapOptions(Bootstrap bootStrap) {
        bootStrap.group(group);
        bootStrap.channel(NioSocketChannel.class);
        bootStrap.option(ChannelOption.TCP_NODELAY, true);
        bootStrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootStrap.option(ChannelOption.SO_REUSEADDR, false);
        bootStrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECTION_TIMEOUT_SEC * 1000);
    }

    private static synchronized void addSSLIfRequired(ChannelPipeline pipeline, URI baseUri) throws SSLException {
        if (HTTPS.equalsIgnoreCase(baseUri.getScheme())) {
            if (sslContext == null) {
                sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            }
            pipeline.addLast("ssl", sslContext.newHandler(pipeline.channel().alloc()));
        }
    }

    private int getPort() {
        int port = baseUri.getPort();
        if (port == -1) {
            if (HTTP.equalsIgnoreCase(baseUri.getScheme())) {
                port = 80;
            } else if (HTTPS.equalsIgnoreCase(baseUri.getScheme())) {
                port = 443;
            }
        }
        return port;
    }

    protected ChannelFuture httpConnect() {
        logger.debug("HTTP Connect uri: {}", baseUri);
        return httpBootstrap.connect(baseUri.getHost(), getPort());
    }

    @Override
    public void destroy() {
        WsConnectionContext context;
        EventLoopGroup ioGroup;
        EventLoopGroup cleanupGroup;
        synchronized (wsLifecycleLock) {
            if (destroyed) {
                return;
            }
            destroyed = true;
            context = wsContext;
            clearCurrentContextLocked(context);
            wsClientConnection = null;
            ioGroup = group;
            cleanupGroup = shutDownGroup;
        }

        cancelContextTasks(context);
        closeContext(context, true);

        io.netty.util.concurrent.Future<?> ioShutdown = null;
        io.netty.util.concurrent.Future<?> cleanupShutdown = null;
        if (ioGroup != null && !ioGroup.isShuttingDown()) {
            ioShutdown = ioGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (cleanupGroup != null && !cleanupGroup.isShuttingDown()) {
            cleanupShutdown = cleanupGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (!isInEventLoop(ioGroup) && !isInEventLoop(cleanupGroup)) {
            if (ioShutdown != null) {
                ioShutdown.syncUninterruptibly();
            }
            if (cleanupShutdown != null) {
                cleanupShutdown.syncUninterruptibly();
            }
        }
        group = null;
        shutDownGroup = null;
        logger.debug("WebSocket client destroyed");
    }

    protected String buildURL(String path, List<HttpParam> parametersQuery, boolean withAddress) {
        StringBuilder uriBuilder = new StringBuilder();
        if (withAddress) {
            uriBuilder.append(baseUri);
        } else {
            uriBuilder.append(baseUri.getPath());
        }
        uriBuilder.append("ari");
        uriBuilder.append(path);
        boolean first = true;
        if (parametersQuery != null) {
            for (HttpParam hp : parametersQuery) {
                if (hp.getValue() != null && !hp.getValue().isEmpty()) {
                    if (first) {
                        uriBuilder.append("?");
                        first = false;
                    } else {
                        uriBuilder.append("&");
                    }
                    uriBuilder.append(hp.getName());
                    uriBuilder.append("=");
                    uriBuilder.append(ARIEncoder.encodeUrl(hp.getValue()));
                }
            }
        }
        return uriBuilder.toString();
    }

    // Factory for WS handshakes
    protected WebSocketClientHandshaker getWsHandshake(String path, List<HttpParam> parametersQuery) throws URISyntaxException {
        String url = buildURL(path, parametersQuery, true);
        if (url.regionMatches(true, 0, HTTP, 0, 4)) {
            // http(s):// -> ws(s)://
            url = "ws" + url.substring(4);
        }
        URI uri = new URI(url);
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.set(HttpHeaderNames.AUTHORIZATION, this.auth);
        return WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, false, headers);
    }

    // Build the HTTP request based on the given parameters
    private HttpRequest buildRequest(String path, String method, List<HttpParam> parametersQuery, String body) {
        String url = buildURL(path, parametersQuery, false);
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), url);
        if (body != null && !body.isEmpty()) {
            ByteBuf bbuf = Unpooled.copiedBuffer(body, ARIEncoder.ENCODING);
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            request.headers().set(HttpHeaderNames.CONTENT_LENGTH, bbuf.readableBytes());
            request.content().clear().writeBytes(bbuf);
        }
        request.headers().set(HttpHeaderNames.HOST, baseUri.getHost());
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        request.headers().set(HttpHeaderNames.AUTHORIZATION, this.auth);
        HTTPLogger.traceRequest(request, body);
        return request;
    }

    private RestException makeException(HttpResponseStatus status, String response, List<HttpResponse> errors) {

        if (status == null && response == null) {
            return new RestException("Client Shutdown");
        } else if (status == null) {
            return new RestException("Client Shutdown: " + response);
        }

        if (errors != null) {
            for (HttpResponse hr : errors) {
                if (hr.getCode() == status.code()) {
                    return new RestException(hr.getDescription(), response, status.code());
                }
            }
        }

        return new RestException(response, status.code());
    }

    // Synchronous HTTP action
    @Override
    public String httpActionSync(String uri, String method, List<HttpParam> parametersQuery,
                                 String body, List<HttpResponse> errors) throws RestException {
        NettyHttpClientHandler handler = httpActionSyncHandler(uri, method, parametersQuery, body, errors);
        return handler.getResponseText();
    }

    // Synchronous HTTP action
    @Override
    public byte[] httpActionSyncAsBytes(String uri, String method, List<HttpParam> parametersQuery,
                                        String body, List<HttpResponse> errors) throws RestException {
        NettyHttpClientHandler handler = httpActionSyncHandler(uri, method, parametersQuery, body, errors, true);
        return handler.getResponseBytes();
    }

    private NettyHttpClientHandler httpActionSyncHandler(String uri, String method, List<HttpParam> parametersQuery,
                                                         String body, List<HttpResponse> errors) throws RestException {
        return httpActionSyncHandler(uri, method, parametersQuery, body, errors, false);
    }

    private NettyHttpClientHandler httpActionSyncHandler(String uri, String method, List<HttpParam> parametersQuery,
                                                         String body, List<HttpResponse> errors, boolean binary) throws RestException {
        HttpRequest request = buildRequest(uri, method, parametersQuery, body);
        logger.debug("Sync Action - {} to {}", request.method(), request.uri());
        Channel ch = httpConnect().addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                logger.debug("HTTP connected");
                replaceAggregator(binary, future.channel());
            } else if (future.cause() != null) {
                logger.error("HTTP Connection Error - {}", future.cause().getMessage(), future.cause());
            } else {
                logger.error("HTTP Connection Error - Unknown");
            }
        }).syncUninterruptibly().channel();
        NettyHttpClientHandler handler = (NettyHttpClientHandler) ch.pipeline().get(HTTP_HANDLER);
        ch.writeAndFlush(request);
        ch.closeFuture().syncUninterruptibly();
        if (handler.getException() != null) {
            throw new RestException(handler.getException());
        } else if (httpResponseOkay(handler.getResponseStatus())) {
            return handler;
        } else {
            throw makeException(handler.getResponseStatus(), handler.getResponseText(), errors);
        }
    }

    private void replaceAggregator(boolean binary, Channel ch) {
        if (binary) {
            logger.debug("Is Binary, replace http-aggregator ...");
            ch.pipeline().replace(
                    HTTP_AGGREGATOR, HTTP_AGGREGATOR, new HttpObjectAggregator(MAX_HTTP_BIN_REQUEST));
        }
    }

    // Asynchronous HTTP action, response is passed to HttpResponseHandler
    @Override
    public void httpActionAsync(String uri, String method, List<HttpParam> parametersQuery,
                                String body, final List<HttpResponse> errors,
                                final HttpResponseHandler responseHandler, boolean binary) {

        final HttpRequest request = buildRequest(uri, method, parametersQuery, body);
        logger.debug("Async Action - {} to {}", request.method(), request.uri());
        // Get future channel
        ChannelFuture cf = httpConnect();
        cf.addListener((ChannelFutureListener) future1 -> {
            if (future1.isSuccess()) {
                logger.debug("HTTP connected");
                Channel ch = future1.channel();
                replaceAggregator(binary, ch);
                responseHandler.onChReadyToWrite();
                ch.writeAndFlush(request);
                ch.closeFuture().addListener((ChannelFutureListener) future2 -> {
                    responseHandler.onResponseReceived();
                    if (future2.isSuccess()) {
                        NettyHttpClientHandler handler = (NettyHttpClientHandler) future2.channel().pipeline().get(HTTP_HANDLER);
                        if (handler.getException() != null) {
                            responseHandler.onFailure(new RestException(handler.getException()));
                        } else if (httpResponseOkay(handler.getResponseStatus())) {
                            if (binary) {
                                responseHandler.onSuccess(handler.getResponseBytes());
                            } else {
                                responseHandler.onSuccess(handler.getResponseText());
                            }
                        } else {
                            responseHandler.onFailure(makeException(handler.getResponseStatus(), handler.getResponseText(), errors));
                        }
                    } else {
                        responseHandler.onFailure(future2.cause());
                    }
                });
            } else {
                responseHandler.onFailure(future1.cause());
            }
        });
    }
    // WsClient implementation - connect to WebSocket server

    @Override
    public WsClientConnection connect(final HttpResponseHandler callback, final String url, final List<HttpParam> lParamQuery) throws RestException {
        try {
            WsConnectionContext existing = wsContext;
            if (isLiveOrConnecting(existing)) {
                return createWsClientConnection();
            }

            WebSocketClientHandshaker handshake = getWsHandshake(url, lParamQuery);
            logger.debug("WS Connect uri: {}", handshake.uri());
            WsConnectionContext context;
            synchronized (wsLifecycleLock) {
                if (destroyed || group == null || group.isShuttingDown()) {
                    throw new RestException("WebSocket client is shut down");
                }
                existing = wsContext;
                if (isLiveOrConnecting(existing)) {
                    return createWsClientConnection();
                }

                long generation = wsGenerationSequence.incrementAndGet();
                NettyWSClientHandler handler = new NettyWSClientHandler(handshake, callback, this, generation);
                context = new WsConnectionContext(generation, handler);
                wsEventsUrl = url;
                wsEventsParamQuery = lParamQuery;
                wsCallback = callback;
                wsContext = context;
                wsHandler = handler;
            }
            return connect(new Bootstrap(), callback);
        } catch (Exception e) {
            if (e instanceof RestException) {
                throw (RestException) e;
            }
            throw new RestException("WS Connection Error - " + e.getMessage(), e);
        }
    }

    protected WsClientConnection connect(Bootstrap wsBootStrap, final HttpResponseHandler callback) {
        WsConnectionContext context = wsContext;
        if (context == null) {
            NettyWSClientHandler handler = wsHandler;
            if (handler == null) {
                throw new IllegalStateException("WebSocket handler must be configured before connecting");
            }
            synchronized (wsLifecycleLock) {
                context = new WsConnectionContext(wsGenerationSequence.incrementAndGet(), handler);
                wsContext = context;
            }
        }
        return connect(wsBootStrap, callback, context);
    }

    private WsClientConnection connect(
            Bootstrap wsBootStrap,
            final HttpResponseHandler callback,
            final WsConnectionContext context
    ) {
        bootstrapOptions(wsBootStrap);
        wsBootStrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                addSSLIfRequired(pipeline, baseUri);
                pipeline.addLast(HTTP_CODEC, new HttpClientCodec());
                pipeline.addLast(HTTP_AGGREGATOR, new HttpObjectAggregator(MAX_HTTP_REQUEST));
                pipeline.addLast("ws-handler", context.handler);
            }
        });

        ChannelFuture channelFuture;
        try {
            channelFuture = wsBootStrap.connect(baseUri.getHost(), getPort());
        } catch (RuntimeException e) {
            synchronized (wsLifecycleLock) {
                if (isCurrent(context)) {
                    context.handler.setShuttingDown(true);
                    clearCurrentContextLocked(context);
                }
            }
            cancelContextTasks(context);
            throw e;
        }
        context.channelFuture = channelFuture;
        context.connectionTimeoutTask = group.schedule(
                () -> handleConnectionTimeout(context), connectionTimeout, connectionTimeoutTimeUnit);
        ChannelFutureListener connectListener = future -> handleConnectResult(context, callback, future);
        context.connectListener = connectListener;
        wsFuture = connectListener;
        channelFuture.addListener(connectListener);

        // Provide disconnection handle to client
        return createWsClientConnection();
    }

    private void handleConnectResult(
            WsConnectionContext context,
            HttpResponseHandler callback,
            ChannelFuture future
    ) {
        if (!isCurrent(context)) {
            closeChannel(future.channel());
            return;
        }
        if (!future.isSuccess()) {
            cancelConnectionTimeout(context);
            Throwable cause = future.cause() != null
                    ? future.cause()
                    : new RestException("WS/HTTP Connection Error - Unknown");
            logger.warn("WS transport connection failed: generation={}, cause={}",
                    context.generation, cause.getMessage(), cause);
            requestReconnect(context, cause);
            return;
        }

        logger.debug("HTTP connected, waiting for WS Upgrade: generation={}", context.generation);
        ChannelFuture handshakeFuture = context.handler.handshakeFuture();
        if (handshakeFuture == null) {
            requestReconnect(context, new RestException("WS handshake promise was not initialized"));
            return;
        }
        handshakeFuture.addListener((ChannelFutureListener)
                future1 -> handleHandshakeResult(context, callback, future1));
    }

    private void handleHandshakeResult(
            WsConnectionContext context,
            HttpResponseHandler callback,
            ChannelFuture future
    ) {
        cancelConnectionTimeout(context);
        if (!isCurrent(context)) {
            closeChannel(future.channel());
            return;
        }
        if (!future.isSuccess()) {
            Throwable cause = future.cause() != null
                    ? future.cause()
                    : new RestException("WS Upgrade Error - Unknown");
            logger.warn("WS upgrade failed: generation={}, cause={}",
                    context.generation, cause.getMessage(), cause);
            requestReconnect(context, cause);
            return;
        }

        context.handshakeComplete = true;
        synchronized (wsLifecycleLock) {
            if (!isCurrent(context)) {
                return;
            }
            reconnectCount = 0;
            context.terminalFailureNotified = false;
        }
        startPing(context);
        logger.info("WS connected: generation={}", context.generation);
        callback.onChReadyToWrite();
    }

    private void handleConnectionTimeout(WsConnectionContext context) {
        if (!isCurrent(context) || context.handshakeComplete) {
            return;
        }
        requestReconnect(context, new RestException("WS Connect Timeout"));
    }

    private void cancelConnectionTimeout(WsConnectionContext context) {
        ScheduledFuture<?> timeoutTask = context.connectionTimeoutTask;
        context.connectionTimeoutTask = null;
        cancelFuture(timeoutTask);
    }

    private void startPing(WsConnectionContext context) {
        if (!isCurrent(context)) {
            return;
        }
        cancelFuture(context.pingTask);
        context.pingTask = group.scheduleAtFixedRate(
                () -> sendPingIfIdle(context), 1L, pingPeriod, pingTimeUnit);
    }

    private void sendPingIfIdle(WsConnectionContext context) {
        if (!isConnected(context)) {
            return;
        }
        ScheduledFuture<?> pendingPongTimeout = context.pongTimeoutTask;
        if (pendingPongTimeout != null && !pendingPongTimeout.isDone()) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - wsCallback.getLastResponseTime() <= pingIdleThresholdMillis) {
            return;
        }

        Channel channel = context.channelFuture.channel();
        long pingSentAt = now;
        logger.debug("WS ping sent: generation={}, timestamp={}", context.generation, pingSentAt);
        ChannelFuture writeFuture = writePing(channel,
                new PingWebSocketFrame(Unpooled.wrappedBuffer(WS_PING_PAYLOAD)));
        context.pongTimeoutTask = group.schedule(
                () -> handlePongTimeout(context, pingSentAt), pongTimeout, pongTimeoutTimeUnit);
        writeFuture.addListener(future -> {
            if (!future.isSuccess() && isCurrent(context)) {
                Throwable cause = future.cause() != null
                        ? future.cause()
                        : new RestException("WS Ping write failed");
                requestReconnect(context, cause);
            }
        });
    }

    protected ChannelFuture writePing(Channel channel, WebSocketFrame frame) {
        return channel.writeAndFlush(frame);
    }

    private void handlePongTimeout(WsConnectionContext context, long pingSentAt) {
        context.pongTimeoutTask = null;
        if (!isCurrent(context) || context.lastPongAt >= pingSentAt) {
            return;
        }
        requestReconnect(context, new RestException("No Ping response from server"));
    }

    private WsClientConnection createWsClientConnection() {
        WsClientConnection connection = wsClientConnection;
        if (connection == null) {
            synchronized (wsLifecycleLock) {
                connection = wsClientConnection;
                if (connection == null) {
                    connection = new WsClientConnection() {

                        @Override
                        public void disconnect() throws RestException {
                            disconnectCurrentConnection();
                        }
                    };
                    wsClientConnection = connection;
                }
            }
        }
        return connection;
    }

    /**
     * Checks if a response is okay.
     * All 2XX responses are supposed to be okay.
     *
     * @param status
     * @return whether it is a 2XX code or not (error!)
     */
    private boolean httpResponseOkay(HttpResponseStatus status) {
        return HttpResponseStatus.OK.equals(status)
                || HttpResponseStatus.NO_CONTENT.equals(status)
                || HttpResponseStatus.ACCEPTED.equals(status)
                || HttpResponseStatus.CREATED.equals(status);
    }

    @Override
    public void reconnectWs(Throwable cause) {
        requestReconnect(wsContext, cause);
    }

    @Override
    public void reconnectWs(Throwable cause, long connectionGeneration) {
        WsConnectionContext context = wsContext;
        if (context == null || context.generation != connectionGeneration) {
            logger.debug("Ignoring stale WS reconnect request: requestedGeneration={}, currentGeneration={}",
                    connectionGeneration, context == null ? null : context.generation);
            return;
        }
        requestReconnect(context, cause);
    }

    @Override
    public void pong() {
        WsConnectionContext context = wsContext;
        if (context != null) {
            pong(context.generation);
        }
    }

    @Override
    public void pong(long connectionGeneration) {
        WsConnectionContext context = wsContext;
        if (context == null || context.generation != connectionGeneration || !isCurrent(context)) {
            return;
        }
        context.lastPongAt = System.currentTimeMillis();
        ScheduledFuture<?> timeoutTask = context.pongTimeoutTask;
        context.pongTimeoutTask = null;
        cancelFuture(timeoutTask);
        logger.debug("WS pong received: generation={}, timestamp={}",
                context.generation, context.lastPongAt);
    }

    @Override
    public void disconnected(long connectionGeneration) {
        WsConnectionContext context = wsContext;
        if (context == null || context.generation != connectionGeneration) {
            return;
        }
        notifyDisconnected(context);
    }

    /**
     * The ability to turn on/off the websocket auto reconnect, defaulted to on
     *
     * @param val auto reconnect
     */
    public static void setAutoReconnect(boolean val) {
        NettyHttpClient.autoReconnect = val;
    }

    /**
     * The ability to provide a custom SSL Contect for
     *
     * @param sslContext the ssl context
     */
    public static void setSslContext(SslContext sslContext) {
        NettyHttpClient.sslContext = sslContext;
    }

    /**
     * Checks if websocket is connected
     *
     * @return true when connected, false otherwise
     */
    public boolean isWsConnected() {
        return isConnected(wsContext);
    }

    long currentWsGeneration() {
        WsConnectionContext context = wsContext;
        return context == null ? -1L : context.generation;
    }

    /**
     * Sets maximal reconnect count
     *
     * @param count max number of reconnect attempts, -1 for infinite reconnecting
     */
    @Override
    public void setMaxReconnectCount(int count) {
        if (count < -1) {
            throw new IllegalArgumentException("Reconnect count must be -1 or greater");
        }
        synchronized (wsLifecycleLock) {
            maxReconnectCount = count;
        }
    }

    private void requestReconnect(WsConnectionContext context, Throwable cause) {
        if (context == null || cause == null) {
            return;
        }

        notifyDisconnected(context);

        int attempt = 0;
        long reconnectDelay = 0L;
        boolean terminalFailure = false;
        HttpResponseHandler failureCallback = null;
        synchronized (wsLifecycleLock) {
            if (!isCurrent(context)) {
                logger.debug("Ignoring stale WS reconnect cause: generation={}, cause={}",
                        context.generation, cause.getMessage());
                return;
            }

            cancelHeartbeatTasks(context);
            cancelConnectionTimeout(context);
            if (context.reconnectTask != null && !context.reconnectTask.isDone()) {
                logger.debug("WS reconnect already scheduled: generation={}, cause={}",
                        context.generation, cause.getMessage());
                return;
            }

            if (!autoReconnect || (maxReconnectCount > -1 && reconnectCount >= maxReconnectCount)) {
                if (context.terminalFailureNotified) {
                    return;
                }
                context.terminalFailureNotified = true;
                context.handler.setShuttingDown(true);
                clearCurrentContextLocked(context);
                terminalFailure = true;
                failureCallback = wsCallback;
            } else {
                attempt = ++reconnectCount;
                long delayForAttempt = reconnectDelay(attempt);
                reconnectDelay = delayForAttempt;
                EventLoopGroup reconnectGroup = shutDownGroup;
                if (reconnectGroup == null || reconnectGroup.isShuttingDown()) {
                    context.terminalFailureNotified = true;
                    context.handler.setShuttingDown(true);
                    clearCurrentContextLocked(context);
                    terminalFailure = true;
                    failureCallback = wsCallback;
                } else {
                    context.reconnectTask = reconnectGroup.schedule(
                            () -> executeReconnect(context), delayForAttempt, reconnectDelayTimeUnit);
                }
            }
        }

        if (terminalFailure) {
            cancelContextTasks(context);
            closeContext(context, false);
            logger.error("WS reconnect exhausted: generation={}, attempts={}, cause={}",
                    context.generation, reconnectCount, cause.getMessage(), cause);
            if (failureCallback != null) {
                failureCallback.onFailure(cause);
            }
            return;
        }

        logger.warn("WS reconnect scheduled: generation={}, attempt={}, delay={}, delayUnit={}, cause={}",
                context.generation, attempt, reconnectDelay, reconnectDelayTimeUnit, cause.getMessage());
    }

    private void notifyDisconnected(WsConnectionContext context) {
        if (!isCurrent(context) || !context.handshakeComplete
                || !context.disconnectNotified.compareAndSet(false, true)) {
            return;
        }
        HttpResponseHandler callback = wsCallback;
        if (callback == null) {
            return;
        }
        try {
            callback.onDisconnect();
        } catch (RuntimeException e) {
            logger.warn("WS disconnect callback failed: generation={}, cause={}",
                    context.generation, e.getMessage(), e);
        }
    }

    private long reconnectDelay(int attempt) {
        if (reconnectDelays == null || reconnectDelays.length == 0) {
            return DEFAULT_RECONNECT_DELAYS[DEFAULT_RECONNECT_DELAYS.length - 1];
        }
        int index = Math.min(Math.max(attempt - 1, 0), reconnectDelays.length - 1);
        return reconnectDelays[index];
    }

    private void executeReconnect(WsConnectionContext context) {
        HttpResponseHandler callback;
        String eventsUrl;
        List<HttpParam> eventParameters;
        synchronized (wsLifecycleLock) {
            context.reconnectTask = null;
            if (!isCurrent(context)) {
                logger.debug("Skipping stale scheduled WS reconnect: generation={}", context.generation);
                return;
            }
            context.handler.setShuttingDown(true);
            clearCurrentContextLocked(context);
            callback = wsCallback;
            eventsUrl = wsEventsUrl;
            eventParameters = wsEventsParamQuery;
        }

        cancelContextTasks(context);
        closeContext(context, false);
        try {
            connect(callback, eventsUrl, eventParameters);
        } catch (RestException e) {
            logger.error("WS reconnect could not create a new connection: previousGeneration={}, cause={}",
                    context.generation, e.getMessage(), e);
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

    private void disconnectCurrentConnection() {
        WsConnectionContext context;
        synchronized (wsLifecycleLock) {
            context = wsContext;
            if (context == null) {
                return;
            }
            context.handler.setShuttingDown(true);
            clearCurrentContextLocked(context);
            reconnectCount = 0;
        }
        cancelContextTasks(context);
        closeContext(context, true);
    }

    private void cancelContextTasks(WsConnectionContext context) {
        if (context == null) {
            return;
        }
        cancelConnectionTimeout(context);
        cancelHeartbeatTasks(context);
        ScheduledFuture<?> reconnectTask = context.reconnectTask;
        context.reconnectTask = null;
        cancelFuture(reconnectTask);
    }

    private void cancelHeartbeatTasks(WsConnectionContext context) {
        ScheduledFuture<?> pingTask = context.pingTask;
        context.pingTask = null;
        cancelFuture(pingTask);
        ScheduledFuture<?> pongTimeoutTask = context.pongTimeoutTask;
        context.pongTimeoutTask = null;
        cancelFuture(pongTimeoutTask);
    }

    private void closeContext(WsConnectionContext context, boolean sendCloseFrame) {
        if (context == null) {
            return;
        }
        context.handler.setShuttingDown(true);
        ChannelFuture channelFuture = context.channelFuture;
        if (channelFuture == null) {
            return;
        }
        ChannelFutureListener connectListener = context.connectListener;
        if (connectListener != null) {
            channelFuture.removeListener(connectListener);
        }
        Channel channel = channelFuture.channel();
        if (channel != null) {
            if (sendCloseFrame && context.handshakeComplete && channel.isActive()) {
                channel.writeAndFlush(new CloseWebSocketFrame());
            }
            closeChannel(channel);
        }
        if (!channelFuture.isDone()) {
            channelFuture.cancel(false);
        }
    }

    private void closeChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private boolean isLiveOrConnecting(WsConnectionContext context) {
        return context != null && isCurrent(context) && !context.handler.isShuttingDown();
    }

    private boolean isConnected(WsConnectionContext context) {
        if (!isCurrent(context) || !context.handshakeComplete || context.handler.isShuttingDown()) {
            return false;
        }
        ChannelFuture channelFuture = context.channelFuture;
        return channelFuture != null
                && !channelFuture.isCancelled()
                && channelFuture.channel() != null
                && channelFuture.channel().isActive();
    }

    private boolean isCurrent(WsConnectionContext context) {
        return context != null && !destroyed && context == wsContext;
    }

    private void clearCurrentContextLocked(WsConnectionContext context) {
        if (context != null && wsContext != context) {
            return;
        }
        wsContext = null;
        wsHandler = null;
        wsFuture = null;
    }

    private void cancelFuture(ScheduledFuture<?> future) {
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
    }

    private boolean isInEventLoop(EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup == null) {
            return false;
        }
        for (io.netty.util.concurrent.EventExecutor executor : eventLoopGroup) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }
}
