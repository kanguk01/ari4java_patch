package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.tools.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * NettyWSClientHandler handles the transactions with the remote
 * WebSocket, forwarding to the client HttpResponseHandler interface.
 *
 * @author mwalton
 *
 */
@ChannelHandler.Sharable
public class NettyWSClientHandler extends NettyHttpClientHandler {

    private static final long UNTRACKED_CONNECTION = -1L;

    final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    final HttpResponseHandler wsCallback;
    private final WsClientAutoReconnect wsClient;
    private final long connectionGeneration;
    private final AtomicBoolean reconnectRequested = new AtomicBoolean(false);
    private volatile boolean shuttingDown = false;
    private final Logger logger = LoggerFactory.getLogger(NettyWSClientHandler.class);

    public NettyWSClientHandler(WebSocketClientHandshaker handshaker, HttpResponseHandler wsCallback, WsClientAutoReconnect wsClient) {
        this(handshaker, wsCallback, wsClient, UNTRACKED_CONNECTION);
    }

    public NettyWSClientHandler(WebSocketClientHandshaker handshaker, HttpResponseHandler wsCallback) {
        this(handshaker, wsCallback, null, UNTRACKED_CONNECTION);
    }

    NettyWSClientHandler(
            WebSocketClientHandshaker handshaker,
            HttpResponseHandler wsCallback,
            WsClientAutoReconnect wsClient,
            long connectionGeneration
    ) {
        this.handshaker = handshaker;
        this.wsCallback = wsCallback;
        this.wsClient = wsClient;
        this.connectionGeneration = connectionGeneration;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!shuttingDown && this.wsClient != null) {
            logger.debug("WS channel inactive: generation={}, context={}", connectionGeneration, ctx);
            try {
                wsCallback.onDisconnect();
            } finally {
                requestReconnect(new RestException("WS channel inactive"));
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.debug("Received Message - {}", msg.getClass().getSimpleName());
        Channel ch = ctx.channel();

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            responseBytes = new byte[response.content().readableBytes()];
            response.content().readBytes(responseBytes);
            HTTPLogger.traceResponse(response, responseBytes);
            if (!handshaker.isHandshakeComplete()) {
                logger.debug("Finish WS Handshake...");
                handshaker.finishHandshake(ch, response);
                handshakeFuture.trySuccess();
                return;
            }
            String error = "Unexpected FullHttpResponse (getStatus=" + response.status().toString() + ", content=" + getResponseText() + ')';
            logger.error(error);
            throw new ARIException(error);
        }

        // call this so we can set the last received time
        wsCallback.onResponseReceived();

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) msg;
            String text = textFrame.content().toString(ARIEncoder.ENCODING);
            HTTPLogger.traceWebSocketFrame(text);
            responseBytes = text.getBytes(ARIEncoder.ENCODING);
            wsCallback.onSuccess(text);
        } else if (msg instanceof CloseWebSocketFrame) {
            if (!shuttingDown) {
                if (this.wsClient != null) {
                    requestReconnect(new RestException("CloseWebSocketFrame received"));
                } else {
                    wsCallback.onDisconnect();
                }
            }
            ch.close();
        } else if (msg instanceof PongWebSocketFrame) {
            recordPong();
        } else {
            HTTPLogger.traceWebSocketFrame(msg.toString());
            String error = "Not expecting: " + msg.getClass().getSimpleName();
            logger.error(error);
            throw new ARIException(error);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (shuttingDown) {
            return;
        }

        logger.warn("WS exception: generation={}, cause={}", connectionGeneration, cause.getMessage(), cause);
        if (handshakeFuture != null && !handshakeFuture.isDone()) {
            handshakeFuture.tryFailure(cause);
        }
        if (wsClient != null) {
            requestReconnect(cause);
        } else {
            wsCallback.onFailure(cause);
        }
        ctx.close();
    }

    private void requestReconnect(Throwable cause) {
        if (!reconnectRequested.compareAndSet(false, true)) {
            logger.debug("WS reconnect already requested by handler: generation={}, cause={}",
                    connectionGeneration, cause.getMessage());
            return;
        }
        if (wsClient instanceof NettyWsConnectionLifecycle && connectionGeneration != UNTRACKED_CONNECTION) {
            ((NettyWsConnectionLifecycle) wsClient).reconnectWs(cause, connectionGeneration);
            return;
        }
        wsClient.reconnectWs(cause);
    }

    private void recordPong() {
        if (wsClient == null) {
            return;
        }
        if (wsClient instanceof NettyWsConnectionLifecycle && connectionGeneration != UNTRACKED_CONNECTION) {
            ((NettyWsConnectionLifecycle) wsClient).pong(connectionGeneration);
            return;
        }
        wsClient.pong();
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public void setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }
}
