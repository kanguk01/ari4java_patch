package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.tools.HttpResponseHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class NettyWSClientHandlerTest {

    @Test
    void exceptionRequestsReconnectOnceWithoutPrematureFailureCallback() {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        NettyWsConnectionLifecycle lifecycle = mock(NettyWsConnectionLifecycle.class);
        NettyWSClientHandler handler = new NettyWSClientHandler(handshaker, callback, lifecycle, 7L);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        RuntimeException failure = new RuntimeException("socket failed");

        channel.pipeline().fireExceptionCaught(failure);
        channel.runPendingTasks();

        verify(lifecycle, times(1)).reconnectWs(failure, 7L);
        verify(callback, never()).onFailure(failure);
        verify(callback, times(1)).onDisconnect();
        assertFalse(channel.isOpen());
        channel.finishAndReleaseAll();
    }

    @Test
    void closeFrameAndChannelInactiveShareOneReconnectRequest() {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        NettyWsConnectionLifecycle lifecycle = mock(NettyWsConnectionLifecycle.class);
        NettyWSClientHandler handler = new NettyWSClientHandler(handshaker, callback, lifecycle, 11L);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        channel.writeInbound(new CloseWebSocketFrame());
        channel.runPendingTasks();

        verify(lifecycle, times(1)).reconnectWs(
                org.mockito.ArgumentMatchers.any(Throwable.class),
                org.mockito.ArgumentMatchers.eq(11L));
        verify(callback, times(1)).onDisconnect();
        channel.finishAndReleaseAll();
    }

    @Test
    void pongIsAttributedToOwningConnection() {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        NettyWsConnectionLifecycle lifecycle = mock(NettyWsConnectionLifecycle.class);
        NettyWSClientHandler handler = new NettyWSClientHandler(handshaker, callback, lifecycle, 13L);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        channel.writeInbound(new PongWebSocketFrame());

        verify(lifecycle, times(1)).pong(13L);
        verify(callback, times(1)).onResponseReceived();
        channel.finishAndReleaseAll();
    }

    @Test
    void intentionalShutdownDoesNotReconnectOrFailCallback() {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        HttpResponseHandler callback = mock(HttpResponseHandler.class);
        NettyWsConnectionLifecycle lifecycle = mock(NettyWsConnectionLifecycle.class);
        NettyWSClientHandler handler = new NettyWSClientHandler(handshaker, callback, lifecycle, 17L);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        handler.setShuttingDown(true);
        RuntimeException ignored = new RuntimeException("expected close");

        channel.pipeline().fireExceptionCaught(ignored);
        channel.close();
        channel.runPendingTasks();

        verify(lifecycle, never()).reconnectWs(
                org.mockito.ArgumentMatchers.any(Throwable.class),
                org.mockito.ArgumentMatchers.anyLong());
        verify(callback, never()).onFailure(ignored);
        verify(callback, never()).onDisconnect();
        channel.finishAndReleaseAll();
    }
}
