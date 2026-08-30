package ch.loway.oss.ari4java.tools.http;

import ch.loway.oss.ari4java.tools.WsClientAutoReconnect;

/**
 * Internal lifecycle contract that associates callbacks with the WebSocket
 * connection that produced them.
 */
interface NettyWsConnectionLifecycle extends WsClientAutoReconnect {

    void reconnectWs(Throwable cause, long connectionGeneration);

    void pong(long connectionGeneration);
}
