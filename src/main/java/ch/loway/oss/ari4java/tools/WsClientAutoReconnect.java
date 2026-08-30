package ch.loway.oss.ari4java.tools;

/**
 * Interface to pluggable WebSocket reconnect implementation
 * 
 * @author grahambrown11
 *
 */
public interface WsClientAutoReconnect {
	void reconnectWs(Throwable cause);
	void pong();

	/**
	 * Configure how many consecutive reconnect attempts are allowed.
	 * Implementations that do not expose a configurable retry policy keep
	 * their existing behavior.
	 *
	 * @param count maximum attempts, or {@code -1} for no limit
	 */
	default void setMaxReconnectCount(int count) {
		// Optional capability for backwards-compatible client implementations.
	}
}
