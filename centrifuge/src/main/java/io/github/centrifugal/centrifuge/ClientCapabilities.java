package io.github.centrifugal.centrifuge;

/**
 * Capabilities the server confirmed for one connection. The object is an immutable
 * snapshot: call {@link Client#getCapabilities()} again after a reconnect.
 */
public final class ClientCapabilities {
    private final boolean rpcOptionsSupported;

    ClientCapabilities(boolean rpcOptionsSupported) {
        this.rpcOptionsSupported = rpcOptionsSupported;
    }

    /**
     * Whether the server accepts per-call RPC options (headers) on this connection.
     * False until a connect reply confirms it.
     */
    public boolean isRpcOptionsSupported() {
        return rpcOptionsSupported;
    }
}
