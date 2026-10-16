package io.github.centrifugal.centrifuge;

import java.util.Objects;

import okhttp3.WebSocket;


/**
 * What a {@link TokenExchange#exchange} call gets: the refresh token it must exchange, and an rpc
 * function bound to the connection the exchange runs on.
 */
public final class TokenExchangeEvent {
    private final ClientHost host;
    private final WebSocket transport;
    private final String refreshToken;


    TokenExchangeEvent(ClientHost host, WebSocket transport, String refreshToken) {
        this.host = host;
        this.transport = transport;
        this.refreshToken = refreshToken;
    }


    /** The refresh token the application loaded for this exchange. */
    public String getRefreshToken() {
        return refreshToken;
    }


    /**
     * Sends a plain rpc call over the connection the exchange runs on, the one being set up or the
     * connected one. Callable from any thread; the callback runs on the client's executor, called once.
     * It carries no message id and is not retransmitted. It fails with IOException when that connection
     * is lost, and with a timeout when no reply comes within the client timeout.
     *
     * @param method: the rpc method name, not null.
     * @param data:   the request payload, not null.
     * @param cb:     called with the reply, or with the failure.
     * @throws NullPointerException when the method or the data is null.
     */
    public void rpc(final String method, final byte[] data, final ResultCallback<RPCResult> cb) {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(data, "data");
        host.runOrInline(() -> TokenExchangeFlow.sendRpc(host, transport, method, data, cb));
    }
}
