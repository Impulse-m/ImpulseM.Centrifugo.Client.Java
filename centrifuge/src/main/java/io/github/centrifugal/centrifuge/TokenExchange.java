package io.github.centrifugal.centrifuge;


/**
 * Lets the client obtain and renew its connection token itself, over the connection, from a refresh
 * token the application persists. The application supplies three callbacks: {@link #load} reads the
 * refresh token, {@link #store} persists a rotated one, and {@link #exchange} trades the refresh token
 * for an access token. The access token becomes the connection token, and the client renews it before
 * it expires. Set with {@link Options#setTokenExchange}, which is exclusive with
 * {@link Options#setTokenGetter}.
 */
public abstract class TokenExchange {
    /**
     * The refresh token the application persisted, or null when there is none. Called on the client's
     * thread before every exchange, so it is never cached: keep it fast. Once the client is closed, it
     * may be called on the thread which completes the exchange instead.
     */
    public abstract String load();


    /**
     * Persists a rotated refresh token. Called on the client's thread before the access token issued
     * with it is used. Once the client is closed, it may be called on the thread which completes the
     * exchange instead. A failure is reported to the listener, and the exchange goes on.
     * <p>
     * A renewal which is in flight while the client stays connected stores its rotation when it completes,
     * even after the application cleared its stored refresh token. On logout, disconnect or close the client
     * before clearing the stored token.
     */
    public abstract void store(String refreshToken);


    /**
     * Exchanges the refresh token for an access token. The callback is called once, from any thread.
     * Fail it with {@link UnauthorizedException} when the issuer refused the refresh token: the session
     * is then dead. Any other failure is reported to the listener and retried with backoff.
     */
    public abstract void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb);
}
