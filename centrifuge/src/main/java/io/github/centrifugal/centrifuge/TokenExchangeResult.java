package io.github.centrifugal.centrifuge;


/**
 * The outcome of a {@link TokenExchange#exchange} call: the access token, and the rotated refresh
 * token when the issuer rotated it.
 */
public final class TokenExchangeResult {
    private final String token;
    private final String refreshToken;
    private final int graceSeconds;


    /**
     * @param token:        the access token, which becomes the connection token.
     * @param refreshToken: the rotated refresh token, or null when it did not change.
     */
    public TokenExchangeResult(String token, String refreshToken) {
        this(token, refreshToken, -1);
    }


    /**
     * @param token:        the access token, which becomes the connection token.
     * @param refreshToken: the rotated refresh token, or null when there is nothing to store: it did not change, or
     *                      the exchange persisted the rotation itself under its own lock.
     * @param graceSeconds: how long the issuer keeps accepting the refresh token this exchange consumed, in seconds,
     *                      or -1 when the issuer publishes none. Above 300 it is clamped to 300.
     */
    public TokenExchangeResult(String token, String refreshToken, int graceSeconds) {
        this.token = token;
        this.refreshToken = refreshToken;
        this.graceSeconds = graceSeconds;
    }


    /** The access token: the connection token from now on. */
    public String getToken() {
        return token;
    }


    /** The rotated refresh token, or null when the issuer did not rotate it. */
    public String getRefreshToken() {
        return refreshToken;
    }


    /** The grace the issuer published with this exchange, in seconds, or -1 when it did not. */
    public int getGrace() {
        return graceSeconds;
    }
}
