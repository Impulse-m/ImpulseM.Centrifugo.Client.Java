package io.github.centrifugal.centrifuge;


/**
 * The outcome of a {@link TokenExchange#exchange} call: the access token, and the rotated refresh
 * token when the issuer rotated it.
 */
public final class TokenExchangeResult {
    private final String token;
    private final String refreshToken;


    /**
     * @param token:        the access token, which becomes the connection token.
     * @param refreshToken: the rotated refresh token, or null when it did not change.
     */
    public TokenExchangeResult(String token, String refreshToken) {
        this.token = token;
        this.refreshToken = refreshToken;
    }


    /** The access token: the connection token from now on. */
    public String getToken() {
        return token;
    }


    /** The rotated refresh token, or null when the issuer did not rotate it. */
    public String getRefreshToken() {
        return refreshToken;
    }
}
