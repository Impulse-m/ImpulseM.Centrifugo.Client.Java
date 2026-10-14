package io.github.centrifugal.centrifuge;


/**
 * Result of {@link Client#refresh}: the identity the server reports for the connection after
 * the refresh.
 */
public class AuthResult {
    private final String user;
    private final String authClass;
    private final boolean expires;
    private final int ttl;


    AuthResult(String user, String authClass, boolean expires, int ttl) {
        this.user = user;
        this.authClass = authClass;
        this.expires = expires;
        this.ttl = ttl;
    }


    /** User the connection is authenticated as. Empty when the server reports no user. */
    public String getUser() {
        return user;
    }


    /** Authentication class of the connection. Empty when the server reports no class. */
    public String getAuthClass() {
        return authClass;
    }


    /** Whether the connection still expires; when true, {@link #getTtl()} is the time left in seconds. */
    public boolean isExpires() {
        return expires;
    }


    /** Seconds until the connection expires, zero when it does not expire. */
    public int getTtl() {
        return ttl;
    }
}
