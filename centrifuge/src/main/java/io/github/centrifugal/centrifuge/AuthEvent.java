package io.github.centrifugal.centrifuge;


/**
 * Authentication identity of a connection. Delivered to {@link EventListener#onAuth} on every
 * connect, and after a refresh when the user or the authentication class changed.
 */
public class AuthEvent {
    private final String user;
    private final String authClass;


    AuthEvent(String user, String authClass) {
        this.user = user;
        this.authClass = authClass;
    }


    /** User the connection is authenticated as. Empty when the server reports no user. */
    public String getUser() {
        return user;
    }


    /** Authentication class of the connection. Empty when the server reports no class. */
    public String getAuthClass() {
        return authClass;
    }
}
