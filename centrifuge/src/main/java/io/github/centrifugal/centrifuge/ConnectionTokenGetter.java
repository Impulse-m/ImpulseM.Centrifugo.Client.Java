package io.github.centrifugal.centrifuge;

/**
 * The getter runs on the client's executor, so it must not block: a getter which waits for the network stalls every
 * other event of the client, and the timeout of the token request cannot interrupt it.
 */
public abstract class ConnectionTokenGetter {
    public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
        cb.Done(null, "");
    };
}
