package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;


/**
 * One endpoint of an {@link RpcPool}, with the connections the pool opened to it. Used on the pool's
 * thread only.
 */
final class RpcPoolEndpoint {
    private final String url;
    private final List<RpcPoolConnection> connections = new ArrayList<>();
    // The endpoint keys which resolved to this endpoint: a connection event looks at the queues of these keys only.
    private final List<String> keys = new ArrayList<>();
    // The connections the client closed for good since one of them connected: the count sets the next hold.
    int closes;
    // The timer which ends the hold of the opens, set while the opens are held back.
    ScheduledFuture<?> holdTask;


    RpcPoolEndpoint(String url) {
        this.url = url;
    }


    String url() {
        return url;
    }


    List<RpcPoolConnection> connections() {
        return connections;
    }


    List<String> keys() {
        return keys;
    }


    // Records a key which resolved to this endpoint, once: a key is resolved once until the pool is closed.
    void addKey(String key) {
        if (!keys.contains(key)) {
            keys.add(key);
        }
    }


    // Whether a connection of the endpoint is still connecting: opens are serialised per endpoint.
    boolean hasConnecting() {
        for (RpcPoolConnection connection : connections) {
            if (connection.connecting) {
                return true;
            }
        }
        return false;
    }


    // Whether the opens are held back after a connection the client closed for good.
    boolean isHeld() {
        return holdTask != null;
    }
}
