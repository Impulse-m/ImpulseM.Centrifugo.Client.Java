package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;


/**
 * One endpoint of an {@link RpcPool}, with the connections the pool opened to it. Used on the pool's
 * thread only.
 */
final class RpcPoolEndpoint {
    private final String url;
    private final List<RpcPoolConnection> connections = new ArrayList<>();
    // The endpoint keys which resolved to this endpoint and have calls waiting: a connection event looks at the queues of
    // these keys only. A key joins when its queue gets its first call, and leaves when the queue empties.
    private final Set<String> queuedKeys = new LinkedHashSet<>();
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


    Set<String> queuedKeys() {
        return queuedKeys;
    }


    // Records that a key of this endpoint has its first call waiting.
    void addQueuedKey(String key) {
        queuedKeys.add(key);
    }


    // Records that the queue of a key of this endpoint is empty.
    void removeQueuedKey(String key) {
        queuedKeys.remove(key);
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
