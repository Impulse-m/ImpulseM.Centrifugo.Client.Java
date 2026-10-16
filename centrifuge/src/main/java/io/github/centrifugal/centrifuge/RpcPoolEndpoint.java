package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;


/**
 * One endpoint of an {@link RpcPool}, with the connections the pool opened to it. Used on the pool's
 * thread only.
 */
final class RpcPoolEndpoint {
    private final String url;
    private final List<RpcPoolConnection> connections = new ArrayList<>();


    RpcPoolEndpoint(String url) {
        this.url = url;
    }


    String url() {
        return url;
    }


    List<RpcPoolConnection> connections() {
        return connections;
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
}
