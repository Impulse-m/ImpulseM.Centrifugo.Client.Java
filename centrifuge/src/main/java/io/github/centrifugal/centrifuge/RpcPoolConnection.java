package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * One connection of an {@link RpcPool}: a plain {@link Client} without an instance, so that its calls
 * are plain requests. Its fields are the state of the pool's thread only: the client's listener hands
 * every event to the pool's thread, and reads nothing of them.
 */
final class RpcPoolConnection {
    final RpcPoolEndpoint endpoint;
    final Client client;
    // The calls on the wire, each holding one of the slots of the connection until its attempt answers.
    final Set<RpcPoolCall> calls = new HashSet<>();
    // The calls whose attempt the connection lost. They hold no slot, and wait for the next event of the connection,
    // which says whether they are sent again or settled.
    final List<RpcPoolCall> lost = new ArrayList<>();
    // True from the connect until the client is connected, and again while it reconnects.
    boolean connecting = true;
    // The pool has dropped the connection: no call goes to it, and it is closed.
    boolean removed;
    // When the connection last became idle (nanoTime), read by the pool's idle check; not meaningful while it has a call.
    long idleSince;
    // The server closed the connection for a frame over its size limit, with this reason: the lost calls are settled, not sent again.
    boolean sizeLimited;
    String closeReason;


    RpcPoolConnection(RpcPool pool, RpcPoolEndpoint endpoint, Options options) {
        this.endpoint = endpoint;
        this.client = new Client(endpoint.url(), options, new ConnectionListener(pool));
    }


    void connect() {
        client.connect();
    }


    /** Closes the client. Closes its transport, and releases its threads, without waiting. */
    void close() {
        try {
            client.close(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private final class ConnectionListener extends EventListener {
        private final RpcPool pool;


        ConnectionListener(RpcPool pool) {
            this.pool = pool;
        }


        @Override
        public void onConnecting(Client c, ConnectingEvent event) {
            pool.post(() -> pool.onConnecting(RpcPoolConnection.this));
        }


        @Override
        public void onConnected(Client c, ConnectedEvent event) {
            pool.post(() -> pool.onConnected(RpcPoolConnection.this));
        }


        @Override
        public void onDisconnected(Client c, DisconnectedEvent event) {
            final String reason = event.getReason();
            final int code = event.getCode();
            pool.post(() -> pool.onDisconnected(RpcPoolConnection.this, code, reason));
        }
    }
}
