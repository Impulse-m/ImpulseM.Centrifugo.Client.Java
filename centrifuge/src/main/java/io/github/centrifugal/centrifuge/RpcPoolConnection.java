package io.github.centrifugal.centrifuge;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;


/**
 * One connection of an {@link RpcPool}: a plain {@link Client} without an instance, so that its calls
 * are plain requests. Its fields are the state of the pool's thread, except {@link #sizeLimited}, which
 * the client's listener sets before it hands the disconnect to the pool.
 */
final class RpcPoolConnection {
    final RpcPoolEndpoint endpoint;
    final Client client;
    // The calls on the wire, each holding one of the slots of the connection until its attempt answers.
    final Set<RpcPoolCall> calls = new HashSet<>();
    // True from the connect until the client is connected, and again while it reconnects.
    boolean connecting = true;
    // The pool has dropped the connection: no call goes to it, and it is closed.
    boolean removed;
    // The idle close of the connection, when it has no call and the pool has an idle timeout.
    ScheduledFuture<?> idleTask;
    // The server closed the connection for a frame over its size limit. Set before the disconnect is posted, so
    // that an attempt which fails with the lost connection is not resent.
    volatile boolean sizeLimited;


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
            if (event.getCode() == Client.DISCONNECTED_MESSAGE_SIZE_LIMIT) {
                sizeLimited = true;
            }
            final String reason = event.getReason();
            final int code = event.getCode();
            pool.post(() -> pool.onDisconnected(RpcPoolConnection.this, code, reason));
        }
    }
}
