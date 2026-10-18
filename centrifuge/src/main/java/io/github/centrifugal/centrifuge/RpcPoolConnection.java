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
    // The token the connection presented at its last connect attempt, recorded when the source delivers it for that attempt
    // (an answer for an attempt the client has left is dropped by the client, and records nothing). A refusal names this token.
    // Written on the main client's executor, and read from the client's threads and the pool's.
    volatile String token = "";
    // The number of connect attempts the client has started: it moves on at each connecting event, on the client's thread.
    volatile int attempt;
    // The identity generation stamped at the connection's first delivery (-1 before it).
    volatile int generation = -1;
    // The pool takes the connection out of its endpoint: it takes no call, and closes once its calls settle.
    boolean retired;
    // The lost calls of the connection are settled, not sent again: the connection ended for a frame over its size limit, or
    // as unauthorized. The reason is the error of those calls.
    boolean noResend;
    String closeReason;


    RpcPoolConnection(RpcPool pool, RpcPoolEndpoint endpoint, Options options, TokenSource source) {
        this.endpoint = endpoint;
        this.client = new Client(endpoint.url(), clientOptions(pool, options, source), new ConnectionListener(pool));
    }


    // The options of a connection which takes its token from a source: a copy of the pool's options, with a token getter bound
    // to this connection. Without a source the connection shares the pool's options, which carry the token of the pool.
    private Options clientOptions(RpcPool pool, Options options, TokenSource source) {
        if (source == null) {
            return options;
        }
        Options copy = new Options();
        copy.setHeaders(options.getHeaders());
        copy.setName(options.getName());
        copy.setVersion(options.getVersion());
        copy.setTimeout(options.getTimeout());
        copy.setMinReconnectDelay(options.getMinReconnectDelay());
        copy.setMaxReconnectDelay(options.getMaxReconnectDelay());
        copy.setOkHttpClient(options.getOkHttpClient());
        copy.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                pool.tokenOf(RpcPoolConnection.this, cb);
            }
        });
        return copy;
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
            attempt++;
            pool.post(() -> pool.onConnecting(RpcPoolConnection.this));
        }


        @Override
        public void onConnected(Client c, ConnectedEvent event) {
            final String user = event.getUser();
            final String authClass = event.getAuthClass();
            pool.post(() -> pool.onConnected(RpcPoolConnection.this, user, authClass));
        }


        @Override
        public void onDisconnected(Client c, DisconnectedEvent event) {
            final String reason = event.getReason();
            final int code = event.getCode();
            pool.post(() -> pool.onDisconnected(RpcPoolConnection.this, code, reason));
        }
    }
}
