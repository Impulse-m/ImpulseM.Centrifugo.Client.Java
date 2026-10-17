package io.github.centrifugal.centrifuge;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * The source of the connection tokens of the pools built with {@link RpcPool.Builder#tokenFrom(Client)}. One per main
 * client, created by its first pool and shared by all its pools. It lends the main client's current token to a pool
 * connection, and when a pool connection is refused a token the main client still holds, it starts one renewal of the
 * main client through the main client's own timer refresh. It has no timer and no poll: every answer is given on an
 * event of the main client, or at once.
 *
 * <p>The state is confined to the main client's executor, except the generation and the ended flag, which are volatile
 * so that the pools read them from their own thread.
 */
final class TokenSource {

    /** Receives the answer to a token request: the error, or the token with the identity generation it was made under. */
    interface Callback {
        void done(Throwable error, String token, int generation);
    }


    // A request which waits for the main client: the token the requester was refused last, and where its answer goes.
    private static final class Waiter {
        final String refused;
        final Callback callback;


        Waiter(String refused, Callback callback) {
            this.refused = refused;
            this.callback = callback;
        }
    }


    private final ClientHost host;
    private final AuthRefresh auth;
    // The waiting requests, one per requester. A new request of a requester replaces its waiting one, which fails as
    // unauthorized: the requester has given up the earlier attempt, and its callback must settle.
    private final Map<Object, Waiter> waiting = new LinkedHashMap<>();
    // The token a renewal was started for, so that one renewal serves every request for it. Cleared when the main client connects.
    private String renewedFor;
    // The identity the source last observed, and whether it has observed one since the session last ended.
    private String observedUser = "";
    private String observedAuthClass = "";
    private boolean observed;
    // Moves on when the identity of the main client changes, and when its session ends. Read by the pools from any thread.
    private volatile int generation;
    // The main client's session ended as unauthorized, and no connect attempt has started since. Read by the pools from any thread.
    private volatile boolean ended;


    TokenSource(ClientHost host, AuthRefresh auth) {
        this.host = host;
        this.auth = auth;
    }


    /**
     * Asks for a token for the requester, which was refused the token {@code refused} last (empty at its first request).
     * The callback runs on the main client's executor, once the answer is known. A request which cannot be answered yet
     * waits for the main client's next event.
     */
    void token(Object requester, String refused, Callback callback) {
        Extensions.submitOrRun(host.executor(), () -> {
            Waiter replaced = waiting.put(requester, new Waiter(refused, callback));
            if (replaced != null) {
                // The request it replaces is given up here, so that its callback settles and no connection waits on it.
                replaced.callback.done(new UnauthorizedException(), null, generation);
            }
            evaluate();
        }, () -> callback.done(new UnauthorizedException(), null, generation));
    }


    /** The identity generation: moves on when the identity of the main client changes, and when its session ends. Any thread. */
    int generation() {
        return generation;
    }


    /** Whether the main client's session ended as unauthorized and no connect attempt has started since. Any thread. */
    boolean ended() {
        return ended;
    }


    /** Drops the waiting request of a requester which left its pool. */
    void forget(Object requester) {
        Extensions.submitOrIgnore(host.executor(), () -> waiting.remove(requester));
    }


    // The main client attempts a connect: the session is alive again until the server says otherwise.
    void onConnectAttempt() {
        ended = false;
    }


    // The main client's identity was confirmed: a connect, or a refresh which was accepted.
    void onAuth(String user, String authClass, boolean always) {
        if (always) {
            renewedFor = null;
        }
        observe(user, authClass);
        if (!always) {
            refusedAgain();
        }
        evaluate();
    }


    // The main client disconnected. A terminal unauthorized disconnect ends the session: the generation moves on and the
    // identity is forgotten, so the next session records its identity without counting it. The client sets its state
    // after this hook, so the requests are evaluated against the state the disconnect leads to.
    void onDisconnect(int code, boolean terminal) {
        if (terminal && code == Client.DISCONNECTED_UNAUTHORIZED) {
            ended = true;
            generation++;
            observed = false;
        }
        evaluate(terminal ? ClientState.DISCONNECTED : ClientState.CONNECTING);
    }


    // Records the identity of the main client. The first identity records without counting; a different one counts once.
    private void observe(String user, String authClass) {
        if (!observed) {
            observedUser = user;
            observedAuthClass = authClass;
            observed = true;
            return;
        }
        if (!user.equals(observedUser) || !authClass.equals(observedAuthClass)) {
            observedUser = user;
            observedAuthClass = authClass;
            generation++;
        }
    }


    // A refresh accepted with the token a waiting request was refused gave that token back, which no request can use: those
    // requests fail as unauthorized now. Waiting for the next event would wait up to a whole token ttl.
    private void refusedAgain() {
        String token = host.connectionToken();
        if (token.isEmpty()) {
            return;
        }
        Iterator<Waiter> iterator = waiting.values().iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next();
            if (waiter.refused.equals(token)) {
                iterator.remove();
                fail(waiter);
            }
        }
    }


    private void evaluate() {
        evaluate(host.state());
    }


    // Answers the waiting requests which the state of the main client allows to answer now. A token is lent only while the
    // client is connected, so that its identity is confirmed. The others keep waiting, or fail when the client is gone.
    private void evaluate(ClientState state) {
        if (waiting.isEmpty()) {
            return;
        }
        String token = host.connectionToken();
        Iterator<Waiter> iterator = waiting.values().iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next();
            if (ended) {
                iterator.remove();
                fail(waiter);
            } else if (state == ClientState.CONNECTED) {
                if (token.isEmpty()) {
                    iterator.remove();
                    deliver(waiter, token);
                } else if (usable(token, waiter.refused)) {
                    iterator.remove();
                    deliver(waiter, token);
                } else if (!auth.canRenew()) {
                    iterator.remove();
                    fail(waiter);
                } else if (!token.equals(renewedFor)) {
                    renewedFor = token;
                    auth.renewNow();
                }
            } else if (state != ClientState.CONNECTING) {
                iterator.remove();
                fail(waiter);
            }
        }
    }


    // Whether the main client's token can be lent to a requester which was refused the given one: it is non-empty,
    // it is not the refused one, and the token exchange has not expired it.
    private boolean usable(String token, String refused) {
        return !token.isEmpty() && !token.equals(refused) && !auth.tokenExpired(token);
    }


    // Called only while the client is connected: the identity is observed before the token is lent with its generation.
    private void deliver(Waiter waiter, String token) {
        observe(auth.currentUser(), auth.currentAuthClass());
        waiter.callback.done(null, token, generation);
    }


    private void fail(Waiter waiter) {
        waiter.callback.done(new UnauthorizedException(), null, generation);
    }
}
