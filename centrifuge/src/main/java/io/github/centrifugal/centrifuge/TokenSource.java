package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The source of the connection tokens of the pools built with {@link RpcPool.Builder#tokenFrom(Client)}. One per main
 * client, created by its first pool and shared by all its pools. It lends the main client's confirmed token to a pool
 * connection, and when a pool connection is refused a token the main client still holds, it starts one renewal of the
 * main client through the main client's own timer refresh. It has no timer and no poll: every answer is given on an
 * event of the main client, or at once.
 *
 * <p>The state is confined to the main client's executor, except the phase, the confirmed and unusable tokens, the
 * identity stamp and the set of listening pools, which are volatile or concurrent so that the pools read them from
 * their own thread.
 */
final class TokenSource {

    /** Receives the answer to a token request: the error, or the token with the identity generation it was made under. */
    interface Callback {
        void done(Throwable error, String token, int generation);
    }


    /** The state of the main client as the pools read it. */
    enum Phase {
        /** Connected, and the identity of the connect was confirmed. */
        CONNECTED,
        /** Connecting: the first connect, a reconnect, or a connect before its identity is confirmed. */
        CONNECTING,
        /** Disconnected, and the session has not ended as unauthorized. */
        DISCONNECTED,
        /** Disconnected as unauthorized, and no connect attempt has started since. */
        ENDED,
    }


    /** The identity generation, and the identity recorded for it, published together so that a pool reads a consistent pair. */
    static final class Stamp {
        final int generation;
        // Whether an identity is recorded: it is not after the session ended as unauthorized, until the next confirmation.
        final boolean recorded;
        final String user;
        final String authClass;


        Stamp(int generation, boolean recorded, String user, String authClass) {
            this.generation = generation;
            this.recorded = recorded;
            this.user = user;
            this.authClass = authClass;
        }
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


    // The code of a refresh the server refused for its permissions: the main client does not retry it.
    private static final int PERMISSION_DENIED = 103;

    private final ClientHost host;
    private final AuthRefresh auth;
    // The waiting requests, one per requester. A new request of a requester replaces its waiting one, which fails as
    // unauthorized: the requester has given up the earlier attempt, and its callback must settle.
    private final Map<Object, Waiter> waiting = new LinkedHashMap<>();
    // The renewal started for a confirmed token, so that one renewal serves the requests refused it. Cleared at each connect.
    private String renewedFor;
    // The pools which have a call in a key line. A notification reaches only these, and each drains its waiting keys.
    private final Set<RpcPool> listening = ConcurrentHashMap.newKeySet();
    // The phase, the confirmed token and the unusable token are read by the pools from their own thread.
    private volatile Phase phase = Phase.DISCONNECTED;
    // The token in effect at the last confirmation (c). Null before the first.
    private volatile String confirmed;
    // The token a pool connection found unusable (u), cleared at each confirmation. Null when none.
    private volatile String unusable;
    // The token the fleet refused and no renewal can replace before the next confirmation (d). Null when none. Cleared at each
    // confirmation, unless that confirmation spends the new token again.
    private volatile String spent;
    private volatile Stamp stamp = new Stamp(0, false, "", "");
    // Whether the source took the state of the main client, on the executor. Until then the pools read the client's own state.
    private volatile boolean initialized;


    TokenSource(ClientHost host, AuthRefresh auth) {
        this.host = host;
        this.auth = auth;
    }


    /**
     * Takes the state of the main client when the source is made: a source made while the main client is connected takes the
     * confirmation it missed, without counting an identity change. Runs on the executor, before the first request of the source.
     */
    void initialize() {
        if (host.state() == ClientState.CONNECTED) {
            phase = Phase.CONNECTED;
            confirmed = auth.confirmedToken();
            stamp = new Stamp(stamp.generation, true, auth.currentUser(), auth.currentAuthClass());
        } else {
            phase = Phase.DISCONNECTED;
        }
        initialized = true;
        notifyListening();
    }


    /**
     * Asks for a token for the requester, which was refused the token {@code refused} last (empty at its first request).
     * The callback runs on the main client's executor, once the answer is known. A request which cannot be answered yet
     * waits for the main client's next event.
     */
    void token(Object requester, String refused, Callback callback) {
        Extensions.submitOrRun(host.executor(), () -> {
            Waiter replaced = waiting.remove(requester);
            if (replaced != null) {
                // The request it replaces is given up here: its callback settles, and the answer ends nothing on the connection.
                replaced.callback.done(new UnauthorizedException(), null, generation());
            }
            Waiter waiter = new Waiter(refused, callback);
            if (!answer(requester, waiter)) {
                waiting.put(requester, waiter);
            }
        }, () -> callback.done(new UnauthorizedException(), null, generation()));
    }


    /** The identity generation: moves on when the identity of the main client changes, and when its session ends. Any thread. */
    int generation() {
        return stamp.generation;
    }


    /** The stamp of the identity generation and the recorded identity. Any thread. */
    Stamp stamp() {
        return stamp;
    }


    /**
     * The state of the main client as the pools read it. The client's own state decides while it connects or is connected:
     * the client enters its connecting state before the source hears of the attempt. The source's phase decides the terminal
     * states, which it records before the client leaves its connected state. Any thread.
     */
    Phase mainState() {
        ClientState state = host.state();
        if (state == ClientState.CONNECTING) {
            return Phase.CONNECTING;
        }
        if (!initialized) {
            // The source is not taken yet: the client's state decides, and a connected client waits for the source to take it.
            return state == ClientState.CONNECTED ? Phase.CONNECTING : Phase.DISCONNECTED;
        }
        Phase current = phase;
        if (current == Phase.ENDED || current == Phase.DISCONNECTED) {
            return current == Phase.ENDED && state == ClientState.DISCONNECTED ? Phase.ENDED : Phase.DISCONNECTED;
        }
        if (state == ClientState.CONNECTED) {
            return current == Phase.CONNECTED ? Phase.CONNECTED : Phase.CONNECTING;
        }
        return Phase.DISCONNECTED;
    }


    /**
     * Whether a pool may open a connection now: the main client is connected with its identity confirmed, and the token it
     * confirmed is neither the one a connection found unusable nor the one the fleet refused for good. Any thread.
     */
    boolean canLend() {
        String token = confirmed;
        return mainState() == Phase.CONNECTED && token != null && !token.equals(unusable) && !token.equals(spent);
    }


    /**
     * Whether the source is spent: the fleet refused the confirmed token, and no renewal can replace it before the next
     * confirmation. A pool connection which has no live connection of its identity fails its calls then. Any thread.
     */
    boolean isSpent() {
        String token = confirmed;
        return token != null && token.equals(spent);
    }


    /** Whether the identity recorded for the current generation is the given one. Any thread. */
    boolean identityMatches(String user, String authClass) {
        Stamp current = stamp;
        return current.recorded && current.user.equals(user) && current.authClass.equals(authClass);
    }


    /**
     * Drops the waiting request of a requester which left its pool. The request's callback is answered as unauthorized,
     * so that the connection's token request settles and the connection ends, with the client ignoring the answer
     * when its socket was already replaced or closed.
     */
    void forget(Object requester) {
        Extensions.submitOrIgnore(host.executor(), () -> {
            Waiter waiter = waiting.remove(requester);
            if (waiter != null) {
                fail(waiter);
            }
        });
    }


    /** A pool connection connected with the token and the identity of another one: the token is unusable until the next confirmation. */
    void unusable(String token) {
        Extensions.submitOrIgnore(host.executor(), () -> unusable = token);
    }


    /** A pool with a call in a key line asks to be notified of the main client's events. Any thread. */
    void listen(RpcPool pool) {
        listening.add(pool);
    }


    /** A pool whose key lines are all empty, or which closed, stops being notified. Any thread. */
    void unlisten(RpcPool pool) {
        listening.remove(pool);
    }


    // The main client attempts a connect: the session is alive again until the server says otherwise.
    void onConnectAttempt() {
        phase = Phase.CONNECTING;
    }


    // The main client's identity was confirmed: a connect, or a refresh which was accepted. A connect confirms the
    // identity and clears the renewal; both clear the unusable token, and move the confirmed token on.
    void onAuth(String user, String authClass, boolean always) {
        String token = auth.confirmedToken();
        boolean unusableCleared = unusable != null;
        boolean spentBefore = spent != null;
        int before = stamp.generation;
        if (always) {
            renewedFor = null;
            phase = Phase.CONNECTED;
        }
        // A refresh accepted with a token the waiting requests were refused, or with the token the renewal was started for, gave
        // the refused token back: the source is spent. A connect, or a refresh which brings another token, clears it.
        String nextSpent = null;
        if (!always && token != null && !token.isEmpty()) {
            boolean brought = refusedAgain(token);
            if (brought || token.equals(renewedFor)) {
                nextSpent = token;
            }
        }
        confirmed = token;
        unusable = null;
        spent = nextSpent;
        observe(user, authClass);
        boolean changed = stamp.generation != before;
        evaluateAll();
        boolean spentChanged = (spent != null) != spentBefore;
        if (always || changed || unusableCleared || spentChanged) {
            notifyListening();
        }
    }


    // The main client disconnected. A terminal unauthorized disconnect ends the session: the generation moves on and the
    // identity is forgotten, so the next session records its identity without counting it. Any other terminal
    // disconnect fails the waiting requests. A transport loss keeps them: the client reconnects.
    void onDisconnect(int code, boolean terminal) {
        if (!terminal) {
            phase = Phase.CONNECTING;
            return;
        }
        if (code == Client.DISCONNECTED_UNAUTHORIZED) {
            phase = Phase.ENDED;
            Stamp current = stamp;
            stamp = new Stamp(current.generation + 1, false, "", "");
        } else {
            phase = Phase.DISCONNECTED;
        }
        failAll();
        notifyListening();
    }


    // A refresh of the main client failed. A refresh the client retries, or a timer refresh the client ends the session for,
    // changes nothing here. A refresh refused with 103 whose renewal is the one the requests wait for fails them all, since the
    // token they wait for is not coming. Any other failure of a refresh() call re-evaluates the waiters, which may start the renewal.
    void onRefreshFailed(Throwable error, boolean explicit) {
        boolean refused = error instanceof ReplyError && ((ReplyError) error).getCode() == PERMISSION_DENIED;
        if (!explicit && !refused) {
            return;
        }
        String token = confirmed;
        if (refused && token != null && token.equals(renewedFor)) {
            // The one renewal of the confirmed token was refused, and the main client does not retry: the token is spent.
            spent = token;
            failAll();
            notifyListening();
            return;
        }
        evaluateAll();
    }


    // Records the identity of the main client. The first identity records without counting; a different one counts once.
    private void observe(String user, String authClass) {
        Stamp current = stamp;
        if (!current.recorded) {
            stamp = new Stamp(current.generation, true, user, authClass);
            return;
        }
        if (!user.equals(current.user) || !authClass.equals(current.authClass)) {
            stamp = new Stamp(current.generation + 1, true, user, authClass);
        }
    }


    // A refresh accepted with the token a waiting request was refused gave that token back, which no request can use: those
    // requests fail as unauthorized now. Waiting for the next event would wait up to a whole token ttl. An empty token
    // refuses nothing: a request which was never refused has the empty token as its refused one.
    private boolean refusedAgain(String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        boolean failed = false;
        Iterator<Map.Entry<Object, Waiter>> iterator = waiting.entrySet().iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next().getValue();
            if (token.equals(waiter.refused)) {
                iterator.remove();
                fail(waiter);
                failed = true;
            }
        }
        return failed;
    }


    // Re-evaluates the waiting requests, on a copy of them: an answer may not change the walk.
    private void evaluateAll() {
        if (waiting.isEmpty()) {
            return;
        }
        List<Map.Entry<Object, Waiter>> entries = new ArrayList<>(waiting.entrySet());
        for (Map.Entry<Object, Waiter> entry : entries) {
            if (waiting.get(entry.getKey()) == entry.getValue() && answer(entry.getKey(), entry.getValue())) {
                waiting.remove(entry.getKey());
            }
        }
    }


    // Answers one request by the rules of the table, and reports whether it was answered (so that it leaves the waiting set).
    // The rules run in order: the main client's phase first, then the confirmed token, then the one renewal.
    private boolean answer(Object requester, Waiter waiter) {
        Phase state = mainState();
        if (state == Phase.ENDED || state == Phase.DISCONNECTED) {
            fail(waiter);
            return true;
        }
        if (state != Phase.CONNECTED) {
            return false;
        }
        String token = confirmed;
        if (usable(token, waiter.refused)) {
            deliver(waiter, token);
            return true;
        }
        // The token was found unusable and the fleet did not refuse it for good: the next confirmation resolves it.
        if (token != null && token.equals(unusable) && !token.equals(spent)) {
            return false;
        }
        if (token != null && token.isEmpty()) {
            deliver(waiter, token);
            return true;
        }
        boolean isSpent = token != null && token.equals(spent);
        if (isSpent || !auth.canRenew()) {
            // No renewal can replace the token, so the requester is answered now. A token refused here spends the source, and
            // the pools are told after the answer.
            fail(waiter);
            if (!isSpent && token != null) {
                spent = token;
                notifyListening();
            }
            return true;
        }
        if (!Objects.equals(token, renewedFor) && auth.renewNow()) {
            renewedFor = token;
        }
        return false;
    }


    // Whether the main client's token can be lent to a requester which was refused the given one: it is non-empty, it is
    // neither the refused one, the unusable one nor the spent one, and the token exchange has not expired it.
    private boolean usable(String token, String refused) {
        return token != null && !token.isEmpty() && !token.equals(refused) && !token.equals(unusable) && !token.equals(spent)
                && !auth.tokenExpired(token);
    }


    // Called only while the client is connected: the identity is observed before the token is lent with its generation.
    private void deliver(Waiter waiter, String token) {
        observe(auth.currentUser(), auth.currentAuthClass());
        waiter.callback.done(null, token, stamp.generation);
    }


    private void fail(Waiter waiter) {
        waiter.callback.done(new UnauthorizedException(), null, stamp.generation);
    }


    // Answers every waiting request as unauthorized.
    private void failAll() {
        if (waiting.isEmpty()) {
            return;
        }
        List<Waiter> all = new ArrayList<>(waiting.values());
        waiting.clear();
        for (Waiter waiter : all) {
            fail(waiter);
        }
    }


    // Tells the listening pools that the main client changed: each drains the key lines which have calls waiting.
    private void notifyListening() {
        for (RpcPool pool : listening) {
            pool.post(pool::onMainEvent);
        }
    }
}
