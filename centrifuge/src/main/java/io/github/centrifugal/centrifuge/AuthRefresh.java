package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;
import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;
import okhttp3.WebSocket;


/**
 * The refreshes of the connection token: the refresh a caller asks for with
 * {@link Client#refresh}, and the timer which refreshes before the token expires. One refresh is
 * in flight at a time, the others wait their turn. The rpc calls wait for the refresh command in
 * flight, see {@link RpcSession}. Used on the client's executor thread only, except the inline run of
 * {@link ClientHost#runOrInline} after the client closed: that run completes the refresh in flight
 * too, so it fails the waiting refreshes and flushes the rpc calls on the completing thread.
 * <p>
 * The fetch and retry loop of the timer mirrors {@code Subscription.sendRefresh}. It is kept here,
 * so that the subscription code stays as it is. With a {@link TokenExchange} the timer fetches its
 * token through the exchange, see {@link TokenExchangeFlow}.
 */
final class AuthRefresh {
    // A refresh which waits for the refresh in flight. A requested refresh carries its token and the
    // callbacks of the refresh calls which asked for it. A timer refresh carries neither: it fetches
    // its token when it starts, and the generation of the timer which queued it.
    private static final class Waiting {
        final String token;
        final List<ResultCallback<AuthResult>> callbacks;
        final long generation;


        Waiting(String token, List<ResultCallback<AuthResult>> callbacks, long generation) {
            this.token = token;
            this.callbacks = callbacks;
            this.generation = generation;
        }
    }


    // A refused transition keeps the connection and its identity, and is reported without a
    // retry. A token which expired is retried by the timer with a fresh token.
    private static final int PERMISSION_DENIED = 103;

    // Server error code of an expired connection token: the timer refresh retries it with a fresh token.
    static final int ERROR_CODE_TOKEN_EXPIRED = 109;

    // Bounds of the retry delay after a failed token fetch or refresh, in milliseconds.
    static final int RETRY_MIN_MILLIS = 10000;
    static final int RETRY_MAX_MILLIS = 20000;

    // Error codes below this one are client errors, which the timer retries with a fresh token.
    private static final int SERVER_ERROR_CODE_MIN = 100;

    // The refresh() calls in progress, the one in flight and the ones which wait for it, are bounded by this.
    private static final int MAX_IN_PROGRESS = 32;

    private final ClientHost host;
    // The bounds of the retry delay after a failed token fetch or refresh, in milliseconds.
    private final int retryMinMillis;
    private final int retryMaxMillis;
    private final Backoff backoff = new Backoff();
    // Null without a token exchange option: the connection token is then fetched by the token getter.
    private final TokenExchangeFlow flow;
    // The refresh in flight is the one which runs now. The refreshes waiting for it run in order.
    private boolean inFlight = false;
    // A refresh command awaits its reply. The rpc calls wait for it, and for a requested refresh which
    // has not sent its command yet. A timer refresh which is still fetching its token does not hold them.
    private boolean commandInFlight = false;
    // The token and the callbacks of the requested refresh in flight. Null when none is in flight.
    private String inFlightToken;
    private List<ResultCallback<AuthResult>> inFlightCallbacks;
    // The refresh() calls which have not called back yet, in flight or waiting. Timer entries do not count.
    private int requestedInProgress = 0;
    // A refresh made while the connection is down sets the token of the next connect. The token it
    // replaced is kept here, and restored when that refresh is refused.
    private boolean connectTokenStored = false;
    private String tokenBeforeConnect;
    // The token the last refresh made while disconnected stored. A token set since then by the caller is kept.
    private String connectTokenSet;
    private final Deque<Waiting> waiting = new ArrayDeque<>();
    // The scheduled refresh. The generation is bumped on each cancel, so a timer which fired
    // after its cancel does nothing.
    private ScheduledFuture<?> timer;
    private long timerGeneration = 0;
    // A timer refresh was superseded by a refresh() call which waited behind its fetch, or a queued timer refresh
    // was dropped behind a refused refresh. A refused refresh then arms the retry, since the connection would
    // otherwise have no timer. Cleared when a token is accepted or the retry is armed.
    private boolean timerSuperseded = false;
    // Moves on with each refresh() call accepted and each token accepted. A timer token fetched under an older value is superseded.
    private long refreshGeneration = 0;
    // Identity the server last reported for the connection, see applyAuth. Used on the executor thread only.
    private String currentUser = "";
    private String currentAuthClass = "";


    AuthRefresh(ClientHost host, int retryMinMillis, int retryMaxMillis) {
        TokenExchangeFlow.check(host.options());
        this.host = host;
        this.retryMinMillis = retryMinMillis;
        this.retryMaxMillis = retryMaxMillis;
        TokenExchange tokenExchange = host.tokenExchange();
        this.flow = tokenExchange == null ? null : new TokenExchangeFlow(host, tokenExchange);
    }


    /** Whether the client was created with a token exchange. */
    boolean hasFlow() {
        return flow != null;
    }


    /** Moves on with each refresh() call and each accepted token. */
    long refreshGeneration() {
        return refreshGeneration;
    }


    /** The connection token getter of a token exchange: the timer fetches its token through the exchange. */
    ConnectionTokenGetter exchangeTokenGetter() {
        return flow.renewalGetter();
    }


    /** Called at the start of every connect attempt, before the connect command is built. */
    void beforeConnect() {
        if (flow != null) {
            flow.beforeConnect();
        }
    }


    /** Called with the connect reply before the connected state. Returns true when a token exchange took it over. */
    boolean interceptConnect(Protocol.Reply reply) {
        return flow != null && flow.interceptConnect(reply);
    }


    /** Called for every push. Returns true when the push was held by a token exchange which is connecting. */
    boolean holdPush(Protocol.Push push) {
        return flow != null && flow.holdPush(push);
    }


    /** Whether a refresh is in progress: a refresh command awaits its reply, or a refresh call has not called back. The rpc calls wait for it. */
    boolean isInFlight() {
        return commandInFlight || requestedInProgress > 0;
    }


    /**
     * Refreshes the connection with a token the caller gave. Runs on the executor. A refresh with the
     * token of a pending refresh call shares its command, otherwise it waits its turn. A refresh
     * beyond the bound of the refresh calls in progress is refused at once with LIMIT_EXCEEDED.
     */
    void refreshWith(String token, ResultCallback<AuthResult> cb) {
        // Closed: nothing will send the command, so the refresh fails now, as an rpc call does in RpcSession.start.
        if (host.state() == ClientState.CLOSED) {
            cb.onDone(new IOException(), null);
            return;
        }
        if (requestedInProgress >= MAX_IN_PROGRESS) {
            cb.onDone(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many refreshes are in progress", false), null);
            return;
        }
        requestedInProgress++;
        refreshGeneration++;
        if (host.state() != ClientState.CONNECTED) {
            keepTokenForConnect(token);
        }
        List<ResultCallback<AuthResult>> shared = pendingCallbacksWith(token);
        if (shared != null) {
            shared.add(cb);
            return;
        }
        List<ResultCallback<AuthResult>> callbacks = new ArrayList<>();
        callbacks.add(cb);
        if (inFlight) {
            waiting.add(new Waiting(token, callbacks, 0L));
            return;
        }
        inFlight = true;
        startRequested(token, callbacks);
    }


    /**
     * Fails the refreshes which wait for the refresh in flight, as the client closes. Runs on the
     * executor, after the connection is torn down. Every waiting refresh is failed even when a callback
     * throws, and the first such failure is reported to the listener: the close goes on either way.
     */
    void failWaiting() {
        Throwable failure = null;
        while (!waiting.isEmpty()) {
            Waiting next = waiting.poll();
            if (next.token == null) {
                continue;
            }
            requestedInProgress -= next.callbacks.size();
            Throwable thrown = deliverAll(next.callbacks, new IOException(), null);
            if (failure == null) {
                failure = thrown;
            }
        }
        if (failure != null) {
            host.reportError(failure);
        }
    }


    /**
     * Schedules the refresh before the token expires, or cancels the timer when the token does
     * not expire. Replaces the timer scheduled before, so one timer chain runs.
     */
    void scheduleExpiry(boolean expires, int ttl) {
        observeExpiry(expires, ttl);
        if (!expires) {
            cancelTimer();
        } else if (flow != null) {
            scheduleTimer(flow.renewalDelayMillis(ttl), TimeUnit.MILLISECONDS);
        } else {
            scheduleTimer(ttl, TimeUnit.SECONDS);
        }
    }


    // Records the expiry of the connection token, as the flow needs it for the next connect, whatever the state.
    private void observeExpiry(boolean expires, int ttl) {
        if (flow != null) {
            flow.observe(host.connectionToken(), expires, ttl);
        }
    }


    /** Cancels the timer, if one is scheduled. */
    void cancelTimer() {
        timerGeneration++;
        if (timer != null) {
            timer.cancel(true);
            timer = null;
        }
    }


    /**
     * The connection is gone. The timer is cancelled, and the command gate is reset: the refresh
     * command in flight fails with the connection, so nothing awaits its reply. The refresh in flight
     * is not reset here. Its callback completes it, and only then does a waiting refresh start.
     * A disconnect the client was asked for, or a terminal one the server ended the session with, ends the
     * session of the token exchange; a transport loss does not.
     */
    void onDisconnect(int code, boolean terminal) {
        cancelTimer();
        commandInFlight = false;
        timerSuperseded = false;
        if (flow != null) {
            flow.reset(code == Client.DISCONNECTED_DISCONNECT_CALLED || terminal);
        }
    }


    /**
     * Records the identity the server reports for the connection. A connect always reports it, so
     * the listener hears every connect; a refresh reports it only when it changed.
     */
    void applyAuth(String user, String authClass, boolean always) {
        boolean changed = !user.equals(this.currentUser) || !authClass.equals(this.currentAuthClass);
        this.currentUser = user;
        this.currentAuthClass = authClass;
        if (always || changed) {
            host.reportAuth(new AuthEvent(user, authClass));
        }
    }


    private void scheduleTimer(long delay, TimeUnit unit) {
        cancelTimer();
        final long generation = timerGeneration;
        timer = host.scheduler().schedule(() -> Extensions.submitOrIgnore(host.executor(), () -> onTimer(generation)), delay, unit);
    }


    private void scheduleRetry() {
        scheduleTimer(backoff.duration(0, retryMinMillis, retryMaxMillis), TimeUnit.MILLISECONDS);
    }


    // The timer fired: fetch a token and refresh with it, under the single-flight gate.
    void onTimer(long generation) {
        if (generation != timerGeneration || host.tokenGetter() == null) {
            return;
        }
        timer = null;
        if (inFlight) {
            waiting.add(new Waiting(null, null, generation));
            return;
        }
        inFlight = true;
        fetchAndRefresh();
    }


    private void fetchAndRefresh() {
        WebSocket transport = host.transport();
        final long generation = refreshGeneration;
        if (host.state() != ClientState.CONNECTED) {
            complete(false);
            return;
        }
        // The getter may never answer, so the fetch is bounded by the client timeout. A late answer is ignored.
        // A getter which throws fails the fetch as an error does, so the refresh still completes.
        CompletableFuture<String> fetched = new CompletableFuture<>();
        try {
            host.tokenGetter().getConnectionToken(new ConnectionTokenEvent(), (Throwable err, String token) -> {
                if (err != null) {
                    fetched.completeExceptionally(err);
                } else {
                    fetched.complete(token);
                }
            });
        } catch (RuntimeException | Error e) {
            fetched.completeExceptionally(e);
        }
        fetched.orTimeout(host.timeoutMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((String token, Throwable err) -> host.runOrInline(() -> onTokenFetched(transport, generation, Extensions.unwrapCompletion(err), token)));
    }


    private void onTokenFetched(WebSocket transport, long generation, Throwable err, String token) {
        // The refresh in flight is completed here, unless a refresh command goes out: its callback completes it.
        boolean awaitingCommand = false;
        try {
            if (transport != host.transport() || host.state() == ClientState.CLOSED) {
                return;
            }
            // The session is over whatever asked for the token: the application removed its refresh token, or the issuer
            // refused it past the grace horizon. A refresh() which waits for this fetch does not keep the session.
            if (err instanceof UnauthorizedException) {
                host.failUnauthorized();
                return;
            }
            if (host.state() != ClientState.CONNECTED) {
                keepTimerTokenForConnect(err, token, generation);
                return;
            }
            // A refresh() which started during the fetch waits behind it. This token is superseded: it is not sent,
            // and not fetched again. The waiting refresh runs when this fetch completes.
            if (hasRequestWaiting()) {
                timerSuperseded = true;
                return;
            }
            if (err != null) {
                scheduleRetry();
                host.reportError(new TokenError(err));
                return;
            }
            if (token == null || token.equals("")) {
                host.failUnauthorized();
                return;
            }
            commandInFlight = true;
            sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> host.runOrInline(() -> finishTimer(transport, token, error, result)));
            awaitingCommand = true;
        } finally {
            if (!awaitingCommand) {
                complete(false);
            }
        }
    }


    // Whether a refresh() call waits for the refresh in flight. A queued timer refresh does not count.
    private boolean hasRequestWaiting() {
        for (Waiting entry : waiting) {
            if (entry.token != null) {
                return true;
            }
        }
        return false;
    }


    private void finishTimer(WebSocket transport, String token, Throwable error, Protocol.RefreshResult result) {
        try {
            if (transport != host.transport() || host.state() != ClientState.CONNECTED) {
                return;
            }
            if (error == null) {
                host.storeToken(token);
                timerSuperseded = false;
                refreshGeneration++;
                scheduleExpiry(result.getExpires(), result.getTtl());
                applyAuth(result.getUser(), result.getAuthClass(), false);
                return;
            }
            try {
                host.reportError(new RefreshError(error));
            } finally {
                settleTimerError(error);
            }
        } finally {
            complete(false);
        }
    }


    /** Whether a refresh refused with this error is retried with a fresh token: a temporary error, a client error or an expired token. */
    static boolean isRetryableRefresh(ReplyError replyError) {
        return replyError.isTemporary() || replyError.getCode() < SERVER_ERROR_CODE_MIN || replyError.getCode() == ERROR_CODE_TOKEN_EXPIRED;
    }


    // Decides on a failed timer refresh: retried with backoff, reported as a disconnect, or kept as it is.
    private void settleTimerError(Throwable error) {
        if (!(error instanceof ReplyError)) {
            scheduleRetry();
            return;
        }
        ReplyError replyError = (ReplyError) error;
        if (isRetryableRefresh(replyError)) {
            scheduleRetry();
        } else if (replyError.getCode() != PERMISSION_DENIED) {
            host.disconnect(replyError.getCode(), replyError.getMessage());
        }
    }


    private void finishRequested(String token, Throwable error, Protocol.RefreshResult result, List<ResultCallback<AuthResult>> callbacks) {
        try {
            inFlightToken = null;
            inFlightCallbacks = null;
            requestedInProgress -= callbacks.size();
            if (error != null) {
                settleConnectToken(error);
                rethrow(deliverAll(callbacks, error, null));
                return;
            }
            host.storeToken(token);
            connectTokenStored = false;
            timerSuperseded = false;
            refreshGeneration++;
            if (flow != null) {
                flow.tokenAccepted();
            }
            if (host.state() == ClientState.CONNECTED) {
                scheduleExpiry(result.getExpires(), result.getTtl());
            } else {
                observeExpiry(result.getExpires(), result.getTtl());
            }
            AuthResult authResult = new AuthResult(result.getUser(), result.getAuthClass(), result.getExpires(), result.getTtl());
            try {
                // The identity is reported for a connected client only, as the expiry is scheduled for one. A connect
                // reports the identity again, so nothing is lost when the client is not connected.
                if (host.state() == ClientState.CONNECTED) {
                    applyAuth(result.getUser(), result.getAuthClass(), false);
                }
            } finally {
                rethrow(deliverAll(callbacks, null, authResult));
            }
        } finally {
            complete(error != null);
        }
    }


    // Sends the refresh command with the token the callers gave. The callbacks are those of every
    // refresh call which shares the command.
    private void startRequested(String token, List<ResultCallback<AuthResult>> callbacks) {
        inFlightToken = token;
        inFlightCallbacks = callbacks;
        commandInFlight = true;
        sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> host.runOrInline(() -> finishRequested(token, error, result, callbacks)));
    }


    // Sends the refresh command with the token and hands its outcome to the callback. The command gets the
    // client timeout as its reply deadline; the callback is called once, with the error or the result.
    private void sendRefreshCommand(String token, ResultCallback<Protocol.RefreshResult> cb) {
        Protocol.RefreshRequest req = Protocol.RefreshRequest.newBuilder()
                .setToken(token)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(host.nextCommandId())
                .setRefresh(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        // One stage delivers the outcome, so a callback which throws is never called a second time.
        f.orTimeout(host.timeoutMillis(), TimeUnit.MILLISECONDS).handle((Protocol.Reply reply, Throwable e) -> {
            if (e != null) {
                host.runOrInline(() -> {
                    host.cleanCommand(cmd);
                    cb.onDone(Extensions.unwrapCompletion(e), null);
                });
                return null;
            }
            host.cleanCommand(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(host.replyError(reply), null);
            } else {
                Protocol.RefreshResult replyResult = reply.getRefresh();
                cb.onDone(null, replyResult);
            }
            return null;
        });

        try {
            host.enqueueCommand(cmd, f);
        } catch (RuntimeException e) {
            // The command could not be sent: the stage above delivers the failure once, as it does a failed reply.
            f.completeExceptionally(e);
        }
    }


    // Hands the outcome to every callback. A callback which throws does not keep the others from theirs:
    // the first failure is returned once all of them ran, and null when none failed.
    private static Throwable deliverAll(List<ResultCallback<AuthResult>> callbacks, Throwable error, AuthResult result) {
        Throwable first = null;
        for (ResultCallback<AuthResult> cb : callbacks) {
            try {
                cb.onDone(error, result);
            } catch (RuntimeException | Error failure) {
                if (first == null) {
                    first = failure;
                }
            }
        }
        return first;
    }


    // Rethrows a failure a callback threw, unchanged. Nothing is thrown for null.
    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }


    // The callbacks of a requested refresh which is pending with the token: the newest queued one with it,
    // or else the one in flight. Null when the token starts a refresh of its own.
    private List<ResultCallback<AuthResult>> pendingCallbacksWith(String token) {
        Iterator<Waiting> queued = waiting.descendingIterator();
        while (queued.hasNext()) {
            Waiting entry = queued.next();
            if (entry.token != null && entry.token.equals(token)) {
                return entry.callbacks;
            }
        }
        if (inFlightToken != null && inFlightToken.equals(token)) {
            return inFlightCallbacks;
        }
        return null;
    }


    // A timer token fetched while the connection is down, on the connection of the fetch, is the token of the
    // next connect, unless a refresh() call or an accepted token superseded it since the fetch started.
    private void keepTimerTokenForConnect(Throwable err, String token, long generation) {
        if (generation != refreshGeneration) {
            return;
        }
        if (err != null) {
            // The fetch failed while the connection is down: the error is reported, as a fetch on a connected client is.
            host.reportError(new TokenError(err));
            return;
        }
        if (token == null || token.isEmpty()) {
            host.failUnauthorized();
            return;
        }
        host.storeToken(token);
    }


    // A refresh made while the connection is down sets the token the next connect sends.
    private void keepTokenForConnect(String token) {
        if (!connectTokenStored) {
            tokenBeforeConnect = host.connectionToken();
            connectTokenStored = true;
        }
        connectTokenSet = token;
        host.storeToken(token);
    }


    // Settles the token of a refresh made while the connection was down, once the refresh failed. Only a
    // refusal puts back the token the connection had before; the others keep the token, since the connect
    // may have carried it. Nothing is put back while another requested refresh may still be refused.
    private void settleConnectToken(Throwable error) {
        if (requestedInProgress > 0) {
            return;
        }
        if (error instanceof ReplyError) {
            restoreConnectToken();
        } else {
            connectTokenStored = false;
        }
    }


    // Puts back the token the connection had before a refresh made while it was down. A token which the caller
    // set meanwhile, with setToken, is newer than that refresh and is kept.
    private void restoreConnectToken() {
        if (connectTokenStored) {
            if (Objects.equals(host.connectionToken(), connectTokenSet)) {
                host.storeToken(tokenBeforeConnect);
            }
            connectTokenStored = false;
        }
    }


    // The refresh in flight is over. The next waiting refresh starts: while the connection is down its command
    // waits in the connect commands, and it is sent with the next connect. Queued timer refreshes are dropped
    // while the connection is down, since no token is fetched then. A queued timer refresh behind a refused
    // refresh is not fetched at once: the retry is armed instead, as for a superseded timer. A timer fetch
    // does not hold the rpc calls, so they are written before it starts; without a refresh to start, the rpc
    // calls held for the refresh are written too. The waiting refreshes fail only at close, see failWaiting.
    private void complete(boolean refused) {
        inFlight = false;
        commandInFlight = false;
        if (host.state() == ClientState.CLOSED) {
            try {
                failWaiting();
            } finally {
                host.flushRpc();
            }
            return;
        }
        while (!waiting.isEmpty()) {
            Waiting next = waiting.poll();
            if (next.token != null) {
                inFlight = true;
                startRequested(next.token, next.callbacks);
                return;
            }
            // A queued timer refresh is stale once a later schedule or cancel replaced its timer.
            if (host.state() != ClientState.CONNECTED || next.generation != timerGeneration) {
                continue;
            }
            if (refused) {
                timerSuperseded = true;
                continue;
            }
            inFlight = true;
            host.flushRpc();
            fetchAndRefresh();
            return;
        }
        if (refused && timerSuperseded) {
            timerSuperseded = false;
            scheduleRetry();
        }
        host.flushRpc();
    }
}
