package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
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
 * {@link #runOnExecutorOrInline} after the client closed, which only settles the callbacks.
 * <p>
 * The fetch and retry loop of the timer mirrors {@code Subscription.sendRefresh}. It is kept here,
 * so that the subscription code stays as it is.
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

    // Error codes below this one are client errors, which the timer retries with a fresh token.
    private static final int SERVER_ERROR_CODE_MIN = 100;

    // The refresh() calls in progress, the one in flight and the ones which wait for it, are bounded by this.
    private static final int MAX_IN_PROGRESS = 32;

    private final Client client;
    // The bounds of the retry delay after a failed token fetch or refresh, in milliseconds.
    private final int retryMinMillis;
    private final int retryMaxMillis;
    private final Backoff backoff = new Backoff();
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
    private final Deque<Waiting> waiting = new ArrayDeque<>();
    // The scheduled refresh. The generation is bumped on each cancel, so a timer which fired
    // after its cancel does nothing.
    private ScheduledFuture<?> timer;
    private long timerGeneration = 0;
    // Identity the server last reported for the connection, see applyAuth. Used on the executor thread only.
    private String currentUser = "";
    private String currentAuthClass = "";


    AuthRefresh(Client client, int retryMinMillis, int retryMaxMillis) {
        this.client = client;
        this.retryMinMillis = retryMinMillis;
        this.retryMaxMillis = retryMaxMillis;
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
        if (client.getState() == ClientState.CLOSED) {
            cb.onDone(new IOException(), null);
            return;
        }
        if (requestedInProgress >= MAX_IN_PROGRESS) {
            cb.onDone(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many refreshes are in progress", false), null);
            return;
        }
        requestedInProgress++;
        if (client.getState() != ClientState.CONNECTED) {
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
            client.reportError(failure);
        }
    }


    /**
     * Schedules the refresh before the token expires, or cancels the timer when the token does
     * not expire. Replaces the timer scheduled before, so one timer chain runs.
     */
    void scheduleExpiry(boolean expires, int ttl) {
        if (expires) {
            scheduleTimer(ttl, TimeUnit.SECONDS);
        } else {
            cancelTimer();
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
     */
    void onDisconnect() {
        cancelTimer();
        commandInFlight = false;
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
            client.reportAuth(new AuthEvent(user, authClass));
        }
    }


    private void scheduleTimer(long delay, TimeUnit unit) {
        cancelTimer();
        final long generation = timerGeneration;
        timer = client.getScheduler().schedule(() -> client.sendRefresh(generation), delay, unit);
    }


    private void scheduleRetry() {
        scheduleTimer(backoff.duration(0, retryMinMillis, retryMaxMillis), TimeUnit.MILLISECONDS);
    }


    // The timer fired: fetch a token and refresh with it, under the single-flight gate.
    void onTimer(long generation) {
        if (generation != timerGeneration || client.getOpts().getTokenGetter() == null) {
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
        WebSocket transport = client.transport();
        if (client.getState() != ClientState.CONNECTED) {
            complete();
            return;
        }
        // The getter may never answer, so the fetch is bounded by the client timeout. A late answer is ignored.
        // A getter which throws fails the fetch as an error does, so the refresh still completes.
        CompletableFuture<String> fetched = new CompletableFuture<>();
        try {
            client.getOpts().getTokenGetter().getConnectionToken(new ConnectionTokenEvent(), (Throwable err, String token) -> {
                if (err != null) {
                    fetched.completeExceptionally(err);
                } else {
                    fetched.complete(token);
                }
            });
        } catch (RuntimeException | Error e) {
            fetched.completeExceptionally(e);
        }
        fetched.orTimeout(client.getOpts().getTimeout(), TimeUnit.MILLISECONDS)
                .whenComplete((String token, Throwable err) -> runOnExecutorOrInline(() -> onTokenFetched(transport, Extensions.unwrapCompletion(err), token)));
    }


    private void onTokenFetched(WebSocket transport, Throwable err, String token) {
        // The refresh in flight is completed here, unless a refresh command goes out: its callback completes it.
        boolean awaitingCommand = false;
        try {
            if (transport != client.transport() || client.getState() != ClientState.CONNECTED) {
                return;
            }
            if (err != null) {
                if (err instanceof UnauthorizedException) {
                    client.failUnauthorized();
                } else {
                    scheduleRetry();
                    client.reportError(new TokenError(err));
                }
                return;
            }
            if (token == null || token.equals("")) {
                client.failUnauthorized();
                return;
            }
            commandInFlight = true;
            sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> runOnExecutorOrInline(() -> finishTimer(transport, token, error, result)));
            awaitingCommand = true;
        } finally {
            if (!awaitingCommand) {
                complete();
            }
        }
    }


    private void finishTimer(WebSocket transport, String token, Throwable error, Protocol.RefreshResult result) {
        try {
            if (transport != client.transport() || client.getState() != ClientState.CONNECTED) {
                return;
            }
            if (error == null) {
                client.storeToken(token);
                scheduleExpiry(result.getExpires(), result.getTtl());
                applyAuth(result.getUser(), result.getAuthClass(), false);
                return;
            }
            try {
                client.reportError(new RefreshError(error));
            } finally {
                settleTimerError(error);
            }
        } finally {
            complete();
        }
    }


    // Decides on a failed timer refresh: retried with backoff, reported as a disconnect, or kept as it is.
    private void settleTimerError(Throwable error) {
        if (!(error instanceof ReplyError)) {
            scheduleRetry();
            return;
        }
        ReplyError replyError = (ReplyError) error;
        if (replyError.isTemporary() || replyError.getCode() < SERVER_ERROR_CODE_MIN || replyError.getCode() == Client.ERROR_CODE_TOKEN_EXPIRED) {
            scheduleRetry();
        } else if (replyError.getCode() != PERMISSION_DENIED) {
            client.processDisconnect(replyError.getCode(), replyError.getMessage(), false);
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
            client.storeToken(token);
            connectTokenStored = false;
            if (client.getState() == ClientState.CONNECTED) {
                scheduleExpiry(result.getExpires(), result.getTtl());
            }
            AuthResult authResult = new AuthResult(result.getUser(), result.getAuthClass(), result.getExpires(), result.getTtl());
            try {
                // The identity is reported for a connected client only, as the expiry is scheduled for one. A connect
                // reports the identity again, so nothing is lost when the client is not connected.
                if (client.getState() == ClientState.CONNECTED) {
                    applyAuth(result.getUser(), result.getAuthClass(), false);
                }
            } finally {
                rethrow(deliverAll(callbacks, null, authResult));
            }
        } finally {
            complete();
        }
    }


    // Sends the refresh command with the token the callers gave. The callbacks are those of every
    // refresh call which shares the command.
    private void startRequested(String token, List<ResultCallback<AuthResult>> callbacks) {
        inFlightToken = token;
        inFlightCallbacks = callbacks;
        commandInFlight = true;
        sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> runOnExecutorOrInline(() -> finishRequested(token, error, result, callbacks)));
    }


    // Sends the refresh command with the token and hands its outcome to the callback. The command gets the
    // client timeout as its reply deadline; the callback is called once, with the error or the result.
    private void sendRefreshCommand(String token, ResultCallback<Protocol.RefreshResult> cb) {
        Protocol.RefreshRequest req = Protocol.RefreshRequest.newBuilder()
                .setToken(token)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(client.nextCommandId())
                .setRefresh(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        // One stage delivers the outcome, so a callback which throws is never called a second time.
        f.orTimeout(client.getOpts().getTimeout(), TimeUnit.MILLISECONDS).handle((Protocol.Reply reply, Throwable e) -> {
            if (e != null) {
                runOnExecutorOrInline(() -> {
                    client.cleanCommandFuture(cmd);
                    cb.onDone(Extensions.unwrapCompletion(e), null);
                });
                return null;
            }
            client.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(client.getReplyError(reply), null);
            } else {
                Protocol.RefreshResult replyResult = reply.getRefresh();
                cb.onDone(null, replyResult);
            }
            return null;
        });

        client.enqueueCommandFuture(cmd, f);
    }


    // Runs the task on the executor. Once the executor is shut down, which is after the client closed, a late
    // completion runs the task inline on the completing thread instead, so the callbacks of the refresh still
    // run exactly once. The task then only settles the callbacks: the client is closed. A failure of an inline
    // task goes to the listener.
    private void runOnExecutorOrInline(Runnable task) {
        Runnable reported = () -> {
            try {
                task.run();
            } catch (RuntimeException failure) {
                client.reportError(failure);
            }
        };
        Extensions.submitOrRun(client.getExecutor(), reported, reported);
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


    // A refresh made while the connection is down sets the token the next connect sends.
    private void keepTokenForConnect(String token) {
        if (!connectTokenStored) {
            tokenBeforeConnect = client.getConnectionToken();
            connectTokenStored = true;
        }
        client.storeToken(token);
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


    // Puts back the token the connection had before a refresh made while it was down.
    private void restoreConnectToken() {
        if (connectTokenStored) {
            client.storeToken(tokenBeforeConnect);
            connectTokenStored = false;
        }
    }


    // The refresh in flight is over. While the connection is down the waiting refreshes fail with it, since
    // nothing is sent while disconnected and the timer does not run. Otherwise the next waiting refresh
    // starts, and without one the rpc calls held for the refresh are written.
    private void complete() {
        inFlight = false;
        commandInFlight = false;
        if (client.getState() != ClientState.CONNECTED) {
            try {
                failWaiting();
            } finally {
                client.flushRpc();
            }
            return;
        }
        while (!waiting.isEmpty()) {
            Waiting next = waiting.poll();
            // A queued timer refresh is stale once a later schedule or cancel replaced its timer.
            if (next.token == null && next.generation != timerGeneration) {
                continue;
            }
            inFlight = true;
            if (next.token == null) {
                fetchAndRefresh();
            } else {
                startRequested(next.token, next.callbacks);
            }
            return;
        }
        client.flushRpc();
    }
}
