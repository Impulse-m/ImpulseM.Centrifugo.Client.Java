package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
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
 * flight, see {@link RpcSession}. Used on the client's executor thread only.
 * <p>
 * The fetch and retry loop of the timer mirrors {@code Subscription.sendRefresh}. It is kept here,
 * so that the subscription code stays as it is.
 */
final class AuthRefresh {
    // A refresh which waits for the refresh in flight. A requested refresh carries its token and
    // callback. A timer refresh carries neither: it fetches its token when it starts.
    private static final class Waiting {
        final String token;
        final ResultCallback<AuthResult> callback;


        Waiting(String token, ResultCallback<AuthResult> callback) {
            this.token = token;
            this.callback = callback;
        }
    }



    // A refused transition keeps the connection and its identity, and is reported without a
    // retry. A token which expired is retried by the timer with a fresh token.
    private static final int PERMISSION_DENIED = 103;

    private final Client client;
    // The bounds of the retry delay after a failed token fetch or refresh, in milliseconds.
    private final int retryMinMillis;
    private final int retryMaxMillis;
    private final Backoff backoff = new Backoff();
    // The refresh in flight is the one which runs now. The refreshes waiting for it run in order.
    private boolean inFlight = false;
    // A refresh command awaits its reply. Only this gates the rpc calls: a timer refresh which is
    // still fetching its token does not hold them.
    private boolean commandInFlight = false;
    private final Deque<Waiting> waiting = new ArrayDeque<>();
    // The scheduled refresh. The generation is bumped on each cancel, so a timer which fired
    // after its cancel does nothing.
    private ScheduledFuture<?> timer;
    private long timerGeneration = 0;
    // Identity the server last reported for the connection, see applyAuth. A refresh reply
    // arrives on the transport thread, so the fields are volatile.
    private volatile String currentUser = "";
    private volatile String currentAuthClass = "";


    AuthRefresh(Client client, int retryMinMillis, int retryMaxMillis) {
        this.client = client;
        this.retryMinMillis = retryMinMillis;
        this.retryMaxMillis = retryMaxMillis;
    }


    /** Whether a refresh command awaits its reply. The rpc calls wait for it. */
    boolean isInFlight() {
        return commandInFlight;
    }


    /** Refreshes the connection with a token the caller gave. Runs on the executor. */
    void refreshWith(String token, ResultCallback<AuthResult> cb) {
        if (inFlight) {
            waiting.add(new Waiting(token, cb));
            return;
        }
        inFlight = true;
        startRequested(token, cb);
    }


    /**
     * Fails the refreshes which wait for the refresh in flight, as the client closes. Runs on the
     * executor, after the connection is torn down.
     */
    void failWaiting() {
        while (!waiting.isEmpty()) {
            Waiting next = waiting.poll();
            if (next.callback != null) {
                next.callback.onDone(new IOException(), null);
            }
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
        timer = client.getScheduler().schedule(() -> client.getExecutor().submit(() -> onTimer(generation)), delay, unit);
    }


    private void scheduleRetry() {
        scheduleTimer(backoff.duration(0, retryMinMillis, retryMaxMillis), TimeUnit.MILLISECONDS);
    }


    // The timer fired: fetch a token and refresh with it, under the single-flight gate.
    private void onTimer(long generation) {
        if (generation != timerGeneration || client.getOpts().getTokenGetter() == null) {
            return;
        }
        timer = null;
        if (inFlight) {
            waiting.add(new Waiting(null, null));
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
        CompletableFuture<String> fetched = new CompletableFuture<>();
        client.getOpts().getTokenGetter().getConnectionToken(new ConnectionTokenEvent(), (Throwable err, String token) -> {
            if (err != null) {
                fetched.completeExceptionally(err);
            } else {
                fetched.complete(token);
            }
        });
        fetched.orTimeout(client.getOpts().getTimeout(), TimeUnit.MILLISECONDS)
                .whenComplete((String token, Throwable err) -> Extensions.runOnExecutor(client.getExecutor(), () -> onTokenFetched(transport, Extensions.unwrapCompletion(err), token)));
    }


    private void onTokenFetched(WebSocket transport, Throwable err, String token) {
        if (transport != client.transport() || client.getState() != ClientState.CONNECTED) {
            complete();
            return;
        }
        if (err != null) {
            if (err instanceof UnauthorizedException) {
                client.failUnauthorized();
            } else {
                client.reportError(new TokenError(err));
                scheduleRetry();
            }
            complete();
            return;
        }
        if (token == null || token.equals("")) {
            client.failUnauthorized();
            complete();
            return;
        }
        commandInFlight = true;
        client.sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> Extensions.runOnExecutor(client.getExecutor(), () -> finishTimer(transport, token, error, result)));
    }


    private void finishTimer(WebSocket transport, String token, Throwable error, Protocol.RefreshResult result) {
        if (transport != client.transport() || client.getState() != ClientState.CONNECTED) {
            complete();
            return;
        }
        if (error == null) {
            applyAuth(result.getUser(), result.getAuthClass(), false);
            client.storeToken(token);
            scheduleExpiry(result.getExpires(), result.getTtl());
            complete();
            return;
        }
        client.reportError(new RefreshError(error));
        if (error instanceof ReplyError) {
            ReplyError replyError = (ReplyError) error;
            if (replyError.isTemporary() || replyError.getCode() == Client.ERROR_CODE_TOKEN_EXPIRED) {
                scheduleRetry();
            } else if (replyError.getCode() != PERMISSION_DENIED) {
                client.processDisconnect(replyError.getCode(), replyError.getMessage(), false);
            }
        } else {
            scheduleRetry();
        }
        complete();
    }


    private void finishRequested(String token, Throwable error, Protocol.RefreshResult result, ResultCallback<AuthResult> cb) {
        if (error != null) {
            cb.onDone(error, null);
            complete();
            return;
        }
        applyAuth(result.getUser(), result.getAuthClass(), false);
        client.storeToken(token);
        if (client.getState() == ClientState.CONNECTED) {
            scheduleExpiry(result.getExpires(), result.getTtl());
        }
        cb.onDone(null, new AuthResult(result.getUser(), result.getAuthClass(), result.getExpires(), result.getTtl()));
        complete();
    }


    // Sends the refresh command with the token the caller gave.
    private void startRequested(String token, ResultCallback<AuthResult> cb) {
        commandInFlight = true;
        client.sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> Extensions.runOnExecutor(client.getExecutor(), () -> finishRequested(token, error, result, cb)));
    }


    // The refresh in flight is over. The next waiting refresh starts; without one, the rpc calls
    // held for the refresh are written.
    private void complete() {
        inFlight = false;
        commandInFlight = false;
        if (!waiting.isEmpty()) {
            inFlight = true;
            Waiting next = waiting.poll();
            if (next.callback == null) {
                fetchAndRefresh();
            } else {
                startRequested(next.token, next.callback);
            }
            return;
        }
        client.flushRpc();
    }
}
