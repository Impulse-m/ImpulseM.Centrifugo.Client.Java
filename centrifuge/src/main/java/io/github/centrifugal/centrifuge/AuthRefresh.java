package io.github.centrifugal.centrifuge;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;
import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import okhttp3.WebSocket;


/**
 * The refreshes of the connection token: the refresh a caller asks for with
 * {@link Client#refresh}, and the timer which refreshes before the token expires. One refresh is
 * in flight at a time, the others wait their turn. The rpc calls wait for the refresh in flight,
 * see {@link RpcSession}. Used on the client's executor thread only.
 */
final class AuthRefresh {
    // Server refresh errors which keep the connection and its identity: a refused transition,
    // and a token which expired. The timer retries them with a fresh token.
    private static final int PERMISSION_DENIED = 103;
    private static final int TOKEN_EXPIRED = 109;

    private static final int RETRY_MIN_MILLIS = 10000;
    private static final int RETRY_MAX_MILLIS = 20000;

    private final Client client;
    private final Backoff backoff = new Backoff();
    // The refresh in flight is the one which runs now. The refreshes waiting for it run in order.
    private boolean inFlight = false;
    private final Deque<Runnable> waiting = new ArrayDeque<>();
    // The scheduled refresh. The generation is bumped on each cancel, so a timer which fired
    // after its cancel does nothing.
    private ScheduledFuture<?> timer;
    private long timerGeneration = 0;


    AuthRefresh(Client client) {
        this.client = client;
    }


    /** Whether a refresh is in flight. The rpc calls wait for it. */
    boolean isInFlight() {
        return inFlight;
    }


    /** Refreshes the connection with a token the caller gave. Runs on the executor. */
    void refreshWith(String token, ResultCallback<AuthResult> cb) {
        run(() -> client.sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> client.getExecutor().submit(() -> finishRequested(token, error, result, cb))));
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


    private void scheduleTimer(long delay, TimeUnit unit) {
        cancelTimer();
        final long generation = timerGeneration;
        timer = client.getScheduler().schedule(() -> client.getExecutor().submit(() -> onTimer(generation)), delay, unit);
    }


    private void scheduleRetry() {
        scheduleTimer(backoff.duration(0, RETRY_MIN_MILLIS, RETRY_MAX_MILLIS), TimeUnit.MILLISECONDS);
    }


    // The timer fired: fetch a token and refresh with it, under the single-flight gate.
    private void onTimer(long generation) {
        if (generation != timerGeneration || client.getOpts().getTokenGetter() == null) {
            return;
        }
        timer = null;
        run(this::fetchAndRefresh);
    }


    private void fetchAndRefresh() {
        WebSocket transport = client.transport();
        if (client.getState() != ClientState.CONNECTED) {
            complete();
            return;
        }
        client.getOpts().getTokenGetter().getConnectionToken(new ConnectionTokenEvent(), (Throwable err, String token) -> client.getExecutor().submit(() -> onTokenFetched(transport, err, token)));
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
        client.sendRefreshCommand(token, (Throwable error, Protocol.RefreshResult result) -> client.getExecutor().submit(() -> finishTimer(transport, token, error, result)));
    }


    private void finishTimer(WebSocket transport, String token, Throwable error, Protocol.RefreshResult result) {
        if (transport != client.transport() || client.getState() != ClientState.CONNECTED) {
            complete();
            return;
        }
        if (error == null) {
            client.storeToken(token);
            scheduleExpiry(result.getExpires(), result.getTtl());
            complete();
            return;
        }
        client.reportError(new RefreshError(error));
        if (error instanceof ReplyError) {
            ReplyError replyError = (ReplyError) error;
            if (replyError.isTemporary() || replyError.getCode() == PERMISSION_DENIED || replyError.getCode() == TOKEN_EXPIRED) {
                scheduleRetry();
            } else {
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
        client.storeToken(token);
        if (client.getState() == ClientState.CONNECTED) {
            scheduleExpiry(result.getExpires(), result.getTtl());
        }
        cb.onDone(null, new AuthResult(result.getUser(), result.getAuthClass(), result.getExpires(), result.getTtl()));
        complete();
    }


    // Starts the attempt now, or once the refresh in flight is over.
    private void run(Runnable attempt) {
        if (inFlight) {
            waiting.add(attempt);
            return;
        }
        inFlight = true;
        attempt.run();
    }


    // The refresh in flight is over. The next waiting refresh starts; without one, the rpc calls
    // held for the refresh are written.
    private void complete() {
        inFlight = false;
        if (!waiting.isEmpty()) {
            inFlight = true;
            waiting.poll().run();
            return;
        }
        client.flushRpc();
    }
}
