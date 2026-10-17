package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;
import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;
import okhttp3.WebSocket;


/**
 * The token exchange of a client, see {@link TokenExchange}. An anonymous connect is completed by an
 * exchange of the refresh token for an access token over the same socket: the access token is set as
 * the connection token and a refresh command with it binds the connection to the identity. The timer
 * renews the access token over the connected socket through the same exchange. Used on the client's
 * executor thread only, except where a method says otherwise.
 * <p>
 * The refresh token is read with {@link TokenExchange#load} before each exchange and is never kept in a
 * field: it lives in the locals of one exchange only.
 */
final class TokenExchangeFlow {
    // The bounds of the retry delay after a failed bootstrap, in milliseconds.
    private static final int BOOTSTRAP_RETRY_MIN_MILLIS = 5000;
    private static final int BOOTSTRAP_RETRY_MAX_MILLIS = 60000;

    // Renewal fires at a random share of the ttl in [RENEWAL_MIN_SHARE, RENEWAL_MIN_SHARE + RENEWAL_JITTER).
    private static final double RENEWAL_MIN_SHARE = 0.7;
    private static final double RENEWAL_JITTER = 0.2;

    // The re-presentations of an unchanged refresh token a refusal run allows: one, at a random moment inside the
    // remaining grace.
    private static final int REFUSAL_RETRIES = 1;
    // The upper bound of the grace in seconds.
    private static final int GRACE_MAX_SECONDS = 300;
    // The grace of a result which publishes none, see TokenExchangeResult.getGrace.
    private static final int ABSENT_GRACE = -1;
    // The cause of the retry of a refused refresh token, reported as an error. It names no token.
    private static final String REFUSED_RETRY_MESSAGE = "refresh token refused, retrying within the grace";


    // The access token of the connection and when it expires. Null when no expiry is known.
    private static final class Expiry {
        final String token;
        final long expiresAtMillis;


        Expiry(String token, long expiresAtMillis) {
            this.token = token;
            this.expiresAtMillis = expiresAtMillis;
        }
    }


    // A bootstrap: the anonymous connect reply, held until the access token is obtained. The pushes of the
    // subscriptions the server made for the connection are held too, and merged into the connect result.
    private static final class Bootstrap {
        final WebSocket transport;
        final Protocol.Reply reply;
        final Map<String, Protocol.SubscribeResult> heldSubs = new LinkedHashMap<>();
        final Set<String> heldUnsubs = new LinkedHashSet<>();
        ScheduledFuture<?> timer;
        // The client timeout of the exchange in flight, cancelled when its callback comes.
        ScheduledFuture<?> exchangeTimeout;
        // Set when the access token was stored for the refresh command, and cleared if that command is lost.
        boolean tokenSet;


        Bootstrap(WebSocket transport, Protocol.Reply reply) {
            this.transport = transport;
            this.reply = reply;
        }
    }


    // A renewal exchange. Its callback is called once: by the exchange, or by the deadline when the exchange is late.
    private static final class Renewal {
        final AtomicBoolean called = new AtomicBoolean();
        // The client timeout of the exchange, cancelled when its callback comes.
        ScheduledFuture<?> deadline;
    }


    private final ClientHost host;
    private final TokenExchange exchange;
    private final ConnectionTokenGetter renewalGetter;
    // The server error names that mean a refused refresh token, copied once from the exchange.
    private final Set<String> refusalTypes;
    private final Backoff backoff = new Backoff();
    private Expiry expiry;
    // Set by beforeConnect: the next connect reply is bootstrapped when it is anonymous.
    private boolean anonymousConnect = false;
    // Set by beforeConnect: the connect carries the access token, so a reply with a user is the server's acceptance of it.
    private boolean connectSentToken = false;
    private Bootstrap bootstrap;
    private int bootstrapAttempt = 0;
    // The grace in seconds: the exchange's, then the one the latest accepted exchange published.
    private int graceSeconds;
    // The refusal run: the deadline of its horizon, null when no run is open, and the refused attempts of the run whose
    // re-read found the value unchanged.
    private ScheduledFuture<?> horizon;
    private int refusals = 0;


    /**
     * The flow of a client created with a token exchange, or null without one. Rejects a client which sets both a token
     * exchange and a token getter, a refusal type which is null or empty, and a negative grace. Called by the client
     * constructor.
     */
    static TokenExchangeFlow create(ClientHost host) {
        Options opts = host.options();
        TokenExchange exchange = opts.getTokenExchange();
        if (exchange == null) {
            return null;
        }
        if (opts.getTokenGetter() != null) {
            throw new IllegalArgumentException("tokenExchange and tokenGetter are exclusive");
        }
        for (String type : refusalTypesOf(exchange)) {
            if (type == null || type.isEmpty()) {
                throw new IllegalArgumentException("refusalTypes() holds a null or empty entry");
            }
        }
        if (exchange.grace() < 0) {
            throw new IllegalArgumentException("grace() is negative");
        }
        return new TokenExchangeFlow(host, exchange);
    }


    private TokenExchangeFlow(ClientHost host, TokenExchange exchange) {
        this.host = host;
        this.exchange = exchange;
        this.refusalTypes = new LinkedHashSet<>(refusalTypesOf(exchange));
        this.graceSeconds = clampGrace(exchange.grace());
        this.renewalGetter = new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                renew(cb);
            }
        };
    }


    /** The token getter of the timer refresh: the exchange over the connected socket. */
    ConnectionTokenGetter renewalGetter() {
        return renewalGetter;
    }


    /**
     * Called at the start of every connect attempt. Clears the connection token when the server asked for a
     * fresh one, or when the expiry record says it expired, so that the connect goes out anonymously.
     */
    void beforeConnect() {
        boolean expired = expiry != null
                && expiry.token.equals(host.connectionToken())
                && System.currentTimeMillis() >= expiry.expiresAtMillis;
        if (host.isRefreshRequired() || expired) {
            host.storeToken("");
            host.setRefreshRequired(false);
            expiry = null;
        }
        anonymousConnect = true;
        connectSentToken = !isEmptyToken(host.connectionToken());
    }


    /**
     * Called with the connect reply before the connected state. Returns true when the exchange took the reply
     * over: the connect resumes with the merged reply later. False when the connect goes on at once, which is
     * the case for a reply which carries a user, whether the connect sent a token or not, for a second call of the
     * same connect, and for an empty refresh token.
     */
    boolean interceptConnect(Protocol.Reply reply) {
        if (!anonymousConnect) {
            return false;
        }
        anonymousConnect = false;
        boolean sentToken = connectSentToken;
        connectSentToken = false;
        if (!reply.getConnect().getUser().isEmpty()) {
            // A user reply to a connect without a token is the server's doing (a connect proxy), not an acceptance of one.
            if (sentToken) {
                tokenAccepted();
            }
            return false;
        }
        // A token the connect sent was refused by an anonymous reply, so none is kept: it is stored again once the exchange stores one.
        host.storeToken("");
        // The server pings the connection from now on, during the bootstrap too: the ping is answered as a connected one is.
        host.applyPing(reply.getConnect());
        Bootstrap started = new Bootstrap(host.transport(), reply);
        bootstrap = started;
        if (startExchange(started)) {
            return true;
        }
        bootstrap = null;
        return false;
    }


    /** Called for every push. Holds the subscribe and unsubscribe pushes of a bootstrap; returns whether it did. */
    boolean holdPush(Protocol.Push push) {
        Bootstrap current = bootstrap;
        if (current == null) {
            return false;
        }
        if (push.hasSubscribe()) {
            current.heldSubs.put(push.getChannel(), subscribeResultOf(push.getSubscribe()));
            current.heldUnsubs.remove(push.getChannel());
            return true;
        }
        if (push.hasUnsubscribe()) {
            current.heldSubs.remove(push.getChannel());
            current.heldUnsubs.add(push.getChannel());
            return true;
        }
        return false;
    }


    /** Refresh timer delay of a ttl in seconds: a random share of it, in milliseconds. */
    long renewalDelayMillis(int ttl) {
        double share = RENEWAL_MIN_SHARE + RENEWAL_JITTER * ThreadLocalRandom.current().nextDouble();
        return (long) (ttl * 1000L * share);
    }


    /** Records when the connection token expires, from a reply which carried it. An empty token has no record. */
    void observe(boolean expires, int ttl) {
        String token = host.connectionToken();
        if (!expires || token == null || token.isEmpty()) {
            expiry = null;
            return;
        }
        expiry = new Expiry(token, System.currentTimeMillis() + ttl * 1000L);
    }


    /**
     * The connection is gone: the bootstrap is abandoned and its retry is cancelled. The backoff of the bootstrap
     * attempts goes on, it restarts when a bootstrap completes or ends the session. A disconnect the client was asked
     * for, or a terminal one, ends the session: its attempts restart with the next connect and its refusal run closes.
     * The expiry record is kept.
     */
    void reset(int code, boolean terminal) {
        if (bootstrap != null) {
            abandon(bootstrap);
        }
        anonymousConnect = false;
        if (code == Client.DISCONNECTED_DISCONNECT_CALLED || terminal) {
            bootstrapAttempt = 0;
            closeRefusalRun();
        }
    }


    /**
     * Called when the server accepted the connection token: a connect reply with a user, or a refresh reply of a
     * refresh call. The session is valid then, so an open refusal run is over, and a later refusal starts a run of
     * its own with a fresh horizon.
     */
    void tokenAccepted() {
        closeRefusalRun();
    }


    /**
     * Writes a plain command on the transport and completes with its reply. The command carries no message
     * id, is not retransmitted, and is not held for the connected state. Fails with IOException when the
     * transport is not the current one, or when it refuses the command. Runs on the executor.
     */
    static CompletableFuture<Protocol.Reply> writePlain(
            final ClientHost host,
            final WebSocket transport,
            Protocol.Command.Builder builder
    ) {
        CompletableFuture<Protocol.Reply> future = new CompletableFuture<>();
        if (host.transport() != transport) {
            future.completeExceptionally(new IOException());
            return future;
        }
        final int id = host.nextCommandId();
        host.addFuture(id, future);
        try {
            if (!host.sendCommand(builder.setId(id).build())) {
                future.completeExceptionally(new IOException());
            }
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future.orTimeout(host.timeoutMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((Protocol.Reply reply, Throwable err) -> host.removeFuture(id));
    }


    /**
     * Sends a plain rpc call over the transport and hands its outcome to the callback. Runs on the executor, or inline
     * once the executor is shut down: a closed client fails the call here, so no state of the flow is touched inline.
     */
    static void sendRpc(
            final ClientHost host,
            final WebSocket transport,
            final Set<String> refusalTypes,
            String method,
            byte[] data,
            final ResultCallback<RPCResult> cb
    ) {
        if (host.state() == ClientState.CLOSED) {
            cb.onDone(new IOException(), null);
            return;
        }
        Protocol.Command.Builder builder = Protocol.Command.newBuilder()
                .setRpc(Protocol.RPCRequest.newBuilder()
                        .setMethod(method)
                        .setData(com.google.protobuf.ByteString.copyFrom(data)));
        final ResultCallback<RPCResult> classified = classifyRefusal(cb, refusalTypes);
        writePlain(host, transport, builder).whenComplete((Protocol.Reply reply, Throwable err) -> {
            host.runOrInline(() -> RpcSession.deliver(host, classified, reply, err));
        });
    }


    // The callback of an exchange rpc: a server error whose app type is a refusal type is passed on as the refusal.
    private static ResultCallback<RPCResult> classifyRefusal(final ResultCallback<RPCResult> cb, final Set<String> refusalTypes) {
        return (Throwable err, RPCResult result) -> {
            if (err instanceof ReplyError && refusalTypes.contains(((ReplyError) err).getAppType())) {
                cb.onDone(new TokenExchangeRefusedException((ReplyError) err), null);
                return;
            }
            cb.onDone(err, result);
        };
    }


    private static Set<String> refusalTypesOf(TokenExchange exchange) {
        Set<String> types = exchange.refusalTypes();
        return types == null ? Collections.<String>emptySet() : types;
    }


    private static int clampGrace(int seconds) {
        return Math.max(0, Math.min(GRACE_MAX_SECONDS, seconds));
    }


    // Starts the exchange of a bootstrap. Returns false when the refresh token is empty: the connect then goes on
    // anonymously. Returns true when the connect waits for the exchange or for a retry of it.
    private boolean startExchange(Bootstrap b) {
        String refreshToken;
        try {
            refreshToken = exchange.load();
        } catch (UnauthorizedException e) {
            endBootstrapSession();
            return true;
        } catch (RuntimeException e) {
            failBootstrap(b, e);
            return true;
        }
        if (isEmptyToken(refreshToken)) {
            if (horizon != null) {
                // The token was removed during a refusal run: the session ends, as a refusal read again would end it.
                endBootstrapSession();
                return true;
            }
            return false;
        }
        runExchange(b, refreshToken);
        return true;
    }


    // Runs one exchange of the bootstrap. The callback hops back to the client's executor.
    private void runExchange(final Bootstrap b, final String refreshToken) {
        final AtomicBoolean called = new AtomicBoolean();
        try {
            b.exchangeTimeout = host.scheduler().schedule(
                    () -> Extensions.submitOrIgnore(host.executor(), () -> onExchangeTimeout(b, called)),
                    host.timeoutMillis(),
                    TimeUnit.MILLISECONDS);
            exchange.exchange(new TokenExchangeEvent(host, b.transport, refreshToken, refusalTypes), (Throwable err, TokenExchangeResult result) -> {
                if (called.compareAndSet(false, true)) {
                    host.runOrInline(() -> {
                        onExchanged(b, refreshToken, Extensions.unwrapCompletion(err), result);
                    });
                }
            });
        } catch (RuntimeException e) {
            if (called.compareAndSet(false, true)) {
                onExchanged(b, refreshToken, e, null);
            } else {
                host.reportError(new TokenError(e));
            }
        }
    }


    // The bootstrap exchange got no callback within the client timeout. The connection is closed after the failure backoff,
    // so the reconnect bootstraps again on a new socket, which a silently dead one would never do. A callback which comes
    // later does not bootstrap, but its rotation is still stored, see storeAbandonedRotation.
    private void onExchangeTimeout(Bootstrap b, AtomicBoolean called) {
        if (!isCurrent(b) || called.get()) {
            return;
        }
        abandonAndClose(b, new TimeoutException("token exchange timed out"));
    }


    // Ends a bootstrap whose exchange stalled, or whose refresh outcome is unknown: the error is reported, and the
    // connection is closed once the failure backoff has run, so the reconnect bootstraps again. The backoff escalates
    // across reconnects, as the retries of a failed bootstrap do, and restarts with a bootstrap that completes or a
    // disconnect the application asked for. The close is skipped when the connection went away or ended meanwhile.
    private void abandonAndClose(Bootstrap b, Throwable err) {
        abandon(b);
        host.reportError(new TokenError(err));
        final WebSocket transport = b.transport;
        host.scheduler().schedule(
                () -> Extensions.submitOrIgnore(host.executor(), () -> onAbandonedClose(transport)),
                nextBootstrapBackoff(),
                TimeUnit.MILLISECONDS);
    }


    private void onAbandonedClose(WebSocket transport) {
        if (host.transport() == transport && host.state() == ClientState.CONNECTING) {
            host.closeTransport();
        }
    }


    private void onExchanged(
            Bootstrap b,
            String refreshToken,
            Throwable err,
            TokenExchangeResult result
    ) {
        cancelTimer(b);
        // A result which arrives after its connection went away, or after the bootstrap gave up, does not bootstrap, and its
        // outcome changes no run: only its rotation is stored.
        if (!isCurrent(b)) {
            storeAbandonedRotation(result, refreshToken);
            return;
        }
        if (err == null && isAccepted(result)) {
            acceptedExchange(result);
        } else if (err instanceof UnauthorizedException) {
            openRefusalRun();
        }
        try {
            storeRotation(result, refreshToken);
        } catch (UnauthorizedException e) {
            endBootstrapSession();
            return;
        }
        if (err instanceof UnauthorizedException) {
            reloadAfterRefusal(b, refreshToken);
            return;
        }
        if (err != null) {
            failBootstrap(b, err);
            return;
        }
        if (result == null || result.getToken() == null || result.getToken().isEmpty()) {
            failBootstrap(b, new IllegalStateException("token exchange returned no access token"));
            return;
        }
        refreshBootstrap(b, result.getToken());
    }


    // The issuer refused the refresh token: the refusal run goes on. The storage is read again first: a changed token is
    // presented at once, an empty one ends the session, and the same one is presented again once, at a random moment
    // inside the remaining grace; a second refusal of an unchanged value, or one at the horizon, ends the session. A read
    // which throws UnauthorizedException ends the session; another failed read is retried.
    private void reloadAfterRefusal(Bootstrap b, String refreshToken) {
        String again;
        try {
            again = exchange.load();
        } catch (UnauthorizedException e) {
            endBootstrapSession();
            return;
        } catch (RuntimeException e) {
            failBootstrap(b, e);
            return;
        }
        if (isEmptyToken(again)) {
            endBootstrapSession();
            return;
        }
        if (!again.equals(refreshToken)) {
            runExchange(b, again);
            return;
        }
        if (horizonPassed() || refusals >= REFUSAL_RETRIES) {
            endBootstrapSession();
            return;
        }
        host.reportError(new TokenError(new IllegalStateException(REFUSED_RETRY_MESSAGE)));
        long delay = ThreadLocalRandom.current().nextLong(Math.max(0, horizon.getDelay(TimeUnit.MILLISECONDS)) + 1);
        refusals++;
        scheduleRetry(b, delay);
    }


    // The session ends: the run is closed, and the bootstrap backoff starts afresh with the next connect.
    private void endBootstrapSession() {
        bootstrapAttempt = 0;
        closeRefusalRun();
        host.failUnauthorized();
    }


    // Opens the refusal run at its first refusal. Its horizon is the grace, timed on the scheduler's own monotonic clock.
    // Called for a current attempt only: a refusal of a connection which went away, or which arrives after the application
    // disconnected or closed the client, belongs to no session and opens no run.
    private void openRefusalRun() {
        if (horizon == null) {
            horizon = host.scheduler().schedule(() -> {}, graceSeconds, TimeUnit.SECONDS);
        }
    }


    // Whether the horizon of the open run has passed. No open run has no horizon left: an application which closed the
    // client while its storage was read closes the run inside that read.
    private boolean horizonPassed() {
        return horizon == null || horizon.getDelay(TimeUnit.MILLISECONDS) <= 0;
    }


    // Ends the refusal run: its horizon is cancelled and its count of refused attempts starts again at zero.
    private void closeRefusalRun() {
        if (horizon != null) {
            horizon.cancel(false);
            horizon = null;
        }
        refusals = 0;
    }


    // An accepted exchange ends the refusal run. The grace is the one the result published, kept for the next run; a result
    // without one (the absent marker) falls back to the option's grace, not to the last published one. Any other negative
    // value is invalid and keeps the grace in use.
    private void acceptedExchange(TokenExchangeResult result) {
        closeRefusalRun();
        if (result == null || result.getGrace() == ABSENT_GRACE) {
            graceSeconds = clampGrace(exchange.grace());
        } else if (result.getGrace() >= 0) {
            graceSeconds = clampGrace(result.getGrace());
        }
    }


    private static boolean isEmptyToken(String token) {
        return token == null || token.isEmpty();
    }


    // Whether a result which came without an error carries an access token. One which does not is a failure of the
    // exchange: it leaves an open refusal run open, as a thrown error does.
    private static boolean isAccepted(TokenExchangeResult result) {
        return result != null && !isEmptyToken(result.getToken());
    }


    // Sends the refresh command with the access token over the bootstrap's socket. The token is set just before the
    // command, and cleared again when the server refuses it.
    private void refreshBootstrap(final Bootstrap b, final String accessToken) {
        host.storeToken(accessToken);
        b.tokenSet = true;
        Protocol.Command.Builder builder = Protocol.Command.newBuilder()
                .setRefresh(Protocol.RefreshRequest.newBuilder().setToken(accessToken));
        writePlain(host, b.transport, builder).whenComplete((Protocol.Reply reply, Throwable err) -> {
            host.runOrInline(() -> onRefreshed(b, reply, err));
        });
    }


    private void onRefreshed(Bootstrap b, Protocol.Reply reply, Throwable err) {
        if (!isCurrent(b)) {
            return;
        }
        Throwable failure = err != null ? Extensions.unwrapCompletion(err) : null;
        if (failure == null && reply.getError().getCode() != 0) {
            failure = host.replyError(reply);
        }
        if (failure == null) {
            finishBootstrap(b, reply.getRefresh());
            return;
        }
        host.storeToken("");
        if (!(failure instanceof ReplyError)) {
            // The outcome is unknown: the server may have bound the connection to the access token. The connection is
            // closed after the backoff, so the reconnect bootstraps on a socket which carries no identity.
            abandonAndClose(b, failure);
            return;
        }
        ReplyError replyError = (ReplyError) failure;
        if (AuthRefresh.isRetryableRefresh(replyError)) {
            host.reportError(replyError);
            scheduleBootstrapRetry(b);
            return;
        }
        bootstrapAttempt = 0;
        host.disconnect(replyError.getCode(), replyError.getMessage());
    }


    // The bootstrap is done: the connect resumes with the reply merged with the refresh result and the held pushes.
    private void finishBootstrap(Bootstrap b, Protocol.RefreshResult refresh) {
        bootstrap = null;
        bootstrapAttempt = 0;
        cancelTimer(b);
        host.cancelPing();
        host.resumeConnect(b.reply.toBuilder().setConnect(merge(b, refresh)).build());
    }


    private static Protocol.ConnectResult merge(Bootstrap b, Protocol.RefreshResult refresh) {
        Protocol.ConnectResult connect = b.reply.getConnect();
        Protocol.ConnectResult.Builder builder = connect.toBuilder();
        if (refresh != null) {
            builder.setExpires(refresh.getExpires())
                    .setTtl(refresh.getTtl())
                    .setUser(refresh.getUser())
                    .setAuthClass(refresh.getAuthClass());
        }
        Map<String, Protocol.SubscribeResult> subs = new LinkedHashMap<>(connect.getSubsMap());
        subs.keySet().removeAll(b.heldUnsubs);
        subs.putAll(b.heldSubs);
        builder.clearSubs().putAllSubs(subs);
        return builder.build();
    }


    private static Protocol.SubscribeResult subscribeResultOf(Protocol.Subscribe sub) {
        return Protocol.SubscribeResult.newBuilder()
                .setRecoverable(sub.getRecoverable())
                .setEpoch(sub.getEpoch())
                .setOffset(sub.getOffset())
                .setPositioned(sub.getPositioned())
                .setData(sub.getData())
                .build();
    }


    // A bootstrap failed in a way which is retried: the error is reported and the exchange runs again after the backoff.
    private void failBootstrap(Bootstrap b, Throwable err) {
        host.reportError(new TokenError(err));
        scheduleBootstrapRetry(b);
    }


    private void scheduleBootstrapRetry(final Bootstrap b) {
        scheduleRetry(b, nextBootstrapBackoff());
    }


    // The delay of the next failed bootstrap step, which counts one more step. The steps escalate across reconnects.
    private long nextBootstrapBackoff() {
        long delay = backoff.duration(bootstrapAttempt, BOOTSTRAP_RETRY_MIN_MILLIS, BOOTSTRAP_RETRY_MAX_MILLIS);
        bootstrapAttempt++;
        return delay;
    }


    private void scheduleRetry(final Bootstrap b, long delay) {
        b.timer = host.scheduler().schedule(() -> Extensions.submitOrIgnore(host.executor(), () -> onRetry(b)), delay, TimeUnit.MILLISECONDS);
    }


    private void onRetry(Bootstrap b) {
        b.timer = null;
        if (!isCurrent(b)) {
            return;
        }
        if (!startExchange(b)) {
            finishBootstrap(b, null);
        }
    }


    // Whether the bootstrap is still the one of the connection which is connecting.
    private boolean isCurrent(Bootstrap b) {
        return bootstrap == b && host.transport() == b.transport && host.state() == ClientState.CONNECTING;
    }


    private static void cancelTimer(Bootstrap b) {
        if (b.timer != null) {
            b.timer.cancel(false);
            b.timer = null;
        }
        if (b.exchangeTimeout != null) {
            b.exchangeTimeout.cancel(false);
            b.exchangeTimeout = null;
        }
    }


    // Gives up a bootstrap, when its connection is lost or its exchange timed out: its timers stop, its access token is
    // cleared, and nothing of it runs again. The retry backoff is kept.
    private void abandon(Bootstrap b) {
        cancelTimer(b);
        if (b.tokenSet) {
            host.storeToken("");
        }
        bootstrap = null;
    }


    // Hands a rotated refresh token to the application, before the access token of the result is used. The rotation is
    // stored only while the application still holds the refresh token the exchange used: a newer token from another
    // exchange or from the application, and a logout which cleared the storage, are never overwritten by it. A failed read
    // or store is reported and the exchange goes on: the issuer already consumed the pair. An UnauthorizedException of the
    // read is thrown, and ends the session of a current attempt.
    private void storeRotation(TokenExchangeResult result, String refreshToken) throws UnauthorizedException {
        if (result == null) {
            return;
        }
        String rotated = result.getRefreshToken();
        if (rotated == null || rotated.isEmpty() || rotated.equals(refreshToken)) {
            return;
        }
        String current;
        try {
            current = exchange.load();
        } catch (RuntimeException e) {
            host.reportError(new TokenError(e));
            return;
        }
        if (!refreshToken.equals(current)) {
            return;
        }
        try {
            exchange.store(rotated);
        } catch (RuntimeException e) {
            host.reportError(new TokenError(e));
        }
    }


    // The rotation of a stale attempt: an exchange whose connection went away, which timed out, or which a newer attempt
    // superseded. It is stored by the rule of storeRotation, and its access token is not used. An UnauthorizedException of
    // the read is dropped with the outcome: a stale attempt never ends the session.
    private void storeAbandonedRotation(TokenExchangeResult result, String refreshToken) {
        try {
            storeRotation(result, refreshToken);
        } catch (UnauthorizedException e) {
            // A stale attempt does not end the session: its rotation is dropped with its outcome.
        }
    }


    // The connection token of the timer refresh: an exchange over the connected socket. Runs on the executor.
    private void renew(TokenCallback cb) {
        WebSocket transport = host.transport();
        // The renewal is superseded by a refresh() call made after this point: its outcome is then a stale one.
        final long generation = host.refreshGeneration();
        String refreshToken;
        try {
            refreshToken = exchange.load();
        } catch (UnauthorizedException e) {
            closeRefusalRun();
            cb.Done(e, null);
            return;
        } catch (RuntimeException e) {
            cb.Done(e, null);
            return;
        }
        if (isEmptyToken(refreshToken)) {
            closeRefusalRun();
            cb.Done(new UnauthorizedException(), null);
            return;
        }
        runRenewal(transport, cb, refreshToken, generation);
    }


    // Runs one renewal exchange. The client timeout is its deadline: a result which comes after it is not delivered to the
    // callback, and changes no run; its rotation is stored as an abandoned one's, see storeAbandonedRotation.
    private void runRenewal(
            final WebSocket transport,
            final TokenCallback cb,
            final String refreshToken,
            final long generation
    ) {
        final Renewal renewal = new Renewal();
        try {
            renewal.deadline = host.scheduler().schedule(
                    () -> Extensions.submitOrIgnore(host.executor(), () -> onRenewalTimeout(cb, renewal)),
                    host.timeoutMillis(),
                    TimeUnit.MILLISECONDS);
            exchange.exchange(new TokenExchangeEvent(host, transport, refreshToken, refusalTypes), (Throwable err, TokenExchangeResult result) -> {
                if (renewal.called.compareAndSet(false, true)) {
                    host.runOrInline(() -> {
                        onRenewed(
                                renewal,
                                transport,
                                cb,
                                refreshToken,
                                generation,
                                Extensions.unwrapCompletion(err),
                                result
                        );
                    });
                } else {
                    // A late result is a stale attempt: its outcome changes no run, and only its rotation is stored.
                    host.runOrInline(() -> storeAbandonedRotation(result, refreshToken));
                }
            });
        } catch (RuntimeException e) {
            if (renewal.called.compareAndSet(false, true)) {
                onRenewed(renewal, transport, cb, refreshToken, generation, e, null);
            } else {
                host.reportError(new TokenError(e));
            }
        }
    }


    // The deadline of a renewal exchange came first: the callback fails, and the late result is not delivered.
    private void onRenewalTimeout(TokenCallback cb, Renewal renewal) {
        if (renewal.called.compareAndSet(false, true)) {
            cb.Done(new TimeoutException("token exchange timed out"), null);
        }
    }


    private void onRenewed(
            Renewal renewal,
            WebSocket transport,
            TokenCallback cb,
            String refreshToken,
            long generation,
            Throwable err,
            TokenExchangeResult result
    ) {
        if (renewal.deadline != null) {
            renewal.deadline.cancel(false);
        }
        // The renewal result of a connection which went away is not used, and its outcome changes no run; its rotation is
        // stored as the bootstrap's is.
        if (transport != host.transport() || host.state() != ClientState.CONNECTED) {
            storeAbandonedRotation(result, refreshToken);
            cb.Done(new IOException("connection lost"), null);
            return;
        }
        // A renewal superseded by a refresh() call is a stale attempt: the call's token decides, so its outcome, a refusal
        // or an empty token included, changes no run and ends no session. Its rotation is stored as the abandoned one's.
        if (generation != host.refreshGeneration()) {
            storeAbandonedRotation(result, refreshToken);
            cb.Done(new IOException("superseded by a refresh"), null);
            return;
        }
        if (err == null && isAccepted(result)) {
            acceptedExchange(result);
        } else if (err instanceof UnauthorizedException) {
            openRefusalRun();
        }
        try {
            storeRotation(result, refreshToken);
        } catch (UnauthorizedException e) {
            closeRefusalRun();
            cb.Done(e, null);
            return;
        }
        if (err instanceof UnauthorizedException) {
            String again;
            try {
                again = exchange.load();
            } catch (UnauthorizedException e) {
                closeRefusalRun();
                cb.Done(e, null);
                return;
            } catch (RuntimeException e) {
                cb.Done(e, null);
                return;
            }
            if (isEmptyToken(again)) {
                closeRefusalRun();
                cb.Done(new UnauthorizedException(), null);
                return;
            }
            if (!again.equals(refreshToken)) {
                runRenewal(transport, cb, again, generation);
                return;
            }
            if (horizonPassed() || refusals >= REFUSAL_RETRIES) {
                closeRefusalRun();
                cb.Done(new UnauthorizedException(), null);
                return;
            }
            // Within the grace: the fetch fails once with a retryable error, and the next fetch reads the storage again. The
            // refusal of an unchanged value after that retry ends the session.
            refusals++;
            cb.Done(new IllegalStateException(REFUSED_RETRY_MESSAGE), null);
            return;
        }
        if (err != null) {
            cb.Done(err, null);
            return;
        }
        if (result == null || result.getToken() == null || result.getToken().isEmpty()) {
            cb.Done(new IllegalStateException("token exchange returned no access token"), null);
            return;
        }
        cb.Done(null, result.getToken());
    }
}
