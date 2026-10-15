package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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

    // Server error code of an expired connection token, retried with a fresh exchange.
    private static final int ERROR_CODE_TOKEN_EXPIRED = 109;

    // Error codes below this one are client errors, which a bootstrap retries.
    private static final int SERVER_ERROR_CODE_MIN = 100;


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


        Bootstrap(WebSocket transport, Protocol.Reply reply) {
            this.transport = transport;
            this.reply = reply;
        }
    }


    private final ClientHost host;
    private final TokenExchange exchange;
    private final ConnectionTokenGetter renewalGetter;
    private final Backoff backoff = new Backoff();
    private Expiry expiry;
    // Set by beforeConnect when the connect command carries no token: the next connect reply is then bootstrapped.
    private boolean anonymousConnect = false;
    private Bootstrap bootstrap;
    private int bootstrapAttempt = 0;


    /** Rejects a client which sets both a token exchange and a token getter. Called by the client constructor. */
    static void checkExclusive(Options opts) {
        if (opts.getTokenExchange() != null && opts.getTokenGetter() != null) {
            throw new IllegalArgumentException("tokenExchange and tokenGetter are exclusive");
        }
    }


    TokenExchangeFlow(ClientHost host, TokenExchange exchange) {
        this.host = host;
        this.exchange = exchange;
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
        anonymousConnect = host.connectionToken().isEmpty();
    }


    /**
     * Called with the connect reply before the connected state. Returns true when the exchange took the reply
     * over: the connect resumes with the merged reply later. False when the connect goes on at once, which is
     * the case for a connect with a token, for a second call of the same connect, and for an empty refresh token.
     */
    boolean interceptConnect(Protocol.Reply reply) {
        if (!anonymousConnect) {
            return false;
        }
        anonymousConnect = false;
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
    void observe(String token, boolean expires, int ttl) {
        if (!expires || token == null || token.isEmpty()) {
            expiry = null;
            return;
        }
        expiry = new Expiry(token, System.currentTimeMillis() + ttl * 1000L);
    }


    /** The connection is gone: the bootstrap is abandoned and its retry is cancelled. The expiry record is kept. */
    void reset() {
        if (bootstrap != null) {
            cancelTimer(bootstrap);
            bootstrap = null;
        }
        bootstrapAttempt = 0;
        anonymousConnect = false;
    }


    // Starts the exchange of a bootstrap. Returns false when the refresh token is empty: the connect then goes on
    // anonymously. Returns true when the connect waits for the exchange or for a retry of it.
    private boolean startExchange(Bootstrap b) {
        String refreshToken;
        try {
            refreshToken = exchange.load();
        } catch (RuntimeException e) {
            failBootstrap(b, e);
            return true;
        }
        if (refreshToken == null || refreshToken.isEmpty()) {
            return false;
        }
        runExchange(b, refreshToken, false);
        return true;
    }


    // Runs one exchange of the bootstrap. The callback hops back to the client's executor. Reloaded marks the
    // retry after a re-read of a changed refresh token: it is not retried again.
    private void runExchange(final Bootstrap b, final String refreshToken, final boolean reloaded) {
        try {
            exchange.exchange(new TokenExchangeEvent(host, b.transport, refreshToken), (Throwable err, TokenExchangeResult result) -> host.runOrInline(() -> onExchanged(b, refreshToken, reloaded, err, result)));
        } catch (RuntimeException e) {
            onExchanged(b, refreshToken, reloaded, e, null);
        }
    }


    private void onExchanged(Bootstrap b, String refreshToken, boolean reloaded, Throwable err, TokenExchangeResult result) {
        // A result which arrives after its connection went away is dropped, rotation included: it is handled as a
        // reply lost with the connection, and the backend's grace covers the pair.
        if (!isCurrent(b)) {
            return;
        }
        storeRotation(result, refreshToken);
        if (err instanceof UnauthorizedException) {
            reloadAfterRefusal(b, refreshToken, reloaded);
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


    // The issuer refused the refresh token. The token is read again: a changed one is tried once more, anything
    // else ends the session.
    private void reloadAfterRefusal(Bootstrap b, String refreshToken, boolean reloaded) {
        if (reloaded) {
            host.failUnauthorized();
            return;
        }
        String again;
        try {
            again = exchange.load();
        } catch (RuntimeException e) {
            failBootstrap(b, e);
            return;
        }
        if (again == null || again.isEmpty() || again.equals(refreshToken)) {
            host.failUnauthorized();
            return;
        }
        runExchange(b, again, true);
    }


    // Sends the refresh command with the access token over the bootstrap's socket. The token is set just before the
    // command, and cleared again when the server refuses it.
    private void refreshBootstrap(final Bootstrap b, final String accessToken) {
        host.storeToken(accessToken);
        Protocol.Command.Builder builder = Protocol.Command.newBuilder()
                .setRefresh(Protocol.RefreshRequest.newBuilder().setToken(accessToken));
        writePlain(host, b.transport, builder).whenComplete((Protocol.Reply reply, Throwable err) -> host.runOrInline(() -> onRefreshed(b, reply, err)));
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
            host.reportError(new TokenError(failure));
            scheduleBootstrapRetry(b);
            return;
        }
        ReplyError replyError = (ReplyError) failure;
        if (replyError.isTemporary() || replyError.getCode() < SERVER_ERROR_CODE_MIN || replyError.getCode() == ERROR_CODE_TOKEN_EXPIRED) {
            host.reportError(new RefreshError(replyError));
            scheduleBootstrapRetry(b);
            return;
        }
        host.disconnect(replyError.getCode(), replyError.getMessage());
    }


    // The bootstrap is done: the connect resumes with the reply merged with the refresh result and the held pushes.
    private void finishBootstrap(Bootstrap b, Protocol.RefreshResult refresh) {
        bootstrap = null;
        bootstrapAttempt = 0;
        cancelTimer(b);
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
        long delay = backoff.duration(bootstrapAttempt, BOOTSTRAP_RETRY_MIN_MILLIS, BOOTSTRAP_RETRY_MAX_MILLIS);
        bootstrapAttempt++;
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
    }


    // Hands a rotated refresh token to the application, before the access token of the result is used. A failure
    // is reported and the exchange goes on: the issuer already consumed the pair.
    private void storeRotation(TokenExchangeResult result, String refreshToken) {
        if (result == null) {
            return;
        }
        String rotated = result.getRefreshToken();
        if (rotated == null || rotated.isEmpty() || rotated.equals(refreshToken)) {
            return;
        }
        try {
            exchange.store(rotated);
        } catch (RuntimeException e) {
            host.reportError(new TokenError(e));
        }
    }


    // The connection token of the timer refresh: an exchange over the connected socket. Runs on the executor.
    private void renew(TokenCallback cb) {
        WebSocket transport = host.transport();
        String refreshToken;
        try {
            refreshToken = exchange.load();
        } catch (RuntimeException e) {
            cb.Done(e, null);
            return;
        }
        if (refreshToken == null || refreshToken.isEmpty()) {
            cb.Done(new UnauthorizedException(), null);
            return;
        }
        runRenewal(transport, cb, refreshToken, false);
    }


    private void runRenewal(final WebSocket transport, final TokenCallback cb, final String refreshToken, final boolean reloaded) {
        try {
            exchange.exchange(new TokenExchangeEvent(host, transport, refreshToken), (Throwable err, TokenExchangeResult result) -> host.runOrInline(() -> onRenewed(transport, cb, refreshToken, reloaded, err, result)));
        } catch (RuntimeException e) {
            onRenewed(transport, cb, refreshToken, reloaded, e, null);
        }
    }


    private void onRenewed(WebSocket transport, TokenCallback cb, String refreshToken, boolean reloaded, Throwable err, TokenExchangeResult result) {
        // The renewal result of a connection which went away is dropped, as the bootstrap drops it.
        if (transport != host.transport() || host.state() != ClientState.CONNECTED) {
            cb.Done(new IOException("connection lost"), null);
            return;
        }
        storeRotation(result, refreshToken);
        if (err instanceof UnauthorizedException) {
            if (reloaded) {
                cb.Done(err, null);
                return;
            }
            String again;
            try {
                again = exchange.load();
            } catch (RuntimeException e) {
                cb.Done(e, null);
                return;
            }
            if (again == null || again.isEmpty() || again.equals(refreshToken)) {
                cb.Done(new UnauthorizedException(), null);
                return;
            }
            runRenewal(transport, cb, again, true);
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


    /**
     * Writes a plain command on the transport and completes with its reply. The command carries no message
     * id, is not retransmitted, and is not held for the connected state. Fails with IOException when the
     * transport is not the current one, or when it refuses the command. Runs on the executor.
     */
    static CompletableFuture<Protocol.Reply> writePlain(final ClientHost host, final WebSocket transport, Protocol.Command.Builder builder) {
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


    /** Sends a plain rpc call over the transport and hands its outcome to the callback. Runs on the executor. */
    static void sendRpc(final ClientHost host, final WebSocket transport, String method, byte[] data, final ResultCallback<RPCResult> cb) {
        Protocol.Command.Builder builder = Protocol.Command.newBuilder()
                .setRpc(Protocol.RPCRequest.newBuilder()
                        .setMethod(method)
                        .setData(com.google.protobuf.ByteString.copyFrom(data)));
        writePlain(host, transport, builder).whenComplete((Protocol.Reply reply, Throwable err) -> host.runOrInline(() -> deliverRpc(host, reply, err, cb)));
    }


    private static void deliverRpc(ClientHost host, Protocol.Reply reply, Throwable err, ResultCallback<RPCResult> cb) {
        if (err != null) {
            cb.onDone(Extensions.unwrapCompletion(err), null);
            return;
        }
        if (reply.getError().getCode() != 0) {
            cb.onDone(host.replyError(reply), null);
            return;
        }
        RPCResult result = new RPCResult();
        result.setData(reply.getRpc().getData().toByteArray());
        cb.onDone(null, result);
    }
}
