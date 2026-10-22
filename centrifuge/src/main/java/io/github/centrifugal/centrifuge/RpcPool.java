package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;
import okhttp3.Authenticator;
import okhttp3.Callback;
import okhttp3.CookieJar;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;


/**
 * A pool of connections for calls which carry large payloads: several plain connections per endpoint,
 * a per-connection window of calls, and a bounded queue over the pool. A call goes to the endpoints of
 * its endpoint key, which the application names; the pool keeps the connections, and resends a call
 * which its connection lost, which the server refused for now, or which the server moved.
 *
 * <p>The calls are plain: a pool connection has no instance, so a call carries no msg_id. The pool resends
 * a call which its connection lost on a fresh connection, except in three cases where the outcome is known: the
 * connection closed for a frame over its size limit, or as unauthorized, or the connection was retired or went
 * stale (for example after an account switch). Those calls settle with the loss error and are not sent again.
 * So a call may run more than once when the server commits it and the reply is lost. The methods
 * of the calls must be idempotent; a method which is not should use an exactly-once call on the main client
 * instead. All the state
 * of the pool is confined to one thread of its own, and the callbacks run on that thread, as the
 * callbacks of a {@link Client} run on its executor. The one exception is a call made after {@link #close()}:
 * its callback runs at once on the calling thread, with {@link ReplyError#CANCELLED}.
 */
public final class RpcPool {

    /**
     * Client-local code of a call which the pool refuses: a bad option, a header the pool sets itself, or an endpoint key
     * which resolves to no endpoint. A server never sends this code.
     */
    public static final int BAD_CONFIGURATION = 12;

    /**
     * Client-local code of a redirect which could not be fetched: a network failure, a status which is not 2xx, a body over
     * its limit, or a URL the endpoint does not allow. A server never sends this code.
     */
    public static final int REDIRECT_FAILED = 18;

    /** Resolves an endpoint key to the endpoints of its connections. */
    public interface EndpointResolver {
        /**
         * The endpoints of the key, each a {@code ws://} or {@code wss://} URL, at least one. The pool
         * calls it on its thread once per key, until the pool is closed, and a key which resolves to
         * nothing rejects its calls with {@link RpcPool#BAD_CONFIGURATION}. The pool's thread waits for
         * the resolver, so it must not block.
         */
        List<String> endpoints(String endpointKey);
    }


    /** Recognises a reply or an error which moves the bytes of a call to another path. */
    public interface RedirectHook {
        /**
         * The redirect of the outcome of an attempt, or null when the outcome is no redirect. Called
         * once for the outcome which settles the call, a reply or an error, but not for a cancel, a
         * close, or a deadline: the deadline of the call and the timeout of its attempt.
         */
        Redirect redirect(RPCResult reply, Throwable error);
    }


    /**
     * Receives the disconnects of the connections which the client closed for good: a frame over the size limit of
     * the server, or another close after which the client does not reconnect.
     */
    public interface ErrorListener {
        void onError(String endpoint, Throwable error);
    }


    /** The bytes of a redirect, fetched over HTTP: the URL, its request headers, and the body bound. */
    public static final class Redirect {
        /** Body bytes accepted at most, when the redirect sets none. */
        public static final long DEFAULT_MAX_BYTES = 16L * 1024L * 1024L;

        private final String url;
        private final Map<String, String> headers;
        private final long maxBytes;


        private Redirect(Builder builder) {
            this.url = builder.url;
            this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
            this.maxBytes = builder.maxBytes;
        }


        /** The redirect to the URL. */
        public static Builder builder(String url) {
            return new Builder(url);
        }


        /** The URL the bytes are fetched from. */
        public String getUrl() {
            return url;
        }


        /** The request headers of the fetch, in the order they were added. */
        public Map<String, String> getHeaders() {
            return headers;
        }


        /** Body bytes accepted at most. */
        public long getMaxBytes() {
            return maxBytes;
        }


        public static final class Builder {
            private final String url;
            private final Map<String, String> headers = new LinkedHashMap<>();
            private long maxBytes = DEFAULT_MAX_BYTES;


            private Builder(String url) {
                this.url = Objects.requireNonNull(url, "redirect url");
            }


            /**
             * Adds a request header of the fetch. Adding the same key again replaces its value. The header goes to the
             * redirect URL, the one request the pool makes: no redirect is followed, so a 3xx answer fails the fetch.
             */
            public Builder header(String key, String value) {
                this.headers.put(Objects.requireNonNull(key, "header key"), Objects.requireNonNull(value, "header value"));
                return this;
            }


            /** Body bytes accepted at most, from 1 to {@link Integer#MAX_VALUE}. */
            public Builder maxBytes(long maxBytes) {
                if (maxBytes <= 0 || maxBytes > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("maxBytes must be between 1 and " + Integer.MAX_VALUE);
                }
                this.maxBytes = maxBytes;
                return this;
            }


            public Redirect build() {
                return new Redirect(this);
            }
        }
    }


    /** Per-call options of a pool call. */
    public static final class CallOptions {
        private static final CallOptions NONE = builder().build();

        private final String endpointKey;
        private final String affinity;
        private final Map<String, String> headers;
        private final long timeout;


        private CallOptions(Builder builder) {
            this.endpointKey = builder.endpointKey;
            this.affinity = builder.affinity;
            this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
            this.timeout = builder.timeout;
        }


        public static Builder builder() {
            return new Builder();
        }


        /** The endpoint key of the call, the empty string for a call without one. */
        public String getEndpointKey() {
            return endpointKey;
        }


        /** The affinity hint of the call, null when it has none. */
        public String getAffinity() {
            return affinity;
        }


        /** The headers of the call, in the order they were added. */
        public Map<String, String> getHeaders() {
            return headers;
        }


        /** The deadline of the call in milliseconds, 0 for the timeout of the pool. */
        public long getTimeout() {
            return timeout;
        }


        public static final class Builder {
            private String endpointKey = "";
            private String affinity;
            private final Map<String, String> headers = new LinkedHashMap<>();
            private long timeout;


            /** The endpoint key of the call, given to the {@link EndpointResolver}. */
            public Builder endpointKey(String endpointKey) {
                this.endpointKey = Objects.requireNonNull(endpointKey, "endpoint key");
                return this;
            }


            /** The affinity hint: a value the server issued, sent in the affinity header of the pool. */
            public Builder affinity(String affinity) {
                this.affinity = Objects.requireNonNull(affinity, "affinity");
                return this;
            }


            /** Adds a header of the call. Adding the same key again replaces its value. */
            public Builder header(String key, String value) {
                this.headers.put(Objects.requireNonNull(key, "header key"), Objects.requireNonNull(value, "header value"));
                return this;
            }


            /**
             * The deadline of the call in milliseconds, which holds across its resends and its redirect fetch. 0 takes the
             * timeout of the pool. A value that is negative, or longer than the connect timeout of the pool, is refused with
             * {@link RpcPool#BAD_CONFIGURATION} when the call is made.
             */
            public Builder timeout(long timeout) {
                this.timeout = timeout;
                return this;
            }


            public CallOptions build() {
                return new CallOptions(this);
            }
        }
    }


    /** A call of the pool, which can be cancelled. */
    public static final class Call {
        private final RpcPool pool;
        private final RpcPoolCall call;


        private Call(RpcPool pool, RpcPoolCall call) {
            this.pool = pool;
            this.call = call;
        }


        /**
         * Rejects the call with {@link ReplyError#CANCELLED} at once. A call still waiting for a connection is
         * never sent; a call on the wire is not cancelled on the server, and drains on its connection.
         */
        public void cancel() {
            pool.cancel(call);
        }
    }


    /** Counts of the pool at one moment. */
    public static final class Stats {
        private final int connections;
        private final int inflight;
        private final int queued;
        private final int redirects;


        Stats(int connections, int inflight, int queued, int redirects) {
            this.connections = connections;
            this.inflight = inflight;
            this.queued = queued;
            this.redirects = redirects;
        }


        /** Connections of the pool, connecting or connected. */
        public int getConnections() {
            return connections;
        }


        /** Calls on the wire, over all connections. */
        public int getInflight() {
            return inflight;
        }


        /**
         * Calls waiting for a connection with room. The calls waiting for a redirect fetch slot are not counted: their deadline
         * is their only bound.
         */
        public int getQueued() {
            return queued;
        }


        /** Redirect fetches in flight. */
        public int getRedirects() {
            return redirects;
        }
    }


    /**
     * Builds a pool. The defaults are safe for a server with the default settings: two connections per endpoint, one call
     * per connection at a time, 64 waiting calls, and three resends.
     */
    public static final class Builder {
        private final EndpointResolver resolver;
        private final Options options = new Options();
        // Whether token or tokenGetter was called: the pool's own token is exclusive with tokenFrom.
        private boolean ownToken;
        private Client tokenFrom;
        private String affinityHeader;
        private int maxConnectionsPerEndpoint = 2;
        private int maxInflightPerConnection = 1;
        private int maxQueued = 64;
        private long idleTimeout = 60_000L;
        private boolean removeCancelledTimers = true;
        private int retries = RpcSession.LATER_RESENDS;
        private Function<ReplyError, String> reroute;
        private RedirectHook redirect;
        private int maxRedirectsInflight = 4;
        private OkHttpClient okHttpClient;
        private Function<Response, Response> responseTap;
        private ErrorListener errorListener;
        private Random random;


        public Builder(EndpointResolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "endpoint resolver");
        }


        /** The token of the connections, see {@link Options#setToken}. Exclusive with {@link #tokenFrom(Client)}. */
        public Builder token(String token) {
            options.setToken(token);
            ownToken = true;
            return this;
        }


        /** The token getter of the connections, see {@link Options#setTokenGetter}. Exclusive with {@link #tokenFrom(Client)}. */
        public Builder tokenGetter(ConnectionTokenGetter tokenGetter) {
            options.setTokenGetter(tokenGetter);
            ownToken = true;
            return this;
        }


        /**
         * The main client whose token the connections use. Each connection gets the client's current access token, and when
         * its token expires or is refused, the client's newer one. The client renews itself when a connection is refused the
         * token the client still holds, and the connections never run the token exchange or the token getter themselves.
         * Works with every token mode of the client: a token exchange, a token getter or a static token. Exclusive with
         * {@link #token(String)} and {@link #tokenGetter(ConnectionTokenGetter)}, which build a pool without a client.
         *
         * <p>A pool and its client: the pool follows the client's connect and disconnect. A call made while the
         * client's session has ended as unauthorized fails at once, and a call made after the client connects again goes out.
         * After a logout the idle connections of the pool are not closed at once: each closes at {@code idleTimeout}.
         *
         * <p>The token is handed over unchanged, so the fleet must accept its claims as they are, including any channels or
         * subscriptions it carries. A fleet that refuses such a claim closes the connection with code 102: the pool reports the
         * reason through its error listener, and the calls end in a timeout, not as {@code token refused}. The token getter
         * route, {@link #tokenGetter(ConnectionTokenGetter)}, does not follow the client's session, and costs one getter call
         * per connect and per refresh.
         */
        public Builder tokenFrom(Client client) {
            this.tokenFrom = Objects.requireNonNull(client, "tokenFrom");
            return this;
        }


        /** The connect headers of the connections, see {@link Options#setHeaders}. */
        public Builder headers(Map<String, String> headers) {
            options.setHeaders(headers);
            return this;
        }


        /** The client name of the connections, see {@link Options#setName}. */
        public Builder name(String name) {
            options.setName(name);
            return this;
        }


        /** The client version of the connections, see {@link Options#setVersion}. */
        public Builder version(String version) {
            options.setVersion(version);
            return this;
        }


        /** The connect timeout of the connections, and the default deadline of a call, in milliseconds. */
        public Builder timeout(int timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            options.setTimeout(timeout);
            return this;
        }


        /**
         * The name of the header which carries {@link CallOptions#getAffinity()}. The header must be allow-listed
         * for the method on the server. Without it a call may not pass an affinity. The name is kept in lower case, as
         * the server matches it.
         */
        public Builder affinityHeader(String affinityHeader) {
            this.affinityHeader = Objects.requireNonNull(affinityHeader, "affinity header").toLowerCase(Locale.ROOT);
            return this;
        }


        /** Connections opened per endpoint at most, from 1. Default 2. */
        public Builder maxConnectionsPerEndpoint(int maxConnectionsPerEndpoint) {
            if (maxConnectionsPerEndpoint < 1) {
                throw new IllegalArgumentException("maxConnectionsPerEndpoint must be at least 1");
            }
            this.maxConnectionsPerEndpoint = maxConnectionsPerEndpoint;
            return this;
        }


        /** Calls in flight per connection at most, from 1. Default 1, which fits the server default of one command at a time. */
        public Builder maxInflightPerConnection(int maxInflightPerConnection) {
            if (maxInflightPerConnection < 1) {
                throw new IllegalArgumentException("maxInflightPerConnection must be at least 1");
            }
            this.maxInflightPerConnection = maxInflightPerConnection;
            return this;
        }


        /**
         * Calls waiting for a connection at most, over all endpoints, and each redirect fetch that waits for a fetch slot.
         * Beyond it a call is rejected with LIMIT_EXCEEDED. A fetch that finds a free slot is not refused. Default 64.
         */
        public Builder maxQueued(int maxQueued) {
            if (maxQueued < 0) {
                throw new IllegalArgumentException("maxQueued must not be negative");
            }
            this.maxQueued = maxQueued;
            return this;
        }


        /**
         * Closes a connection with no call for this long, in milliseconds; 0 keeps it. At most 2147483647, the limit of
         * a timer, which {@link #build()} checks. Default 60000.
         */
        public Builder idleTimeout(long idleTimeout) {
            if (idleTimeout < 0) {
                throw new IllegalArgumentException("idleTimeout must not be negative");
            }
            this.idleTimeout = idleTimeout;
            return this;
        }


        /**
         * Resends of a call, counted together over a lost connection, a later refusal and a reroute. Default 3,
         * the count of the client for a later refusal.
         */
        public Builder retries(int retries) {
            if (retries < 0) {
                throw new IllegalArgumentException("retries must not be negative");
            }
            this.retries = retries;
            return this;
        }


        /**
         * A server error which moves the call to another endpoint key: returns the key, or null when the error is
         * not a move. Called on the error replies of the server only, while the call has resends left.
         */
        public Builder reroute(Function<ReplyError, String> reroute) {
            this.reroute = reroute;
            return this;
        }


        /**
         * A reply or an error which moves the bytes to another path, see {@link RedirectHook}. On a plaintext endpoint,
         * a {@code ws://} URL, the redirect body and headers travel unprotected, and an on-path attacker can read them.
         */
        public Builder redirect(RedirectHook redirect) {
            this.redirect = redirect;
            return this;
        }


        // The seam of the tests: each redirect answer passes through it before its body is read, so a test can observe the close
        // of the body the pool releases. The public API has no setter for it.
        Builder responseTap(Function<Response, Response> responseTap) {
            this.responseTap = responseTap;
            return this;
        }


        /**
         * Redirect fetches in flight at most, over the pool, from 1. Default 4. The pool's peak for redirect bodies is this
         * count times 2 times the body limit, held briefly during a buffer growth step or the final copy.
         */
        public Builder maxRedirectsInflight(int maxRedirectsInflight) {
            if (maxRedirectsInflight < 1) {
                throw new IllegalArgumentException("maxRedirectsInflight must be at least 1");
            }
            this.maxRedirectsInflight = maxRedirectsInflight;
            return this;
        }


        /**
         * The OkHttp client of the connections, shared with the redirect fetches for its transport settings only: the
         * dispatcher, the connection pool, TLS, the proxy and DNS. A redirect fetch runs with none of its interceptors,
         * network interceptors, cookie jar or authenticator, so an application credential of this client is never sent to
         * a redirect URL. A credential of a fetch goes with {@link Redirect.Builder#header}. Optional: without one, the
         * redirect fetches share a default client of the process, and each connection builds its own.
         */
        public Builder okHttpClient(OkHttpClient okHttpClient) {
            this.okHttpClient = okHttpClient;
            options.setOkHttpClient(okHttpClient);
            return this;
        }


        /** Receives the errors of the connections. Optional. */
        public Builder errorListener(ErrorListener errorListener) {
            this.errorListener = errorListener;
            return this;
        }


        // The delays of the hold of an endpoint after its connection ends, which the tests set; the public API has no setter for them.
        Builder reconnectDelays(int minMillis, int maxMillis) {
            options.setMinReconnectDelay(minMillis);
            options.setMaxReconnectDelay(maxMillis);
            return this;
        }


        // The source of the random delays of the later resends, which the tests seed.
        Builder random(Random random) {
            this.random = random;
            return this;
        }


        // Whether the scheduler drops a cancelled timer at once. The tests turn it off to run the fallback of the
        // platforms below Android API 21, where a cancelled timer stays queued until its delay ends.
        Builder removeCancelledTimers(boolean removeCancelledTimers) {
            this.removeCancelledTimers = removeCancelledTimers;
            return this;
        }


        public RpcPool build() {
            if (tokenFrom != null && ownToken) {
                throw new IllegalArgumentException("tokenFrom is exclusive with token and tokenGetter");
            }
            if (idleTimeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("idleTimeout must be at most 2147483647");
            }
            return new RpcPool(this);
        }
    }


    // The outcome of placing a call on the key line: placed, settled, or waiting. A waiting call that may open a connection says so.
    private static final int PLACED = 0;
    private static final int FAILED = 1;
    private static final int STOP = 2;
    private static final int STOP_OPEN = 3;

    private static final Backoff BACKOFF = new Backoff();

    // How long the pool's thread stays without a task before it ends. A pending timer keeps it, whatever this is.
    private static final long POOL_THREAD_KEEP_ALIVE_MILLIS = 1000L;

    // The client of the redirect fetches of the pools without an OkHttp client of their own, created on the first fetch.
    private static OkHttpClient sharedHttpClient;

    private final EndpointResolver resolver;
    private final Options options;
    private final String affinityHeader;
    private final int maxConnectionsPerEndpoint;
    private final int maxInflightPerConnection;
    private final int maxQueued;
    private final long idleTimeoutMillis;
    private final boolean removeCancelledTimers;
    private final int retries;
    private final Function<ReplyError, String> reroute;
    private final RedirectHook redirectHook;
    private final int maxRedirectsInflight;
    private final OkHttpClient httpClient;
    private final Function<Response, Response> responseTap;
    private final ErrorListener errorListener;
    private final ScheduledThreadPoolExecutor executor;
    private final AtomicReference<Thread> poolThread = new AtomicReference<>();
    private final Random random;
    // The token source of the main client, null when the pool takes its token from its own options.
    private final TokenSource source;

    // The state below belongs to the pool's thread.
    // The waiting calls, one queue per endpoint key with calls waiting; a queue is dropped when it empties.
    private final Map<String, RpcPoolQueue> queues = new HashMap<>();
    // The calls waiting in all the queues, which the pool-wide bound counts.
    private int queuedCount;
    // The keys whose queues hold calls: the pool is listening to the main client while there is one.
    private final Set<String> waitingKeys = new HashSet<>();
    // The calls which have no outcome yet, to settle them on close.
    private final Set<RpcPoolCall> unsettled = new HashSet<>();
    // The endpoints of the keys which resolved, by key: cached until the pool is closed.
    private final Map<String, List<RpcPoolEndpoint>> resolved = new HashMap<>();
    private final Map<String, RpcPoolEndpoint> endpoints = new HashMap<>();
    // The connections taken out of their endpoints, which close once their last call settles.
    private final Set<RpcPoolConnection> retiring = new HashSet<>();
    // The connections with no call, in the order their idle clocks started: the head is the one which expires first.
    private final Set<RpcPoolConnection> idleSet = new LinkedHashSet<>();
    // The calls whose redirect waits for a fetch slot, in order. They count in the queued calls of the pool.
    private final RpcPoolQueue redirectWaiting = new RpcPoolQueue();
    // The one check of the idle connections, armed while there may be one; never cancelled, so it leaves no cancelled timer.
    private ScheduledFuture<?> idleCheck;
    private OkHttpClient fetchHttpClient;
    private int redirectsInflight;
    // The key lines the pool examined, for the tests of the lookups.
    private long keyLookups;
    // The calls resent after their connection was lost, counted for the tests.
    private int lostResends;
    private boolean closed;


    /**
     * Sends the call to an endpoint of its key, and returns it. The callback gets the reply or the error:
     * a {@link ReplyError} of the server or of the pool, {@link java.util.concurrent.TimeoutException} at the deadline,
     * or an {@link IOException} when the connection of the call was lost and the call could not be sent again. A
     * {@link RuntimeException} the endpoint resolver throws reaches the callback as it was thrown.
     * The data is read when the call is sent, which can be after it waited in the queue: the array must not change
     * until the callback.
     *
     * @throws NullPointerException when the method, the data or the callback is null.
     */
    public Call rpc(String method, byte[] data, CallOptions callOptions, ResultCallback<RPCResult> cb) {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(cb, "callback");
        CallOptions opts = callOptions != null ? callOptions : CallOptions.NONE;
        // 0 is not given: the pool's timeout. Any other value is checked on the pool's thread, with the other admission checks.
        long timeout = opts.getTimeout() != 0 ? opts.getTimeout() : options.getTimeout();
        final RpcPoolCall call = new RpcPoolCall(method, data, opts.getEndpointKey(), opts.getHeaders(), opts.getAffinity(), timeout, cb);
        // A call made while the main client is connected is bound to its identity here, on the caller's thread: an identity
        // change between this call and the pool's thread does not move it.
        if (source != null && source.mainState() == TokenSource.Phase.CONNECTED) {
            call.generation = source.generation();
        }
        Extensions.submitOrRun(executor, () -> start(call), () -> call.callback.onDone(cancelled(), null));
        return new Call(this, call);
    }


    /**
     * The counts of the pool at this moment. Called from a callback of the pool, it reads the counts at once,
     * and from any other thread it waits for the pool's thread.
     */
    public Stats stats() {
        if (Thread.currentThread() == poolThread.get()) {
            return statsOnPool();
        }
        try {
            return executor.submit(this::statsOnPool).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Stats(0, 0, 0, 0);
        } catch (ExecutionException | RejectedExecutionException e) {
            return new Stats(0, 0, 0, 0);
        }
    }


    /**
     * Closes every connection and rejects the calls: the calls waiting for a connection with
     * {@link ReplyError#CANCELLED}, the calls on the wire with an {@link IOException}. The pool takes no
     * calls afterwards.
     */
    public void close() {
        Extensions.submitOrIgnore(executor, this::closeOnPool);
    }


    // Called on the pool's thread by a call's cancel.
    void cancel(RpcPoolCall call) {
        Extensions.submitOrIgnore(executor, () -> settle(call, null, cancelled()));
    }


    // Runs the task on the pool's thread, unless the pool is closed.
    void post(Runnable task) {
        Extensions.submitOrIgnore(executor, task);
    }


    // The main client's event reached a pool which has calls in a key line: each key with calls waiting is drained, and no other.
    void onMainEvent() {
        if (closed || waitingKeys.isEmpty()) {
            return;
        }
        for (String key : waitingKeys.toArray(new String[0])) {
            pumpKey(key);
        }
    }


    // The number of key lines the pool examined, read on its thread. For the tests.
    long keyLookups() throws Exception {
        return executor.submit(() -> keyLookups).get();
    }


    // The number of calls the pool resent after a connection lost them, read on its thread. For the tests.
    int lostResends() throws Exception {
        return executor.submit(() -> lostResends).get();
    }


    // The number of key queues the pool holds, read on its thread. For the tests.
    int queueKeys() throws Exception {
        return executor.submit(() -> queues.size()).get();
    }


    // The transport of a connection is lost and its client reconnects. A stale connection is retired before its client reuses
    // the token it stored. The calls on the wire were lost before this event, and are decided here.
    void onConnecting(RpcPoolConnection conn) {
        if (conn.removed) {
            return;
        }
        conn.connecting = true;
        if (stale(conn)) {
            retire(conn);
        }
        // The client drops the replies of its old transport, so the calls on the wire are lost here, at the connection's event.
        conn.lost.addAll(conn.calls);
        conn.calls.clear();
        releaseLost(conn);
        pumpEndpoint(conn.endpoint);
    }


    // The connection is connected. Without a token source it is live at once. With one, the identity the server confirmed must
    // be the one the pool recorded for the connection's generation: a connection of another identity is retired, and one of a
    // generation which ended is retired silently.
    void onConnected(RpcPoolConnection conn, String user, String authClass) {
        if (conn.removed) {
            return;
        }
        if (source != null) {
            TokenSource.Stamp stamp = source.stamp();
            if (conn.generation < 0 || conn.generation != stamp.generation || !stamp.recorded) {
                retire(conn);
                pumpEndpoint(conn.endpoint);
                return;
            }
            if (!user.equals(stamp.user) || !authClass.equals(stamp.authClass)) {
                // The token leads to another identity than the main client's: the token is unusable until the main client confirms another.
                source.unusable(conn.token);
                retire(conn);
                report(conn.endpoint.url(), new IOException("identity mismatch"));
                pumpEndpoint(conn.endpoint);
                return;
            }
        }
        conn.connecting = false;
        conn.endpoint.closes = 0;
        releaseLost(conn);
        pumpEndpoint(conn.endpoint);
    }


    // The connection is gone for good: the client does not reconnect it. A retired connection is removed with no error and no
    // hold. A frame over the size limit settles the calls with no resend, since the chunk would be over the limit again, and
    // opens no replacement. Any other close is reported, and holds the opens of the endpoint back for a backoff delay.
    void onDisconnected(RpcPoolConnection conn, int code, String reason) {
        if (conn.removed) {
            return;
        }
        // The calls which the client has not reported yet are lost with the connection: no answer will come for them.
        conn.lost.addAll(conn.calls);
        conn.calls.clear();
        // The client's close reason is the error of the calls which the close loses.
        conn.closeReason = reason;
        // A size-limit or unauthorized end settles its lost calls with the loss error and no resend, whatever the token state: a
        // chunk over the limit would be over it again, and a connection that ended as unauthorized cannot carry the call.
        conn.noResend = code == Client.DISCONNECTED_MESSAGE_SIZE_LIMIT || code == Client.DISCONNECTED_UNAUTHORIZED;
        // A stale connection's end is its next event: it is retired, with no error, no hold and no drain of its endpoint.
        if (conn.retired || stale(conn)) {
            retire(conn);
            remove(conn);
            releaseLost(conn);
            return;
        }
        if (code == Client.DISCONNECTED_MESSAGE_SIZE_LIMIT) {
            report(conn.endpoint.url(), new IOException(reason));
            remove(conn);
            releaseLost(conn);
            pumpEndpoint(conn.endpoint);
            return;
        }
        if (code == Client.DISCONNECTED_DISCONNECT_CALLED) {
            remove(conn);
            releaseLost(conn);
            pumpEndpoint(conn.endpoint);
            return;
        }
        report(conn.endpoint.url(), new IOException(reason));
        remove(conn);
        hold(conn.endpoint);
        releaseLost(conn);
        pumpEndpoint(conn.endpoint);
    }


    // Decides the calls whose attempt the connection lost, on the connection's event: a message-size close or an unauthorized
    // close settles them, a retired or stale connection settles them, and any other loss sends them again while the bound of
    // the call allows.
    private void releaseLost(RpcPoolConnection conn) {
        if (!conn.lost.isEmpty()) {
            List<RpcPoolCall> lost = new ArrayList<>(conn.lost);
            conn.lost.clear();
            // The loss error of the calls: the close reason of the client, or the fixed text when the connection did not end.
            String lossReason = conn.closeReason != null ? conn.closeReason : "rpc connection lost";
            for (RpcPoolCall call : lost) {
                call.connection = null;
                if (call.settled) {
                    continue;
                }
                // A call lost on a connection that ended over its size limit or as unauthorized, or on a retired or stale connection,
                // is not sent again, whatever the token state: it was written and its outcome is unknown, so the loss error is the
                // truthful answer. The calls of a stale connection are also bound to an older identity, which the token state does
                // not concern.
                if (conn.noResend || conn.retired || stale(conn)) {
                    finishAttempt(call, null, new IOException(lossReason));
                } else if (canResend(call)) {
                    call.resends++;
                    lostResends++;
                    resend(call, false);
                } else {
                    finishAttempt(call, null, new IOException(lossReason));
                }
            }
        }
        refreshIdle(conn);
    }


    private static boolean idle(RpcPoolConnection conn) {
        return conn.calls.isEmpty() && conn.lost.isEmpty();
    }


    private RpcPool(Builder builder) {
        this.resolver = builder.resolver;
        this.options = builder.options;
        this.affinityHeader = builder.affinityHeader;
        this.maxConnectionsPerEndpoint = builder.maxConnectionsPerEndpoint;
        this.maxInflightPerConnection = builder.maxInflightPerConnection;
        this.maxQueued = builder.maxQueued;
        this.idleTimeoutMillis = builder.idleTimeout;
        this.removeCancelledTimers = builder.removeCancelledTimers;
        this.retries = builder.retries;
        this.reroute = builder.reroute;
        this.redirectHook = builder.redirect;
        this.maxRedirectsInflight = builder.maxRedirectsInflight;
        this.httpClient = builder.okHttpClient;
        if (builder.okHttpClient == null) {
            // The connections of a pool given no client share the process's one, so each shares its dispatcher and pool.
            this.options.setOkHttpClient(defaultHttpClient());
        }
        this.responseTap = builder.responseTap;
        this.errorListener = builder.errorListener;
        this.random = builder.random != null ? builder.random : new Random();
        this.source = builder.tokenFrom != null ? builder.tokenFrom.auth.tokenSource() : null;
        this.executor = createExecutor();
    }


    // The pool's thread is created with the first task, and is a daemon: an idle pool keeps the process alive
    // no more than the clients of its connections do. The thread times out when no task is left, so a pool that is
    // never closed is collected once its calls settle and its connections close: a live thread would keep it reachable.
    // A pending timer keeps the thread alive, since the queue is not empty.
    private ScheduledThreadPoolExecutor createExecutor() {
        ThreadFactory factory = (Runnable runnable) -> {
            Thread thread = new Thread(runnable, "centrifuge-rpc-pool");
            thread.setDaemon(true);
            poolThread.set(thread);
            return thread;
        };
        ScheduledThreadPoolExecutor executor = newScheduler(factory, removeCancelledTimers);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setKeepAliveTime(POOL_THREAD_KEEP_ALIVE_MILLIS, TimeUnit.MILLISECONDS);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }


    // A scheduler of one thread, whose cancelled timers leave its queue at once: a cancelled deadline must not keep its call
    // alive until its delay ends. The policy is missing below Android API 21, where the cancelled timers stay queued.
    private static ScheduledThreadPoolExecutor newScheduler(ThreadFactory factory, boolean removeCancelled) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, factory);
        if (!removeCancelled) {
            return scheduler;
        }
        try {
            scheduler.setRemoveOnCancelPolicy(true);
        } catch (NoSuchMethodError e) {
            // The policy is not in this platform's runtime: the cancelled timers stay queued until their delay ends.
        }
        return scheduler;
    }


    // The error of a call which the client cancelled: the same error on every path of the pool.
    private static ReplyError cancelled() {
        return new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false);
    }


    // Holds the opens of the endpoint for the next backoff delay, which grows with each close until a connection connects.
    private void hold(RpcPoolEndpoint endpoint) {
        long delayMillis = BACKOFF.duration(endpoint.closes, options.getMinReconnectDelay(), options.getMaxReconnectDelay());
        endpoint.closes++;
        if (endpoint.holdTask != null) {
            endpoint.holdTask.cancel(false);
        }
        endpoint.holdTask = executor.schedule(() -> onHoldEnd(endpoint), delayMillis, TimeUnit.MILLISECONDS);
    }


    private void onHoldEnd(RpcPoolEndpoint endpoint) {
        endpoint.holdTask = null;
        pumpEndpoint(endpoint);
    }


    // Admission of a call, in a fixed order: closed, the main client's ended and disconnected states, the options, the key,
    // and then the placement or the queue bound.
    private void start(RpcPoolCall call) {
        if (closed) {
            settle(call, null, cancelled());
            return;
        }
        if (source != null) {
            TokenSource.Phase phase = source.mainState();
            if (phase == TokenSource.Phase.ENDED) {
                settle(call, null, new IOException("unauthorized"));
                return;
            }
            if (phase == TokenSource.Phase.DISCONNECTED) {
                settle(call, null, new IOException("client disconnected"));
                return;
            }
        }
        if (call.affinity != null && affinityHeader == null) {
            settle(call, null, badConfiguration("affinity needs the affinityHeader option"));
            return;
        }
        if (affinityHeader != null && hasHeaderIgnoreCase(call.headers, affinityHeader)) {
            settle(call, null, badConfiguration("affinity header is given as a header of the call"));
            return;
        }
        if (call.timeoutMillis <= 0) {
            settle(call, null, badConfiguration("call timeout must be positive"));
            return;
        }
        if (call.timeoutMillis > options.getTimeout()) {
            settle(call, null, badConfiguration("call timeout is over the connection timeout"));
            return;
        }
        List<RpcPoolEndpoint> eps;
        try {
            eps = resolveKey(call.endpointKey);
        } catch (RuntimeException e) {
            settle(call, null, e);
            return;
        }
        if (eps == null) {
            settle(call, null, badConfiguration("endpoint key has no endpoint"));
            return;
        }
        unsettled.add(call);
        // The timer runs on the deadline of the call, which started at its submit: the time it waited for the pool's thread is spent.
        call.deadlineTask = executor.schedule(() -> onDeadline(call), call.remainingNanos(), TimeUnit.NANOSECONDS);
        RpcPoolQueue line = queues.get(call.endpointKey);
        // A call goes out at once only when no call of its key waits: the waiting calls are older, and go first.
        if (line == null || line.isEmpty()) {
            int outcome = place(call, eps);
            if (outcome == PLACED || outcome == FAILED) {
                return;
            }
        }
        // The queue bound applies to a call which cannot open a connection: a call which can is queued and opens one.
        if (queuedCount >= maxQueued && openable(eps) == null) {
            settle(call, null, new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls are waiting", false));
            return;
        }
        enqueue(call, false);
        pumpKey(call.endpointKey);
    }


    private static boolean hasHeaderIgnoreCase(Map<String, String> headers, String name) {
        for (String key : headers.keySet()) {
            if (key.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }


    // A connection event of an endpoint: only the queues of its keys with calls waiting can have a call to place. An
    // endpoint with no waiting call costs one check, and nothing is copied for it. The walk copies the keys, since a
    // pass empties the queues it places from, and is bounded by the calls waiting, which maxQueued bounds.
    private void pumpEndpoint(RpcPoolEndpoint endpoint) {
        Set<String> keys = endpoint.queuedKeys();
        if (keys.isEmpty()) {
            return;
        }
        for (String key : keys.toArray(new String[0])) {
            pumpKey(key);
        }
    }


    // Places the waiting calls of one key, in order, and stops at the first call which can be neither placed nor settled. The
    // calls of a key share its endpoints, so a call which must wait ends the pass: none of the later calls can go either. The
    // connection the waiting call needs is opened here, when the placement allows it.
    private void pumpKey(String endpointKey) {
        keyLookups++;
        RpcPoolQueue queue = queues.get(endpointKey);
        // A queued call's key has resolved: it resolved when the call was started or rerouted, and the resolution is kept.
        List<RpcPoolEndpoint> eps = resolved.get(endpointKey);
        if (closed || queue == null || queue.isEmpty() || eps == null) {
            return;
        }
        while (!queue.isEmpty()) {
            RpcPoolCall call = queue.first();
            int outcome = place(call, eps);
            if (outcome == PLACED || outcome == FAILED) {
                continue;
            }
            if (outcome == STOP_OPEN) {
                RpcPoolEndpoint endpoint = openable(eps);
                if (endpoint != null) {
                    // The call waits for the connection to connect: the connection's event pumps the key again.
                    open(endpoint);
                }
            }
            return;
        }
    }


    // Places one call by the main client's state and the binding of the call. A call which the main client's state refuses is
    // settled with that state's error; a call bound to another identity is settled as the identity changed.
    private int place(RpcPoolCall call, List<RpcPoolEndpoint> eps) {
        if (source == null) {
            return tryAssign(call, eps) ? PLACED : STOP_OPEN;
        }
        switch (source.mainState()) {
            case ENDED:
                settle(call, null, new IOException("unauthorized"));
                return FAILED;
            case DISCONNECTED:
                settle(call, null, new IOException("client disconnected"));
                return FAILED;
            case CONNECTING:
                // Not confirmed yet: a bound call goes on a live connection of its identity only, and an unbound call waits.
                if (call.generation < 0) {
                    return STOP;
                }
                if (call.generation != source.generation()) {
                    settle(call, null, new IOException("identity changed"));
                    return FAILED;
                }
                return tryAssign(call, eps) ? PLACED : STOP;
            default:
                // Confirmed: an unbound call takes the identity now, and a call bound to another identity fails.
                if (call.generation < 0) {
                    call.generation = source.generation();
                } else if (call.generation != source.generation()) {
                    settle(call, null, new IOException("identity changed"));
                    return FAILED;
                }
                retireStale(eps);
                if (tryAssign(call, eps)) {
                    return PLACED;
                }
                if (source.canLend()) {
                    return STOP_OPEN;
                }
                if (source.isSpent() && !hasLive(eps, call.generation)) {
                    // The fleet refused the token and no connection of the identity is left to take the call.
                    settle(call, null, new IOException("token refused"));
                    return FAILED;
                }
                return STOP;
        }
    }


    // Whether a connection of the endpoints is live for the identity: connected and verified, and stamped with the generation.
    private static boolean hasLive(List<RpcPoolEndpoint> eps, int generation) {
        for (RpcPoolEndpoint endpoint : eps) {
            for (RpcPoolConnection conn : endpoint.connections()) {
                if (!conn.connecting && conn.generation == generation) {
                    return true;
                }
            }
        }
        return false;
    }


    // The endpoints of the key, or null when the resolver names none that is a WebSocket URL. A resolver which
    // throws leaves the throw to the caller, and the failed key is not cached: the next call resolves it again.
    private List<RpcPoolEndpoint> resolveKey(String endpointKey) {
        List<RpcPoolEndpoint> cached = resolved.get(endpointKey);
        if (cached != null) {
            return cached;
        }
        List<String> urls = resolver.endpoints(endpointKey);
        if (urls == null || urls.isEmpty()) {
            return null;
        }
        List<RpcPoolEndpoint> eps = new ArrayList<>();
        for (String url : urls) {
            if (url == null || !isWebSocketUrl(url)) {
                return null;
            }
            RpcPoolEndpoint endpoint = endpoints.get(url);
            if (endpoint == null) {
                endpoint = new RpcPoolEndpoint(url);
                endpoints.put(url, endpoint);
            }
            if (!eps.contains(endpoint)) {
                eps.add(endpoint);
            }
        }
        resolved.put(endpointKey, eps);
        return eps;
    }


    private static boolean isWebSocketUrl(String url) {
        String lower = url.toLowerCase(Locale.ROOT);
        return lower.startsWith("ws://") || lower.startsWith("wss://");
    }


    // Sends the call on the live connection with the fewest calls among those with room, if there is one. With a token source
    // the connection must carry the call's identity: a connection which is connecting takes no call.
    private boolean tryAssign(RpcPoolCall call, List<RpcPoolEndpoint> eps) {
        RpcPoolConnection best = null;
        for (RpcPoolEndpoint endpoint : eps) {
            for (RpcPoolConnection conn : endpoint.connections()) {
                if (conn.connecting || conn.calls.size() >= maxInflightPerConnection) {
                    continue;
                }
                if (source != null && conn.generation != call.generation) {
                    continue;
                }
                if (best == null || conn.calls.size() < best.calls.size()) {
                    best = conn;
                }
            }
        }
        if (best == null) {
            return false;
        }
        attach(call, best);
        return true;
    }


    // The first endpoint of the list which may open a connection now, or null when none may. The opens of an endpoint are
    // serialised, and with a token source a connection opens only while the main client is connected and can lend its token.
    private RpcPoolEndpoint openable(List<RpcPoolEndpoint> eps) {
        if (source != null && !source.canLend()) {
            return null;
        }
        for (RpcPoolEndpoint endpoint : eps) {
            if (mayOpen(endpoint)) {
                return endpoint;
            }
        }
        return null;
    }


    private boolean mayOpen(RpcPoolEndpoint endpoint) {
        return endpoint.connections().size() < maxConnectionsPerEndpoint && !endpoint.hasConnecting() && !endpoint.isHeld();
    }


    // Opens a connection to the endpoint: the client, its listeners, the endpoint, then the connect. Its idle clock starts now,
    // so a connection whose calls all time out while it connects is closed after the idle timeout.
    private void open(RpcPoolEndpoint endpoint) {
        RpcPoolConnection conn = new RpcPoolConnection(this, endpoint, options, source);
        endpoint.connections().add(conn);
        markIdle(conn);
        conn.connect();
    }


    private void attach(RpcPoolCall call, RpcPoolConnection conn) {
        unqueue(call);
        conn.calls.add(call);
        idleSet.remove(conn);
        call.connection = conn;
        call.endpointUrl = conn.endpoint.url();
        RpcOptions.Builder builder = new RpcOptions.Builder().timeout(call.remainingMillis());
        for (Map.Entry<String, String> header : call.headers.entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }
        if (call.affinity != null) {
            builder.header(affinityHeader, call.affinity);
        }
        conn.client.rpcCall(call.method, call.data, builder.build(), (Throwable error, RPCResult result) -> {
            post(() -> onAttemptDone(call, conn, result, error));
        });
    }


    // The answer of an attempt, on the pool's thread. The slot of the call is given back first, so the pump sees it.
    private void onAttemptDone(RpcPoolCall call, RpcPoolConnection conn, RPCResult result, Throwable error) {
        if (!conn.calls.remove(call)) {
            // The attempt was given up with its connection, which settled the call.
            pumpEndpoint(conn.endpoint);
            return;
        }
        call.connection = null;
        if (!call.settled) {
            if (error == null) {
                finishAttempt(call, result, null);
            } else if (error instanceof ReplyError) {
                onReplyError(call, (ReplyError) error);
            } else if (error instanceof IOException) {
                // The loss is not decided here: the client posts its losses before its events, and the event of the
                // connection says whether the call is sent again. The call keeps the connection until then.
                call.connection = conn;
                conn.lost.add(call);
                // The client reports a loss after the events of its transport, possibly after the connection connected again. A
                // retired or removed connection has no further event to decide the loss, and its outcome is known. A connection
                // which is connected again has lost the transport the call was on, which is the reconnect the loss waits for.
                if (conn.removed || conn.retired || !conn.connecting) {
                    releaseLost(conn);
                }
            } else {
                finishAttempt(call, null, error);
            }
        }
        // A connection whose token went stale leaves its endpoint now, and a retired one closes once it is idle.
        if (stale(conn)) {
            retire(conn);
        }
        refreshIdle(conn);
        pumpEndpoint(conn.endpoint);
    }


    // Whether the connection holds a token of an identity which has ended: the main client's generation moved on since.
    private boolean stale(RpcPoolConnection conn) {
        return source != null && conn.generation >= 0 && conn.generation != source.generation();
    }


    // Retires the stale connections of the endpoints, before a call is placed on them. Walks each list backwards, as a retire
    // takes its connection out of the list.
    private void retireStale(List<RpcPoolEndpoint> eps) {
        if (source == null) {
            return;
        }
        for (RpcPoolEndpoint endpoint : eps) {
            List<RpcPoolConnection> connections = endpoint.connections();
            for (int i = connections.size() - 1; i >= 0; i--) {
                RpcPoolConnection conn = connections.get(i);
                if (stale(conn)) {
                    retire(conn);
                }
            }
        }
    }


    // Takes a connection out of its endpoint, so that no call goes to it, and records that its lost calls are not sent again.
    // It closes at once when it has no call, and otherwise when its last call settles. Retiring is not an error of the connection.
    private void retire(RpcPoolConnection conn) {
        if (conn.removed || conn.retired) {
            return;
        }
        conn.retired = true;
        conn.endpoint.connections().remove(conn);
        retiring.add(conn);
        source.forget(conn);
        refreshIdle(conn);
    }


    // The token getter of a pool connection, which runs on the client's threads. It asks the source with the token the connection
    // was given last, and never takes a token of another identity: a connection of an ended identity is retired and fails.
    void tokenOf(RpcPoolConnection conn, TokenCallback cb) {
        int known = conn.generation;
        if (known >= 0 && known != source.generation()) {
            post(() -> retire(conn));
            cb.Done(new UnauthorizedException(), null);
            return;
        }
        // The attempt the request serves. An answer that comes after the client started another attempt is dropped by the client,
        // so it records no token: the token of a refusal is one the connection presented.
        int attempt = conn.attempt;
        source.token(conn, conn.token, (Throwable error, String token, int delivered) -> {
            if (error != null) {
                cb.Done(error, null);
                return;
            }
            // An answer for an attempt the client abandoned records nothing: neither its token nor its generation.
            if (conn.attempt != attempt) {
                cb.Done(null, token);
                return;
            }
            if (conn.generation < 0) {
                conn.generation = delivered;
            } else if (conn.generation != delivered) {
                post(() -> retire(conn));
                cb.Done(new UnauthorizedException(), null);
                return;
            }
            // Recorded at delivery to the client's current connect attempt, because the client offers no hook at its connect
            // frame. An attempt the client abandons before its connect is replaced by the next attempt's delivery, so a refusal
            // may name a token whose connect never ran. That costs at most one extra renewal, which the token source's
            // one-renewal rule bounds.
            conn.token = token;
            cb.Done(null, token);
        });
    }


    private void onReplyError(RpcPoolCall call, ReplyError error) {
        if (canResend(call)) {
            // A move the reroute hook maps wins over the later signal: the call goes to the new key at once.
            if (reroute != null && isServerError(error)) {
                String key = applyReroute(call, error);
                if (key != null) {
                    call.endpointKey = key;
                    call.resends++;
                    routeFirst(call);
                    return;
                }
            }
            if (Extensions.RPC_RETRY_LATER.equals(retrySignal(error))) {
                scheduleLater(call);
                return;
            }
        }
        finishAttempt(call, null, error);
    }


    // The retry value of an error of a reply, or null when the server set none: the same rule as the client's retrySignal.
    private static String retrySignal(ReplyError error) {
        return error.getMetadata().get(Extensions.RPC_RETRY_METADATA_KEY);
    }


    private boolean canResend(RpcPoolCall call) {
        return call.resends < retries && call.remainingNanos() > 0L;
    }


    // The later refusal is sent again after the main client's delay. The draw index is the count of later resends, capped
    // at the count of the client's later delays, so a call with more retries waits at most as long as the client does.
    private void scheduleLater(RpcPoolCall call) {
        int draws = Math.min(call.laterResends, RpcSession.LATER_RESENDS - 1);
        call.resends++;
        call.laterResends++;
        long delayNanos = RpcSession.laterResendDelayNanos(random, draws, call.remainingNanos());
        call.resendTask = executor.schedule(() -> onResendDue(call), delayNanos, TimeUnit.NANOSECONDS);
    }


    private void onResendDue(RpcPoolCall call) {
        call.resendTask = null;
        if (!call.settled) {
            resend(call, false);
        }
    }


    // Sends a call again: a call which can never be placed settles now, by the main client's state or the binding that refuses
    // it, and any other joins its line, at the front for a reroute and at the back otherwise.
    private void resend(RpcPoolCall call, boolean first) {
        if (source != null) {
            TokenSource.Phase phase = source.mainState();
            if (phase == TokenSource.Phase.ENDED) {
                settle(call, null, new IOException("unauthorized"));
                return;
            }
            if (phase == TokenSource.Phase.DISCONNECTED) {
                settle(call, null, new IOException("client disconnected"));
                return;
            }
            if (call.generation >= 0 && call.generation != source.generation()) {
                settle(call, null, new IOException("identity changed"));
                return;
            }
            List<RpcPoolEndpoint> eps = resolved.get(call.endpointKey);
            if (phase == TokenSource.Phase.CONNECTED && source.isSpent() && eps != null && !hasLive(eps, source.generation())) {
                // The same refusal as a fresh call: the fleet refused the token and no connection of the identity can take the call.
                settle(call, null, new IOException("token refused"));
                return;
            }
        }
        enqueue(call, first);
        pumpKey(call.endpointKey);
    }


    // Queues the call in the queue of its key, at the back or, for a reroute, at the front.
    private void enqueue(RpcPoolCall call, boolean first) {
        RpcPoolQueue queue = queues.get(call.endpointKey);
        if (queue == null) {
            queue = new RpcPoolQueue();
            queues.put(call.endpointKey, queue);
        }
        boolean wasEmpty = queue.isEmpty();
        if (first) {
            queue.addFirst(call);
        } else {
            queue.addLast(call);
        }
        queuedCount++;
        if (wasEmpty) {
            setWaitingKey(call.endpointKey, true);
        }
    }


    // Takes the call out of its queue, if it waits in one. The key of a queue which empties stops being waited for, and its
    // queue is dropped, so that no key which nothing waits for keeps a queue.
    private void unqueue(RpcPoolCall call) {
        RpcPoolQueue queue = call.queue;
        if (queue != null) {
            queue.remove(call);
            queuedCount--;
            if (queue != redirectWaiting && queue.isEmpty()) {
                if (queues.get(call.endpointKey) == queue) {
                    queues.remove(call.endpointKey);
                }
                setWaitingKey(call.endpointKey, false);
            }
        }
    }


    // Records that a key gets its first waiting call, or its last one leaves. The key is on each endpoint it resolved to, and the
    // pool listens to the main client while any key waits.
    private void setWaitingKey(String endpointKey, boolean waiting) {
        List<RpcPoolEndpoint> eps = resolved.get(endpointKey);
        if (eps != null) {
            for (RpcPoolEndpoint endpoint : eps) {
                if (waiting) {
                    endpoint.addQueuedKey(endpointKey);
                } else {
                    endpoint.removeQueuedKey(endpointKey);
                }
            }
        }
        if (waiting) {
            boolean wasEmpty = waitingKeys.isEmpty();
            if (waitingKeys.add(endpointKey) && wasEmpty && source != null) {
                source.listen(this);
            }
        } else if (waitingKeys.remove(endpointKey) && waitingKeys.isEmpty() && source != null) {
            source.unlisten(this);
        }
    }


    // Resends a rerouted call at the front of its new key, after the key has resolved: a key which resolves to nothing
    // settles the call here, as a call of an unknown key does.
    private void routeFirst(RpcPoolCall call) {
        try {
            if (resolveKey(call.endpointKey) == null) {
                settle(call, null, badConfiguration("endpoint key has no endpoint"));
                return;
            }
        } catch (RuntimeException e) {
            settle(call, null, e);
            return;
        }
        resend(call, true);
    }


    // The outcome of an attempt which is final: a redirect when the hook names one, otherwise the outcome itself.
    private void finishAttempt(RpcPoolCall call, RPCResult result, Throwable error) {
        if (call.settled) {
            return;
        }
        // The client's own deadline of the attempt is not a server outcome: the hook is not asked about it.
        RpcPool.Redirect target = error instanceof TimeoutException ? null : consultRedirect(call, result, error);
        if (target == null) {
            settle(call, result, error);
            return;
        }
        startRedirect(call, target);
    }


    private void startRedirect(RpcPoolCall call, RpcPool.Redirect target) {
        if (!RpcPoolRedirect.allowed(call.endpointUrl, target.getUrl())) {
            settle(call, null, new ReplyError(REDIRECT_FAILED, "redirect url is not allowed for the endpoint", false));
            return;
        }
        call.redirect = target;
        if (redirectsInflight < maxRedirectsInflight) {
            fetch(call);
            return;
        }
        // A fetch which would wait counts in the pool-wide bound, as a call which waits for a connection does.
        if (queuedCount >= maxQueued) {
            settle(call, null, new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls are waiting", false));
            return;
        }
        redirectWaiting.addLast(call);
        queuedCount++;
    }


    // Requests the redirect URL, on the pool's thread. The answer comes back on the OkHttp thread. The client follows no
    // redirect: a 3xx answer fails the fetch with the redirect error, since the URL it names was never checked.
    private void fetch(RpcPoolCall call) {
        redirectsInflight++;
        try {
            Request request = RpcPoolRedirect.request(HttpUrl.get(call.redirect.getUrl()), call.redirect);
            final long maxBytes = call.redirect.getMaxBytes();
            call.redirectCall = fetchClient().newCall(request);
            call.redirectCall.enqueue(new Callback() {
                @Override
                public void onFailure(okhttp3.Call httpCall, IOException e) {
                    post(() -> onFetchDone(call, null, e));
                }


                @Override
                public void onResponse(okhttp3.Call httpCall, Response answer) {
                    Response response = answer;
                    byte[] body = null;
                    IOException failure = null;
                    Error fatal = null;
                    try {
                        response = responseTap != null ? responseTap.apply(answer) : answer;
                        body = RpcPoolRedirect.readBody(response, maxBytes);
                    } catch (IOException e) {
                        failure = e;
                    } catch (RuntimeException e) {
                        failure = new IOException("redirect body could not be read", e);
                    } catch (Error e) {
                        // An error, such as an out of memory, must not leave the fetch slot taken: the call fails like any read error,
                        // and the error is rethrown once the slot is back, so that the thread's handler sees it.
                        failure = new IOException("redirect body could not be read", e);
                        fatal = e;
                    } finally {
                        final byte[] bodyOnce = body;
                        final IOException failureOnce = failure;
                        post(() -> onFetchDone(call, bodyOnce, failureOnce));
                        response.close();
                    }
                    if (fatal != null) {
                        throw fatal;
                    }
                }
            });
        } catch (IllegalArgumentException e) {
            redirectsInflight--;
            settle(call, null, new ReplyError(REDIRECT_FAILED, "redirect could not be requested", false));
            startWaitingRedirects();
        }
    }


    private void onFetchDone(RpcPoolCall call, byte[] body, IOException failure) {
        redirectsInflight--;
        call.redirectCall = null;
        if (!call.settled) {
            if (failure != null) {
                // The read error stays the cause of the redirect error, for the application to see what broke. The message is fixed.
                ReplyError error = new ReplyError(REDIRECT_FAILED, "redirect failed", false);
                error.initCause(failure);
                settle(call, null, error);
            } else {
                RPCResult result = new RPCResult();
                result.setData(body);
                settle(call, result, null);
            }
        }
        startWaitingRedirects();
    }


    // Starts the waiting fetches in order while a slot is free. A settled call leaves its queue at once, so each one is live.
    private void startWaitingRedirects() {
        while (redirectsInflight < maxRedirectsInflight && !redirectWaiting.isEmpty()) {
            RpcPoolCall next = redirectWaiting.first();
            unqueue(next);
            fetch(next);
        }
    }


    private void onDeadline(RpcPoolCall call) {
        call.deadlineTask = null;
        settle(call, null, new TimeoutException("rpc call timed out"));
    }


    // Gives the call its outcome: stops its timers, takes it out of the queue, and calls back. A slot on the wire is
    // not given back here: the attempt gives it back when its answer comes.
    private void settle(RpcPoolCall call, RPCResult result, Throwable error) {
        if (call.settled) {
            return;
        }
        call.settled = true;
        unsettled.remove(call);
        if (call.deadlineTask != null) {
            call.deadlineTask.cancel(false);
            call.deadlineTask = null;
        }
        if (call.resendTask != null) {
            call.resendTask.cancel(false);
            call.resendTask = null;
        }
        unqueue(call);
        if (call.connection != null && call.connection.lost.remove(call)) {
            // A lost call waiting for its connection's event leaves the connection with its outcome.
            RpcPoolConnection conn = call.connection;
            call.connection = null;
            refreshIdle(conn);
        }
        if (call.redirectCall != null) {
            call.redirectCall.cancel();
            call.redirectCall = null;
        }
        ResultCallback<RPCResult> callback = call.callback;
        call.release();
        try {
            callback.onDone(error, result);
        } catch (RuntimeException e) {
            report(call.endpointUrl != null ? call.endpointUrl : "", e);
        }
    }


    private RpcPool.Redirect consultRedirect(RpcPoolCall call, RPCResult result, Throwable error) {
        if (redirectHook == null) {
            return null;
        }
        try {
            return redirectHook.redirect(result, error);
        } catch (RuntimeException e) {
            report(call.endpointUrl != null ? call.endpointUrl : "", e);
            return null;
        }
    }


    private String applyReroute(RpcPoolCall call, ReplyError error) {
        try {
            return reroute.apply(error);
        } catch (RuntimeException e) {
            report(call.endpointUrl != null ? call.endpointUrl : "", e);
            return null;
        }
    }


    // A reroute applies to an error of the server only: the codes the pool and the client give themselves are not moves.
    private static boolean isServerError(ReplyError error) {
        int code = error.getCode();
        return code != ReplyError.UNSUPPORTED
                && code != ReplyError.LIMIT_EXCEEDED
                && code != ReplyError.CANCELLED
                && code != BAD_CONFIGURATION
                && code != REDIRECT_FAILED;
    }


    // Brings a connection in line with its calls: one with no call is idle, from the moment its last call (or its open) let go of it,
    // and joins the idle set once, so that its clock is not restarted by the events in between. A retired one closes once idle.
    private void refreshIdle(RpcPoolConnection conn) {
        if (conn.removed) {
            return;
        }
        if (!idle(conn)) {
            idleSet.remove(conn);
            return;
        }
        if (conn.retired) {
            remove(conn);
            return;
        }
        if (!idleSet.contains(conn)) {
            markIdle(conn);
        }
    }


    // Starts the idle clock of a connection which has no call: it is the last in the idle set, and the one check is armed
    // when none is.
    private void markIdle(RpcPoolConnection conn) {
        conn.idleSince = System.nanoTime();
        idleSet.remove(conn);
        idleSet.add(conn);
        if (idleTimeoutMillis > 0L && idleCheck == null && !closed) {
            armIdleCheck(TimeUnit.MILLISECONDS.toNanos(idleTimeoutMillis));
        }
    }


    private void armIdleCheck(long delayNanos) {
        idleCheck = executor.schedule(this::onIdleCheck, delayNanos, TimeUnit.NANOSECONDS);
    }


    // Closes the connections of the idle set whose clock has run out, from the head of the set, and arms the check for the head
    // which is left. A fire costs the connections it closes, and one look at the head which is left.
    private void onIdleCheck() {
        idleCheck = null;
        if (closed) {
            return;
        }
        long now = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMillis);
        long nextNanos = -1L;
        List<RpcPoolConnection> expired = new ArrayList<>();
        for (RpcPoolConnection conn : idleSet) {
            long remaining = conn.idleSince + timeoutNanos - now;
            if (remaining > 0L) {
                nextNanos = remaining;
                break;
            }
            expired.add(conn);
        }
        for (RpcPoolConnection conn : expired) {
            remove(conn);
        }
        if (nextNanos >= 0L) {
            armIdleCheck(nextNanos);
        }
        for (RpcPoolConnection conn : expired) {
            pumpEndpoint(conn.endpoint);
        }
    }


    // Takes the connection out of its endpoint, and closes its client. Its calls on the wire keep their slots until
    // their answers come, which the client gives when it settles them.
    private void remove(RpcPoolConnection conn) {
        if (conn.removed) {
            return;
        }
        conn.removed = true;
        conn.endpoint.connections().remove(conn);
        retiring.remove(conn);
        idleSet.remove(conn);
        if (source != null) {
            source.forget(conn);
        }
        conn.close();
    }


    private void closeOnPool() {
        if (closed) {
            return;
        }
        closed = true;
        if (source != null) {
            source.unlisten(this);
        }
        waitingKeys.clear();
        for (RpcPoolCall call : new ArrayList<>(unsettled)) {
            // A call waiting for a redirect fetch slot counts as in flight: close settles it with the closed error, as it settles a
            // running fetch.
            boolean inFlight = call.connection != null || call.redirectCall != null || call.queue == redirectWaiting;
            settle(call, null, inFlight ? new IOException("rpc pool closed") : cancelled());
        }
        for (RpcPoolEndpoint endpoint : endpoints.values()) {
            for (RpcPoolConnection conn : new ArrayList<>(endpoint.connections())) {
                remove(conn);
            }
        }
        for (RpcPoolConnection conn : new ArrayList<>(retiring)) {
            remove(conn);
        }
        idleSet.clear();
        executor.shutdown();
    }


    // The delays of the timers the scheduler holds, in nanoseconds, for the tests of the timers.
    List<Long> timerDelays() {
        List<Long> delays = new ArrayList<>();
        for (Runnable task : executor.getQueue().toArray(new Runnable[0])) {
            if (task instanceof Delayed) {
                delays.add(((Delayed) task).getDelay(TimeUnit.NANOSECONDS));
            }
        }
        return delays;
    }


    private Stats statsOnPool() {
        int connections = 0;
        int inflight = 0;
        for (RpcPoolEndpoint endpoint : endpoints.values()) {
            for (RpcPoolConnection conn : endpoint.connections()) {
                connections++;
                inflight += conn.calls.size();
            }
        }
        for (RpcPoolConnection conn : retiring) {
            connections++;
            inflight += conn.calls.size();
        }
        return new Stats(connections, inflight, queuedCount, redirectsInflight);
    }


    // The client of the redirect fetches: the transport settings of the pool's client, and none of its application hooks. The
    // fetch follows no redirect, and carries no interceptor, cookie or credential of the application.
    private OkHttpClient fetchClient() {
        if (fetchHttpClient == null) {
            OkHttpClient base = httpClient != null ? httpClient : defaultHttpClient();
            OkHttpClient.Builder builder = base.newBuilder();
            builder.interceptors().clear();
            builder.networkInterceptors().clear();
            fetchHttpClient = builder
                    .cookieJar(CookieJar.NO_COOKIES)
                    .authenticator(Authenticator.NONE)
                    .followRedirects(false)
                    .followSslRedirects(false)
                    .build();
        }
        return fetchHttpClient;
    }


    // The client of the pools which were given none, shared by the process: its dispatcher and connection pool are not
    // left behind by each pool that is closed.
    private static synchronized OkHttpClient defaultHttpClient() {
        if (sharedHttpClient == null) {
            // A websocket holds a slot of its dispatcher for the whole connection, and OkHttp counts it against the total limit
            // of requests (64 by default), which would cap the connections of every pool in the process. The limits are lifted,
            // so the pool's own maxConnectionsPerEndpoint is the only cap.
            Dispatcher dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(Integer.MAX_VALUE);
            dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
            sharedHttpClient = new OkHttpClient.Builder().dispatcher(dispatcher).build();
        }
        return sharedHttpClient;
    }


    private void report(String endpoint, Throwable error) {
        if (errorListener == null) {
            return;
        }
        try {
            errorListener.onError(endpoint, error);
        } catch (RuntimeException ignored) {
            // A listener which throws does not stop the pool.
        }
    }


    private static ReplyError badConfiguration(String message) {
        return new ReplyError(BAD_CONFIGURATION, message, false);
    }
}
