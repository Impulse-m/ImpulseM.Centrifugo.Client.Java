package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import okhttp3.Callback;
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
 * <p>The calls are plain: a pool connection has no instance, so a call carries no msg_id and is never
 * executed twice by the server on purpose. The methods of the calls must be idempotent. All the state
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
         * nothing rejects its calls with {@link RpcPool#BAD_CONFIGURATION}.
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
             * Adds a request header of the fetch. Adding the same key again replaces its value. The header goes only to
             * the host and port of the redirect URL: a redirect to another host is requested without it.
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


            /** The deadline of the call in milliseconds, which holds across its resends and its redirect fetch. */
            public Builder timeout(long timeout) {
                if (timeout <= 0) {
                    throw new IllegalArgumentException("timeout must be positive");
                }
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
        private ErrorListener errorListener;
        private Random random;


        public Builder(EndpointResolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "endpoint resolver");
        }


        /** The token of the connections, see {@link Options#setToken}. */
        public Builder token(String token) {
            options.setToken(token);
            return this;
        }


        /** The token getter of the connections, see {@link Options#setTokenGetter}. */
        public Builder tokenGetter(ConnectionTokenGetter tokenGetter) {
            options.setTokenGetter(tokenGetter);
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


        /** Calls waiting for a connection at most, over all endpoints. Beyond it a call is rejected with LIMIT_EXCEEDED. Default 64. */
        public Builder maxQueued(int maxQueued) {
            if (maxQueued < 0) {
                throw new IllegalArgumentException("maxQueued must not be negative");
            }
            this.maxQueued = maxQueued;
            return this;
        }


        /** Closes a connection with no call for this long, in milliseconds; 0 keeps it. Default 60000. */
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


        /** A reply or an error which moves the bytes to another path, see {@link RedirectHook}. */
        public Builder redirect(RedirectHook redirect) {
            this.redirect = redirect;
            return this;
        }


        /** Redirect fetches in flight at most, over the pool, from 1. Default 4. */
        public Builder maxRedirectsInflight(int maxRedirectsInflight) {
            if (maxRedirectsInflight < 1) {
                throw new IllegalArgumentException("maxRedirectsInflight must be at least 1");
            }
            this.maxRedirectsInflight = maxRedirectsInflight;
            return this;
        }


        /**
         * The OkHttp client of the connections and of the redirect fetches, shared. Optional: without one, the redirect
         * fetches share a default client of the process, and each connection builds its own. The interceptors of this
         * client apply to the redirect fetches too, so an interceptor which adds credentials sends them to the host of
         * each hop.
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
            return new RpcPool(this);
        }
    }


    private static final Backoff BACKOFF = new Backoff();

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
    private final ErrorListener errorListener;
    private final ScheduledThreadPoolExecutor executor;
    private final AtomicReference<Thread> poolThread = new AtomicReference<>();
    private final Random random;

    // The state below belongs to the pool's thread.
    private final Deque<RpcPoolCall> queue = new ArrayDeque<>();
    // The calls which have no outcome yet, to settle them on close.
    private final Set<RpcPoolCall> unsettled = new HashSet<>();
    // The endpoints of the keys which resolved, by key: cached until the pool is closed.
    private final Map<String, List<RpcPoolEndpoint>> resolved = new HashMap<>();
    private final Map<String, RpcPoolEndpoint> endpoints = new HashMap<>();
    private final Deque<RpcPoolCall> redirectWaiting = new ArrayDeque<>();
    // The one check of the idle connections, armed while there may be one; never cancelled, so it leaves no cancelled timer.
    private ScheduledFuture<?> idleCheck;
    private OkHttpClient fetchHttpClient;
    private int redirectsInflight;
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
        long timeout = opts.getTimeout() > 0 ? opts.getTimeout() : options.getTimeout();
        final RpcPoolCall call = new RpcPoolCall(method, data, opts.getEndpointKey(), opts.getHeaders(), opts.getAffinity(), timeout, cb);
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


    void onConnecting(RpcPoolConnection conn) {
        if (!conn.removed) {
            conn.connecting = true;
            releaseLost(conn);
            pump();
        }
    }


    void onConnected(RpcPoolConnection conn) {
        if (!conn.removed) {
            conn.connecting = false;
            conn.endpoint.closes = 0;
            releaseLost(conn);
            pump();
            if (idle(conn)) {
                markIdle(conn);
            }
        }
    }


    // The connection is gone for good: the client does not reconnect it. A frame over the size limit settles
    // the calls on the wire with no resend, since the chunk would be over the limit again, and opens no replacement.
    // Any other close is reported, and holds the opens of the endpoint back for a backoff delay: the calls on the
    // wire are sent again as their attempts end, and a server which closes each connection at once is not dialled in a loop.
    void onDisconnected(RpcPoolConnection conn, int code, String reason) {
        if (conn.removed) {
            return;
        }
        report(conn.endpoint.url(), new IOException(reason));
        remove(conn);
        if (code == Client.DISCONNECTED_MESSAGE_SIZE_LIMIT) {
            conn.sizeLimited = true;
            conn.closeReason = reason;
            // The attempts of the client failed before this event, and are lost calls already; the rest are settled too.
            conn.lost.addAll(conn.calls);
            conn.calls.clear();
        } else {
            hold(conn.endpoint);
        }
        releaseLost(conn);
        pump();
    }


    // Decides the calls whose attempt the connection lost, on the connection's event: a message-size close settles
    // them, and any other loss sends them again while the bound of the call allows.
    private void releaseLost(RpcPoolConnection conn) {
        if (conn.lost.isEmpty()) {
            return;
        }
        List<RpcPoolCall> lost = new ArrayList<>(conn.lost);
        conn.lost.clear();
        for (RpcPoolCall call : lost) {
            call.connection = null;
            if (call.settled) {
                continue;
            }
            if (conn.sizeLimited) {
                finishAttempt(call, null, new IOException(conn.closeReason));
            } else if (canResend(call)) {
                call.resends++;
                enqueue(call);
            } else {
                finishAttempt(call, null, new IOException("rpc connection lost"));
            }
        }
    }


    // Whether the connection has no call on the wire and none waiting for its event: only then it may go idle.
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
        this.errorListener = builder.errorListener;
        this.random = builder.random != null ? builder.random : new Random();
        this.executor = createExecutor();
    }


    // The pool's thread is created with the first task, and is a daemon: an idle pool keeps the process alive
    // no more than the clients of its connections do.
    private ScheduledThreadPoolExecutor createExecutor() {
        ThreadFactory factory = (Runnable runnable) -> {
            Thread thread = new Thread(runnable, "centrifuge-rpc-pool");
            thread.setDaemon(true);
            poolThread.set(thread);
            return thread;
        };
        ScheduledThreadPoolExecutor executor = newScheduler(factory, removeCancelledTimers);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
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
        pump();
    }


    private void start(RpcPoolCall call) {
        if (closed) {
            settle(call, null, cancelled());
            return;
        }
        if (call.affinity != null && affinityHeader == null) {
            settle(call, null, badConfiguration("affinity needs the affinityHeader option"));
            return;
        }
        if (affinityHeader != null && hasHeaderIgnoreCase(call.headers, affinityHeader)) {
            settle(call, null, badConfiguration("affinity header is given as a header of the call"));
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
        // A call goes out at once only when no call waits: the waiting calls are older, and go first.
        if (queue.isEmpty() && tryAssign(call, eps)) {
            return;
        }
        // The queue bound applies to a call which cannot open a connection: a call which can is queued and opens one.
        if (queue.size() >= maxQueued && openable(eps) == null) {
            settle(call, null, new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls are waiting", false));
            return;
        }
        enqueue(call);
        pump();
    }


    private static boolean hasHeaderIgnoreCase(Map<String, String> headers, String name) {
        for (String key : headers.keySet()) {
            if (key.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }


    // Sends the queued calls, in order, to the connections with room, and opens the connections they need. Nothing in one pass
    // frees room, so a key which finds no room and no endpoint to open has none for its later calls either: they are skipped
    // without a search. A pool with no room at all is not searched, so a reply costs the connections of the pool, not the queue.
    private void pump() {
        if (closed || !anyRoom()) {
            return;
        }
        Set<String> blocked = new HashSet<>();
        for (RpcPoolCall call : new ArrayList<>(queue)) {
            if (call.settled) {
                queue.remove(call);
                call.queued = false;
                continue;
            }
            if (blocked.contains(call.endpointKey)) {
                continue;
            }
            List<RpcPoolEndpoint> eps;
            try {
                eps = resolveKey(call.endpointKey);
            } catch (RuntimeException e) {
                settle(call, null, e);
                continue;
            }
            if (eps == null) {
                settle(call, null, badConfiguration("endpoint key has no endpoint"));
                continue;
            }
            if (!tryAssign(call, eps)) {
                RpcPoolEndpoint endpoint = openable(eps);
                if (endpoint == null) {
                    blocked.add(call.endpointKey);
                } else {
                    open(endpoint);
                }
            }
        }
    }


    // Whether a connection has room for a call, or an endpoint may open one: when neither holds, no queued call can be sent.
    private boolean anyRoom() {
        for (RpcPoolEndpoint endpoint : endpoints.values()) {
            if (mayOpen(endpoint)) {
                return true;
            }
            for (RpcPoolConnection conn : endpoint.connections()) {
                if (conn.calls.size() < maxInflightPerConnection) {
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


    // Sends the call on the connection with the fewest calls among those with room, if there is one.
    private boolean tryAssign(RpcPoolCall call, List<RpcPoolEndpoint> eps) {
        RpcPoolConnection best = null;
        for (RpcPoolEndpoint endpoint : eps) {
            for (RpcPoolConnection conn : endpoint.connections()) {
                if (conn.calls.size() < maxInflightPerConnection && (best == null || conn.calls.size() < best.calls.size())) {
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


    // The first endpoint of the list which may open a connection now, or null when none may. The opens of an endpoint are serialised.
    private RpcPoolEndpoint openable(List<RpcPoolEndpoint> eps) {
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


    // A connection with no call is closed after the idle timeout from the open, so that a connection whose calls all
    // time out while it connects does not stay open.
    private void open(RpcPoolEndpoint endpoint) {
        RpcPoolConnection conn = new RpcPoolConnection(this, endpoint, options);
        endpoint.connections().add(conn);
        conn.connect();
        markIdle(conn);
    }


    private void attach(RpcPoolCall call, RpcPoolConnection conn) {
        if (call.queued) {
            queue.remove(call);
            call.queued = false;
        }
        conn.calls.add(call);
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
            pump();
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
                if (conn.removed) {
                    releaseLost(conn);
                }
            } else {
                finishAttempt(call, null, error);
            }
        }
        if (idle(conn)) {
            markIdle(conn);
        }
        pump();
    }


    private void onReplyError(RpcPoolCall call, ReplyError error) {
        if (canResend(call)) {
            if (Extensions.RPC_RETRY_LATER.equals(Extensions.retrySignal(error))) {
                scheduleLater(call);
                return;
            }
            if (reroute != null && isServerError(error)) {
                String key = applyReroute(call, error);
                if (key != null) {
                    call.endpointKey = key;
                    call.resends++;
                    queueFirst(call);
                    return;
                }
            }
        }
        finishAttempt(call, null, error);
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
            enqueue(call);
            pump();
        }
    }


    private void enqueue(RpcPoolCall call) {
        queue.add(call);
        call.queued = true;
    }


    private void queueFirst(RpcPoolCall call) {
        queue.addFirst(call);
        call.queued = true;
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
        } else {
            redirectWaiting.add(call);
            call.redirectWaiting = true;
        }
    }


    private void fetch(RpcPoolCall call) {
        redirectsInflight++;
        try {
            HttpUrl first = HttpUrl.get(call.redirect.getUrl());
            hop(call, first, first, RpcPoolRedirect.MAX_HOPS);
        } catch (IllegalArgumentException e) {
            redirectsInflight--;
            settle(call, null, new ReplyError(REDIRECT_FAILED, "redirect could not be requested", false));
            startWaitingRedirects();
        }
    }


    // Requests one hop of a fetch, on the pool's thread. The answer comes back on the OkHttp thread.
    private void hop(RpcPoolCall call, HttpUrl url, HttpUrl allowedUrl, int hopsLeft) {
        final String endpoint = call.endpointUrl;
        final long maxBytes = call.redirect.getMaxBytes();
        Request request = RpcPoolRedirect.request(url, allowedUrl, call.redirect);
        call.redirectCall = fetchClient().newCall(request);
        call.redirectCall.enqueue(new Callback() {
            @Override
            public void onFailure(okhttp3.Call httpCall, IOException e) {
                post(() -> onFetchDone(call, null, e));
            }


            @Override
            public void onResponse(okhttp3.Call httpCall, Response response) {
                if (RpcPoolRedirect.isRedirect(response)) {
                    onRedirectResponse(call, response, endpoint, allowedUrl, hopsLeft);
                    return;
                }
                byte[] body = null;
                IOException failure = null;
                try {
                    body = RpcPoolRedirect.readBody(response, maxBytes);
                } catch (IOException e) {
                    failure = e;
                } catch (RuntimeException e) {
                    failure = new IOException("redirect body could not be read", e);
                } finally {
                    response.close();
                }
                final byte[] bodyOnce = body;
                final IOException failureOnce = failure;
                post(() -> onFetchDone(call, bodyOnce, failureOnce));
            }
        });
    }


    // Runs on the OkHttp thread: checks the URL the redirect names, and hands the next hop to the pool's thread.
    private void onRedirectResponse(RpcPoolCall call, Response response, String endpoint, HttpUrl allowedUrl, int hopsLeft) {
        HttpUrl next = null;
        IOException failure = null;
        try {
            if (hopsLeft <= 0) {
                throw new IOException("redirects are longer than " + RpcPoolRedirect.MAX_HOPS + " hops");
            }
            next = RpcPoolRedirect.nextUrl(response, endpoint);
        } catch (IOException e) {
            failure = e;
        } finally {
            response.close();
        }
        final HttpUrl nextOnce = next;
        final IOException failureOnce = failure;
        if (failureOnce != null) {
            post(() -> onFetchDone(call, null, failureOnce));
        } else {
            post(() -> onHop(call, nextOnce, allowedUrl, hopsLeft - 1));
        }
    }


    // Starts the next hop on the pool's thread. A call settled meanwhile gives its fetch slot back and no more.
    private void onHop(RpcPoolCall call, HttpUrl next, HttpUrl allowedUrl, int hopsLeft) {
        if (call.settled) {
            onFetchDone(call, null, null);
            return;
        }
        hop(call, next, allowedUrl, hopsLeft);
    }


    private void onFetchDone(RpcPoolCall call, byte[] body, IOException failure) {
        redirectsInflight--;
        call.redirectCall = null;
        if (!call.settled) {
            if (failure != null) {
                settle(call, null, new ReplyError(REDIRECT_FAILED, "redirect failed: " + failure.getMessage(), false));
            } else {
                RPCResult result = new RPCResult();
                result.setData(body);
                settle(call, result, null);
            }
        }
        startWaitingRedirects();
        pump();
    }


    private void startWaitingRedirects() {
        while (redirectsInflight < maxRedirectsInflight && !redirectWaiting.isEmpty()) {
            RpcPoolCall next = redirectWaiting.poll();
            next.redirectWaiting = false;
            if (!next.settled) {
                fetch(next);
            }
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
        if (call.queued) {
            queue.remove(call);
            call.queued = false;
        }
        if (call.connection != null && call.connection.lost.remove(call)) {
            // A lost call waiting for its connection's event leaves the connection with its outcome.
            RpcPoolConnection conn = call.connection;
            call.connection = null;
            if (idle(conn)) {
                markIdle(conn);
            }
        }
        if (call.redirectWaiting) {
            redirectWaiting.remove(call);
            call.redirectWaiting = false;
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


    // Called when a connection may have become idle: it records the moment, and arms the one idle check of the pool if
    // none is armed. Nothing is cancelled here, so a call does not leave a timer behind it.
    private void markIdle(RpcPoolConnection conn) {
        conn.idleSince = System.nanoTime();
        if (idleTimeoutMillis > 0L && idleCheck == null && !closed) {
            armIdleCheck(TimeUnit.MILLISECONDS.toNanos(idleTimeoutMillis));
        }
    }


    private void armIdleCheck(long delayNanos) {
        idleCheck = executor.schedule(this::onIdleCheck, delayNanos, TimeUnit.NANOSECONDS);
    }


    // Closes the connections which have been idle for the timeout, and arms the check again for the next of the others.
    // It runs only while a connection may be idle, and each run sweeps the connections of the pool once.
    private void onIdleCheck() {
        idleCheck = null;
        if (closed) {
            return;
        }
        long now = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMillis);
        long nextNanos = -1L;
        List<RpcPoolConnection> expired = new ArrayList<>();
        for (RpcPoolEndpoint endpoint : endpoints.values()) {
            for (RpcPoolConnection conn : endpoint.connections()) {
                if (!idle(conn)) {
                    continue;
                }
                long remaining = conn.idleSince + timeoutNanos - now;
                if (remaining <= 0L) {
                    expired.add(conn);
                } else if (nextNanos < 0L || remaining < nextNanos) {
                    nextNanos = remaining;
                }
            }
        }
        for (RpcPoolConnection conn : expired) {
            remove(conn);
        }
        if (nextNanos >= 0L) {
            armIdleCheck(nextNanos);
        }
        if (!expired.isEmpty()) {
            pump();
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
        conn.close();
    }


    private void closeOnPool() {
        if (closed) {
            return;
        }
        closed = true;
        for (RpcPoolCall call : new ArrayList<>(unsettled)) {
            if (call.connection != null) {
                settle(call, null, new IOException("rpc pool closed"));
            } else {
                settle(call, null, cancelled());
            }
        }
        for (RpcPoolEndpoint endpoint : endpoints.values()) {
            for (RpcPoolConnection conn : new ArrayList<>(endpoint.connections())) {
                remove(conn);
            }
        }
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
        return new Stats(connections, inflight, queue.size(), redirectsInflight);
    }


    // The client does not follow a redirect itself: the pool checks each hop's scheme and host, and sets the headers of each hop.
    private OkHttpClient fetchClient() {
        if (fetchHttpClient == null) {
            OkHttpClient base = httpClient != null ? httpClient : defaultHttpClient();
            fetchHttpClient = base.newBuilder().followRedirects(false).followSslRedirects(false).build();
        }
        return fetchHttpClient;
    }


    // The client of the pools which were given none, shared by the process: its dispatcher and connection pool are not
    // left behind by each pool that is closed.
    private static synchronized OkHttpClient defaultHttpClient() {
        if (sharedHttpClient == null) {
            sharedHttpClient = new OkHttpClient();
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
