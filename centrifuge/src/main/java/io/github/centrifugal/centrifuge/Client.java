package io.github.centrifugal.centrifuge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;
import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;
import java8.util.concurrent.CompletionException;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

public class Client {
    private WebSocket ws;
    private OkHttpClient httpClient;
    // Whether httpClient was built from scratch (no user-supplied base client),
    // meaning its Dispatcher and ConnectionPool are ours to dispose. Recorded
    // at build time so a later mutation of Options.okHttpClient cannot
    // misclassify ownership of an already-built transport (#87).
    private boolean httpClientOwned;
    private final String endpoint;
    private final Options opts;
    private String token;
    private com.google.protobuf.ByteString data;
    private final EventListener listener;
    private final Map<Integer, CompletableFuture<Protocol.Reply>> futures = new ConcurrentHashMap<>();
    private final Map<Integer, Protocol.Command> connectCommands = new ConcurrentHashMap<>();
    private final Map<Integer, Protocol.Command> connectAsyncCommands = new ConcurrentHashMap<>();
    // rpc calls waiting for the connection: not sent yet, or lost with their transport.
    private final RpcQueue rpcQueue;
    // rpc calls written to the current transport, by the id of their command.
    private final Map<Integer, RpcCall> rpcsOnWire = new ConcurrentHashMap<>();
    // msg_ids of the replies received, sent as the ack of the next rpc call.
    private final List<Long> pendingAcks = new ArrayList<>();
    private long lastMsgId = 0;

    private volatile ClientState state = ClientState.DISCONNECTED;
    private final Map<String, Subscription> subs = new ConcurrentHashMap<>();
    // Channel compaction: numeric channel ID → subscription, used to route pushes
    // that carry an ID instead of the string channel name. IDs are scoped to a
    // server session — the registry is dropped on disconnect and each subscription
    // re-registers from its subscribe reply. Only touched on the executor thread,
    // but kept concurrent for consistency with subs (a leaf map, never iterated).
    private final Map<Long, Subscription> subsById = new ConcurrentHashMap<>();
    private final Map<String, ServerSubscription> serverSubs = new ConcurrentHashMap<>();
    private final Backoff backoff;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private int pingInterval;
    private boolean sendPong;
    private ScheduledFuture<?> pingTask;
    private ScheduledFuture<?> refreshTask;
    private ScheduledFuture<?> reconnectTask;
    private int reconnectAttempts = 0;
    private boolean refreshRequired = false;
    // Confirmed by the connect reply of the current transport, cleared when it closes.
    private volatile boolean rpcOptionsSupported = false;
    // Server clock minus local clock, from the last reply that carried a server time.
    private volatile long serverTimeOffset = 0;
    // Identity the server last reported for the connection, see applyAuth.
    private volatile String currentUser = "";
    private volatile String currentAuthClass = "";
    // Set while a refresh() call waits for its reply; rpc calls and further refresh calls
    // queue in pendingAfterRefresh until it completes. Only touched on the executor thread.
    private boolean refreshInFlight = false;
    private final Deque<Runnable> pendingAfterRefresh = new ArrayDeque<>();

    Options getOpts() {
        return opts;
    }

    ExecutorService getExecutor() {
        return executor;
    }

    ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    private static final int NORMAL_CLOSURE_STATUS = 1000;
    private static final int MESSAGE_SIZE_LIMIT_EXCEEDED_STATUS = 1009;

    static final int DISCONNECTED_DISCONNECT_CALLED = 0;
    static final int DISCONNECTED_UNAUTHORIZED = 1;
    static final int DISCONNECTED_BAD_PROTOCOL = 2;
    static final int DISCONNECTED_MESSAGE_SIZE_LIMIT = 3;

    static final int CONNECTING_CONNECT_CALLED = 0;
    static final int CONNECTING_TRANSPORT_CLOSED = 1;
    static final int CONNECTING_NO_PING = 2;
    static final int CONNECTING_SUBSCRIBE_TIMEOUT = 3;
    static final int CONNECTING_UNSUBSCRIBE_ERROR = 4;

    static final int SUBSCRIBING_SUBSCRIBE_CALLED = 0;
    static final int SUBSCRIBING_TRANSPORT_CLOSED = 1;

    static final int UNSUBSCRIBED_UNSUBSCRIBE_CALLED = 0;
    static final int UNSUBSCRIBED_UNAUTHORIZED = 1;
    static final int UNSUBSCRIBED_CLIENT_CLOSED = 2;

    // Subscription feature flags — bitmask sent in SubscribeRequest flag field.
    //
    // Channel compaction asks the server to replace the string channel name with a
    // short numeric ID in subscription pushes (bandwidth optimization). Safe to
    // request unconditionally: servers that don't support or don't allow it ignore
    // the bit and keep sending the full channel name.
    static final long SUBSCRIPTION_FLAG_CHANNEL_COMPACTION = 1;
    static final long SUBSCRIPTION_FLAG_REJECT_UNRECOVERED = 2;

    // Server error code returned when recovery from the provided position is
    // impossible (only sent when SUBSCRIPTION_FLAG_REJECT_UNRECOVERED was requested).
    static final int ERROR_CODE_UNRECOVERABLE_POSITION = 112;

    // Server-sent "state invalidated" codes. The server determines that the
    // client's cached state and/or token are no longer valid and asks the client
    // to drop them and re-sync.
    //
    // 2502 arrives in an Unsubscribe push for a single subscription (the client
    // clears the subscription state and resubscribes, since it's >= 2500). 3014
    // arrives for the whole connection (the client clears the connection token to
    // force a fresh one via getToken, invalidates all subscriptions, reconnects).
    static final int UNSUBSCRIBED_STATE_INVALIDATED = 2502;
    static final int DISCONNECTED_STATE_INVALIDATED = 3014;

    /**
     * Creates a new instance of Client. Client allows to allocate new Subscriptions to channels,
     * automatically manages reconnects and re-subscriptions on temporary failures.
     *
     * @param endpoint: connection endpoint.
     * @param opts:     client options.
     * @param listener: client event handler.
     */
    public Client(final String endpoint, final Options opts, final EventListener listener) {
        this.endpoint = endpoint;
        this.opts = opts;
        this.listener = listener;
        this.backoff = new Backoff();
        this.rpcQueue = new RpcQueue(opts.getMaxPendingRpc());
        this.token = opts.getToken();
        if (opts.getData() != null) {
            this.data = com.google.protobuf.ByteString.copyFrom(opts.getData());
        }
    }

    void setState(ClientState state) {
        this.state = state;
    }

    public ClientState getState() {
        return this.state;
    }

    /**
     * Capabilities the server confirmed for the current connection. A reconnect starts
     * with none until its connect reply confirms them.
     */
    public ClientCapabilities getCapabilities() {
        return new ClientCapabilities(this.rpcOptionsSupported);
    }

    /**
     * Current server time in milliseconds, estimated from the server time of the last reply
     * that carried one. Local time when no such reply has been seen.
     */
    public long serverTimeMillis() {
        return Extensions.estimateServerTime(System.currentTimeMillis(), this.serverTimeOffset);
    }

    private int _id = 0;

    private int getNextId() {
        return ++_id;
    }

    /**
     * Start connecting to a server.
     */
    public void connect() {
        this.executor.submit(() -> {
            if (Client.this.getState() == ClientState.CONNECTED || Client.this.getState() == ClientState.CONNECTING) {
                return;
            }
            Client.this.reconnectAttempts = 0;
            Client.this.setState(ClientState.CONNECTING);
            ConnectingEvent event = new ConnectingEvent(CONNECTING_CONNECT_CALLED, "connect called");
            Client.this.listener.onConnecting(Client.this, event);
            Client.this._connect();
        });
    }

    /**
     * Disconnect from server and do not reconnect.
     */
    public void disconnect() {
        this.executor.submit(() -> {
            Client.this.processDisconnect(DISCONNECTED_DISCONNECT_CALLED, "disconnect called", false);
        });
    }

    /**
     * setToken allows updating connection token.
     */
    public void setToken(String token) {
        this.executor.submit(() -> {
            Client.this.token = token;
        });
    }

    /**
     * Close disconnects client from server and cleans up client resources including
     * shutdown of internal executors. Client is not usable after calling this method.
     * If you only need to temporary disconnect from server use .disconnect() method.
     *
     * @param awaitMilliseconds is time in milliseconds to wait for executor
     *                          termination (0 means no waiting).
     * @return boolean that indicates whether executor terminated in awaitMilliseconds. For
     * zero awaitMilliseconds close always returns false.
     */
    public boolean close(long awaitMilliseconds) throws InterruptedException {
        try {
            this.executor.submit(() -> {
                // Tear down the connection synchronously on the executor thread
                // so any future-cleanup tasks scheduled by processDisconnect
                // (e.g. orTimeout/exceptionally re-submits) get queued before
                // we mark the executor shutdown.
                Client.this.processDisconnect(DISCONNECTED_DISCONNECT_CALLED, "client closed", false);
                Client.this.setState(ClientState.CLOSED);
                if (Client.this.ws != null) {
                    Client.this.ws.cancel();
                }
                // Release dispatcher threads + pooled connections on transports
                // we built from scratch. When the user supplied a base client,
                // transports derived from it via newBuilder() share the user's
                // Dispatcher and ConnectionPool — we never own those (#87).
                if (Client.this.httpClient != null && Client.this.httpClientOwned) {
                    Client.this.httpClient.dispatcher().executorService().shutdown();
                    Client.this.httpClient.connectionPool().evictAll();
                }
                Client.this.scheduler.shutdownNow();
                // Initiate executor shutdown as the final step. New submits
                // will be rejected, but already-queued cleanup tasks still run.
                Client.this.executor.shutdown();
            });
        } catch (RejectedExecutionException ignored) {
            // Executor already shut down (close called twice).
        }
        if (awaitMilliseconds > 0) {
            return this.executor.awaitTermination(awaitMilliseconds, TimeUnit.MILLISECONDS);
        }
        return false;
    }

    void processDisconnect(int code, String reason, Boolean shouldReconnect) {
        if (this.getState() == ClientState.DISCONNECTED || this.getState() == ClientState.CLOSED) {
            return;
        }

        if (code == DISCONNECTED_STATE_INVALIDATED) {
            // State invalidated (delivered as a WebSocket close frame or a Disconnect
            // push, both funnel here): drop the connection token so the next connect
            // fetches a fresh one via the token getter, and invalidate every
            // subscription's cached state before reconnecting.
            this.token = "";
            this.refreshRequired = true;
            for (Subscription sub : this.subs.values()) {
                sub.invalidateState();
            }
            // Server-side subscriptions have no invalidateState() of their own (they
            // are plain data, not stateful Subscription objects) - reset their cached
            // recovery position here so the next connect request (see sendConnect)
            // does not resend a now-invalid offset/epoch pair. The recoverable flag
            // is left untouched, matching invalidateState() above.
            for (ServerSubscription serverSub : this.serverSubs.values()) {
                serverSub.setLastOffset(0);
                serverSub.setLastEpoch("_");
            }
        }

        ClientState previousState = this.getState();

        if (this.pingTask != null) {
            this.pingTask.cancel(true);
            this.pingTask = null;
        }
        if (this.refreshTask != null) {
            this.refreshTask.cancel(true);
            this.refreshTask = null;
        }
        if (this.reconnectTask != null) {
            this.reconnectTask.cancel(true);
            this.reconnectTask = null;
        }
        boolean needEvent;
        if (shouldReconnect) {
            needEvent = previousState != ClientState.CONNECTING;
            this.setState(ClientState.CONNECTING);
        } else {
            needEvent = previousState != ClientState.DISCONNECTED;
            this.setState(ClientState.DISCONNECTED);
        }
        // The transport is gone: its capabilities go with it until the next connect reply.
        this.rpcOptionsSupported = false;

        // Channel compaction IDs are scoped to a server session — drop the routing
        // registry; each resubscribe re-registers a fresh ID. The per-subscription
        // pushId field is left as-is: moveToSubscribed re-registers it on the next
        // subscribe reply regardless of whether the server reuses the same ID.
        this.subsById.clear();

        synchronized (this.subs) {
            for (Map.Entry<String, Subscription> entry : this.subs.entrySet()) {
                Subscription sub = entry.getValue();
                if (sub.getState() == SubscriptionState.UNSUBSCRIBED) {
                    continue;
                }
                sub.moveToSubscribing(SUBSCRIBING_TRANSPORT_CLOSED, "transport closed");
            }
        }

        // rpc calls with a msg_id are kept for a resend when the client reconnects; the rest fail.
        boolean keepRpcs = Boolean.TRUE.equals(shouldReconnect);
        List<RpcCall> lostRpcs = new ArrayList<>();
        for (Map.Entry<Integer, CompletableFuture<Protocol.Reply>> entry : this.futures.entrySet()) {
            RpcCall call = this.rpcsOnWire.get(entry.getKey());
            if (keepRpcs && call != null && call.wireTracked) {
                lostRpcs.add(call);
                continue;
            }
            CompletableFuture<Protocol.Reply> f = entry.getValue();
            f.completeExceptionally(new IOException());
        }
        this.rpcsOnWire.clear();
        for (RpcCall call : lostRpcs) {
            if (!this.rpcQueue.offer(call)) {
                call.future.completeExceptionally(new IOException());
            }
        }
        if (!keepRpcs) {
            for (RpcCall call : this.rpcQueue.drain()) {
                call.future.completeExceptionally(new IOException());
            }
        }
        // Drop the pending command registry along with the futures: no reply can
        // arrive for a connection that is gone, and command IDs are never reused.
        // Failure handlers do remove their own entry, but only after re-checking
        // that the client is still CONNECTED — which it no longer is at this point
        // (the state was set above), so subscribe/unsubscribe entries would stay
        // behind forever and accumulate one per in-flight command per reconnect.
        // Matches centrifuge-js, which resets _callbacks in _clearOutgoingRequests.
        this.futures.clear();

        if (previousState == ClientState.CONNECTED) {
            for (Map.Entry<String, ServerSubscription> entry : this.serverSubs.entrySet()) {
                this.listener.onSubscribing(this, new ServerSubscribingEvent(entry.getKey()));
            }
        }

        if (needEvent) {
            if (shouldReconnect) {
                ConnectingEvent event = new ConnectingEvent(code, reason);
                this.listener.onConnecting(this, event);
            } else {
                DisconnectedEvent event = new DisconnectedEvent(code, reason);
                this.listener.onDisconnected(this, event);
            }
        }

        this.ws.close(NORMAL_CLOSURE_STATUS, null);
    }

    private void _connect() {
        Headers.Builder headers = new Headers.Builder();
        if (this.opts.getHeaders() != null) {
            for (Map.Entry<String, String> entry : this.opts.getHeaders().entrySet()) {
                headers.add(entry.getKey(), entry.getValue());
            }
        }

        Request request = new Request.Builder()
                .url(this.endpoint)
                .headers(headers.build())
                .addHeader("Sec-WebSocket-Protocol", "centrifuge-protobuf")
                .build();

        if (this.ws != null) {
            this.ws.cancel();
        }

        // If we previously built our own OkHttpClient from scratch, release its
        // dispatcher threads and pooled connections before building a new one.
        // When the user supplied a base client we derive each transport from it
        // via newBuilder(), which shares the user's Dispatcher and
        // ConnectionPool: shutting those down would kill the executor behind
        // the user's client and behind the very transport we are about to
        // build, permanently rejecting all of their calls and our future
        // reconnects (#87). Only transports built from scratch own their
        // resources, tracked via httpClientOwned at build time.
        OkHttpClient previousHttpClient =
                (this.httpClient != null && this.httpClientOwned) ? this.httpClient : null;

        OkHttpClient baseClient = opts.getOkHttpClient();
        OkHttpClient.Builder okHttpBuilder;
        if (baseClient != null) {
            okHttpBuilder = baseClient.newBuilder();
        } else {
            okHttpBuilder = new OkHttpClient.Builder();
        }

        Dns dns = opts.getDns();
        if (dns != null) {
            okHttpBuilder.dns(dns::resolve);
        }

        SSLSocketFactory socketFactory = opts.getSSLSocketFactory();
        if (socketFactory != null) {
            X509TrustManager trustManager = opts.getTrustManager();
            if (trustManager != null) {
                okHttpBuilder.sslSocketFactory(socketFactory, trustManager);
            } else {
                okHttpBuilder.setSslSocketFactoryOrNull$okhttp(socketFactory);
            }
        }

        if (opts.getProxy() != null) {
            okHttpBuilder.proxy(opts.getProxy());
            if (this.opts.getProxyLogin() != null && this.opts.getProxyPassword() != null) {
                okHttpBuilder.proxyAuthenticator((route, response) -> {
                    String credentials = Credentials.basic(opts.getProxyLogin(), opts.getProxyPassword());

                    return response.request()
                            .newBuilder()
                            .header("Proxy-Authorization", credentials)
                            .build();
                });
            }
        }

        this.httpClient = okHttpBuilder.build();
        this.httpClientOwned = baseClient == null;
        // Each transport starts without capabilities; its connect reply confirms them.
        this.rpcOptionsSupported = false;
        this.ws = this.httpClient.newWebSocket(request, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                super.onOpen(webSocket, response);
                try {
                    Client.this.executor.submit(() -> {
                        // Callbacks of a socket that has since been replaced (e.g.
                        // disconnect() then connect() cancels a socket still closing)
                        // belong to a connection the client already gave up on - they
                        // must not act on the new one. Checked on the executor, where
                        // ws is guaranteed to be assigned. Matches centrifuge-js,
                        // which ignores callbacks from a non-actual transport.
                        if (Client.this.ws != webSocket) {
                            return;
                        }
                        try {
                            Client.this.handleConnectionOpen();
                        } catch (Throwable e) {
                            // Catch Throwable (not just Exception) so LinkageError /
                            // NoSuchMethodError caused by protobuf classpath conflicts
                            // (duplicate `protobuf-java` next to `protobuf-javalite`,
                            // R8 stripping a generated field, etc.) surfaces as an
                            // onError event instead of dying silently inside the
                            // executor — see issue #20.
                            e.printStackTrace();
                            Client.this.listener.onError(Client.this, new ErrorEvent(new UnclassifiedError(e)));
                            Client.this.processDisconnect(DISCONNECTED_BAD_PROTOCOL, "bad protocol (open)", false);
                        }
                    });
                } catch (RejectedExecutionException ignored) {
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                super.onMessage(webSocket, bytes);
                try {
                    Client.this.executor.submit(() -> {
                        if (Client.this.ws != webSocket) {
                            return;
                        }
                        if (Client.this.getState() != ClientState.CONNECTING && Client.this.getState() != ClientState.CONNECTED) {
                            return;
                        }
                        InputStream stream = new ByteArrayInputStream(bytes.toByteArray());
                        while (true) {
                            Protocol.Reply reply;
                            try {
                                if (stream.available() <= 0) {
                                    break;
                                }
                                reply = Protocol.Reply.parseDelimitedFrom(stream);
                            } catch (IOException e) {
                                // Should never happen. Corrupted server protocol data?
                                e.printStackTrace();
                                Client.this.listener.onError(Client.this, new ErrorEvent(new UnclassifiedError(e)));
                                Client.this.processDisconnect(DISCONNECTED_BAD_PROTOCOL, "bad protocol (proto)", false);
                                break;
                            }
                            try {
                                Client.this.processReply(reply);
                            } catch (Throwable e) {
                                // Catch Throwable so LinkageError / NoSuchMethodError
                                // raised by protobuf reflection (or any other Error
                                // surfacing from generated code) is reported via
                                // onError rather than silently swallowed by the
                                // executor — see issue #20.
                                e.printStackTrace();
                                Client.this.listener.onError(Client.this, new ErrorEvent(new UnclassifiedError(e)));
                                Client.this.processDisconnect(DISCONNECTED_BAD_PROTOCOL, "bad protocol (message)", false);
                                break;
                            }
                        }
                    });
                } catch (RejectedExecutionException ignored) {
                }
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                super.onClosing(webSocket, code, reason);
                webSocket.close(NORMAL_CLOSURE_STATUS, null);
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                super.onClosed(webSocket, code, reason);
                try {
                    Client.this.executor.submit(() -> {
                        if (Client.this.ws != webSocket) {
                            return;
                        }
                        boolean reconnect = code < 3500 || code >= 5000 || (code >= 4000 && code < 4500);
                        int disconnectCode = code;
                        String disconnectReason = reason;
                        if (disconnectCode < 3000) {
                            if (disconnectCode == MESSAGE_SIZE_LIMIT_EXCEEDED_STATUS) {
                                disconnectCode = DISCONNECTED_MESSAGE_SIZE_LIMIT;
                                disconnectReason = "message size limit exceeded";
                                // Reconnecting will hit the same limit, so this is terminal —
                                // matches centrifuge-js (transport_websocket onClose).
                                reconnect = false;
                            } else {
                                disconnectCode = CONNECTING_TRANSPORT_CLOSED;
                                disconnectReason = "transport closed";
                            }
                        }
                        if (Client.this.getState() != ClientState.DISCONNECTED) {
                            Client.this.processDisconnect(disconnectCode, disconnectReason, reconnect);
                        }
                        if (Client.this.getState() == ClientState.CONNECTING) {
                            Client.this.scheduleReconnect();
                        }
                    });
                } catch (RejectedExecutionException ignored) {
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                super.onFailure(webSocket, t, response);
                try {
                    Client.this.executor.submit(() -> {
                        if (Client.this.ws != webSocket) {
                            return;
                        }
                        Integer responseCode = (response != null) ? response.code() : null;
                        listener.onError(Client.this, new ErrorEvent(t, responseCode));
                        Client.this.processDisconnect(CONNECTING_TRANSPORT_CLOSED, "transport closed", true);
                        if (Client.this.getState() == ClientState.CONNECTING) {
                            // We need to schedule reconnect from here, since onClosed won't be called
                            // after onFailure.
                            Client.this.scheduleReconnect();
                        }
                    });
                } catch (RejectedExecutionException ignored) {
                }
            }
        });

        if (previousHttpClient != null) {
            // Graceful shutdown: in-flight calls (including cancel of the
            // previous WebSocket) finish, then dispatcher threads exit and
            // pooled connections are released.
            previousHttpClient.dispatcher().executorService().shutdown();
            previousHttpClient.connectionPool().evictAll();
        }
    }

    private void handleConnectionOpen() {
        if (this.getState() != ClientState.CONNECTING) {
            return;
        }
        if (this.refreshRequired || (this.token.equals("") && this.opts.getTokenGetter() != null)) {
            ConnectionTokenEvent connectionTokenEvent = new ConnectionTokenEvent();
            if (this.opts.getTokenGetter() == null) {
                this.listener.onError(Client.this, new ErrorEvent(new ConfigurationError(new Exception("tokenGetter function should be provided in Client options to handle token refresh, see Options.setTokenGetter"))));
                this.processDisconnect(DISCONNECTED_UNAUTHORIZED, "unauthorized", false);
                return;
            }
            final WebSocket connectionWs = this.ws;
            this.opts.getTokenGetter().getConnectionToken(connectionTokenEvent, (err, token) -> this.executor.submit(() -> {
                // If the connection has been replaced (reconnect, disconnect)
                // since this token request was issued, the callback is stale.
                if (Client.this.ws != connectionWs) {
                    return;
                }
                if (Client.this.getState() != ClientState.CONNECTING) {
                    return;
                }
                if (err != null) {
                    if (err instanceof UnauthorizedException) {
                        Client.this.failUnauthorized();
                        return;
                    }
                    Client.this.listener.onError(Client.this, new ErrorEvent(new TokenError(err)));
                    this.ws.close(NORMAL_CLOSURE_STATUS, "");
                    return;
                }
                if (token == null) {
                    Client.this.processDisconnect(DISCONNECTED_BAD_PROTOCOL, "bad protocol (token)", false);
                    return;
                }
                Client.this.token = token;
                Client.this.refreshRequired = false;
                this.sendConnect();
            }));
        } else {
            this.sendConnect();
        }
    }

    private void handleConnectionError(Throwable t) {
        this.listener.onError(this, new ErrorEvent(t));
    }

    private void startReconnecting() {
        Client.this.executor.submit(() -> {
            if (Client.this.getState() != ClientState.CONNECTING) {
                return;
            }
            Client.this._connect();
        });
    }

    private void scheduleReconnect() {
        if (this.getState() != ClientState.CONNECTING) {
            return;
        }
        this.reconnectTask = this.scheduler.schedule(
                Client.this::startReconnecting,
                Client.this.backoff.duration(this.reconnectAttempts, this.opts.getMinReconnectDelay(), this.opts.getMaxReconnectDelay()),
                TimeUnit.MILLISECONDS
        );
        this.reconnectAttempts++;
    }

    private void sendSubscribeSynchronized(String channel, Protocol.SubscribeRequest req) {
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setSubscribe(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        this.futures.put(cmd.getId(), f);
        f.thenAccept(reply -> {
            if (Client.this.getState() != ClientState.CONNECTED) {
                return;
            }
            try {
                this.handleSubscribeReply(channel, reply);
            } catch (Exception e) {
                // Should never happen.
                e.printStackTrace();
            }
            this.futures.remove(cmd.getId());
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                if (Client.this.getState() != ClientState.CONNECTED) {
                    return;
                }
                Client.this.futures.remove(cmd.getId());
                Client.this.processDisconnect(CONNECTING_SUBSCRIBE_TIMEOUT, "subscribe timeout", true);
            });
            return null;
        });
        this.ws.send(ByteString.of(this.serializeCommand(cmd)));
    }

    void sendSubscribe(Subscription sub, Protocol.SubscribeRequest req) {
        if (this.getState() != ClientState.CONNECTED) {
            // Subscription registered and will start subscribing as soon as
            // client will be connected.
            return;
        }
        this.sendSubscribeSynchronized(sub.getChannel(), req);
    }

    void sendUnsubscribe(String channel) {
        this.executor.submit(() -> Client.this.sendUnsubscribeSynchronized(channel));
    }

    private void sendUnsubscribeSynchronized(String channel) {
        if (this.getState() != ClientState.CONNECTED) {
            return;
        }
        Protocol.UnsubscribeRequest req = Protocol.UnsubscribeRequest.newBuilder()
                .setChannel(channel)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setUnsubscribe(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        this.futures.put(cmd.getId(), f);
        f.thenAccept(reply -> {
            // No need to handle reply for now.
            this.futures.remove(cmd.getId());
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                if (Client.this.getState() != ClientState.CONNECTED) {
                    return;
                }
                Client.this.futures.remove(cmd.getId());
                Client.this.processDisconnect(CONNECTING_UNSUBSCRIBE_ERROR, "unsubscribe error", true);
            });
            return null;
        });

        this.ws.send(ByteString.of(this.serializeCommand(cmd)));
    }

    private byte[] serializeCommand(Protocol.Command cmd) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            cmd.writeDelimitedTo(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stream.toByteArray();
    }

    private Subscription getSub(String channel) {
        return this.subs.get(channel);
    }

    // Update the channel compaction registry for sub: remove the old numeric ID
    // mapping (only if it still points to this subscription) and register the new
    // one. Either ID may be 0 meaning "no mapping". Called on the executor thread.
    void updateSubscriptionPushId(Subscription sub, long oldId, long newId) {
        if (oldId > 0) {
            // Conditional removal: don't evict another subscription's registration
            // after a cross-session ID collision.
            this.subsById.remove(oldId, sub);
        }
        if (newId > 0) {
            this.subsById.put(newId, sub);
        }
    }

    // Resolve a client-side subscription for a push: by numeric channel ID when
    // channel compaction is in use (the push then has no channel name), by channel
    // name otherwise.
    private Subscription getSubForPush(String channel, long id) {
        if (id > 0) {
            return this.subsById.get(id);
        }
        return this.subs.get(channel);
    }

    private ServerSubscription getServerSub(String channel) {
        return this.serverSubs.get(channel);
    }

    /**
     * Create new subscription to channel with SubscriptionOptions and SubscriptionEventListener
     *
     * @param channel:  to create Subscription for.
     * @param options:  to pass SubscriptionOptions, e.g. token.
     * @param listener: to pass event handler.
     * @return Subscription.
     * @throws DuplicateSubscriptionException if Subscription already exists in internal registry.
     */
    public Subscription newSubscription(String channel, SubscriptionOptions options, SubscriptionEventListener listener) throws DuplicateSubscriptionException {
        Subscription sub;
        synchronized (this.subs) {
            if (this.subs.get(channel) != null) {
                throw new DuplicateSubscriptionException();
            }
            sub = new Subscription(Client.this, channel, listener, options);
            this.subs.put(channel, sub);
        }
        return sub;
    }

    /**
     * Create new subscription to channel with SubscriptionEventListener, see also
     * overloaded newSubscription which allows setting SubscriptionOptions
     *
     * @param channel:  to create Subscription for.
     * @param listener: to pass event handler.
     * @return Subscription.
     * @throws DuplicateSubscriptionException if Subscription already exists in internal registry.
     */
    public Subscription newSubscription(String channel, SubscriptionEventListener listener) throws DuplicateSubscriptionException {
        return newSubscription(channel, new SubscriptionOptions(), listener);
    }

    /**
     * Try to get Subscription from internal client registry. Can return null if Subscription
     * does not exist yet.
     *
     * @param channel: channel to get subscription for.
     * @return Subscription
     */
    public Subscription getSubscription(String channel) {
        Subscription sub;
        synchronized (this.subs) {
            sub = this.getSub(channel);
        }
        return sub;
    }

    /**
     * Say Client that Subscription should be removed from internal registry. Subscription will be
     * automatically unsubscribed before removing.
     *
     * @param sub: Subscription to remove from registry.
     */
    public void removeSubscription(Subscription sub) {
        synchronized (this.subs) {
            sub.unsubscribe();
            if (this.subs.get(sub.getChannel()) != null) {
                this.subs.remove(sub.getChannel());
            }
        }
    }

    private void handleSubscribeReply(String channel, Protocol.Reply reply) throws Exception {
        Subscription sub = this.getSub(channel);
        if (sub != null) {
            if (sub.getState() != SubscriptionState.SUBSCRIBING) {
                // The subscription left SUBSCRIBING while the subscribe command was
                // still in flight — e.g. unsubscribe() called right after subscribe(),
                // or the channel's subscription was removed and re-created. The reply
                // belongs to an attempt nobody waits for anymore: applying it would
                // resurrect the subscription into SUBSCRIBED (emitting onSubscribed
                // after onUnsubscribed) even though the server was already told to
                // unsubscribe. Matches centrifuge-js, which guards both the success
                // and the error path with _isSubscribing().
                return;
            }
            Protocol.SubscribeResult result;
            if (reply.getError().getCode() != 0) {
                ReplyError err = this.getReplyError(reply);
                sub.subscribeError(err);
                return;
            }
            result = reply.getSubscribe();
            sub.moveToSubscribed(result);
        }
    }

    private void _waitServerPing() {
        if (this.getState() != ClientState.CONNECTED) {
            return;
        }
        Client.this.processDisconnect(CONNECTING_NO_PING, "no ping", true);
    }

    private void waitServerPing() {
        this.executor.submit(Client.this::_waitServerPing);
    }

    private void handleConnectReply(Protocol.Reply reply) {
        if (this.getState() != ClientState.CONNECTING) {
            return;
        }
        if (reply.getError().getCode() != 0) {
            this.handleConnectionError(this.getReplyError(reply));
            if (reply.getError().getCode() == 109) { // Token expired.
                this.refreshRequired = true;
                this.ws.close(NORMAL_CLOSURE_STATUS, "");
            } else if (reply.getError().getTemporary()) {
                this.ws.close(NORMAL_CLOSURE_STATUS, "");
            } else {
                this.processDisconnect(reply.getError().getCode(), reply.getError().getMessage(), false);
            }
            return;
        }
        Protocol.ConnectResult result = reply.getConnect();
        this.rpcOptionsSupported = Extensions.hasFlag(result.getFlag(), Extensions.RPC_OPTIONS_FLAG);
        ConnectedEvent event = new ConnectedEvent();
        event.setClient(result.getClient());
        event.setData(result.getData().toByteArray());
        event.setUser(result.getUser());
        event.setAuthClass(result.getAuthClass());
        this.setState(ClientState.CONNECTED);
        this.listener.onConnected(this, event);
        this.applyAuth(result.getUser(), result.getAuthClass(), true);
        this.pingInterval = result.getPing() * 1000;
        this.sendPong = result.getPong();

        synchronized (this.subs) {
            for (Map.Entry<String, Subscription> entry : this.subs.entrySet()) {
                Subscription sub = entry.getValue();
                sub.resubscribeIfNecessary();
            }
        }

        for (Map.Entry<String, Protocol.SubscribeResult> entry : result.getSubsMap().entrySet()) {
            Protocol.SubscribeResult subResult = entry.getValue();
            String channel = entry.getKey();
            ServerSubscription serverSub;
            if (this.serverSubs.containsKey(channel)) {
                serverSub = this.serverSubs.get(channel);
            } else {
                serverSub = new ServerSubscription(subResult.getRecoverable(), subResult.getOffset(), subResult.getEpoch());
                this.serverSubs.put(channel, serverSub);
            }
            serverSub.setRecoverable(subResult.getRecoverable());
            serverSub.setLastEpoch(subResult.getEpoch());

            byte[] data = null;
            if (subResult.getData() != null) {
                data = subResult.getData().toByteArray();
            }

            this.listener.onSubscribed(this, new ServerSubscribedEvent(channel, subResult.getWasRecovering(), subResult.getRecovered(), subResult.getPositioned(), subResult.getRecoverable(), subResult.getPositioned() || subResult.getRecoverable() ? new StreamPosition(subResult.getOffset(), subResult.getEpoch()) : null, data));
            if (subResult.getPublicationsCount() > 0) {
                for (Protocol.Publication publication : subResult.getPublicationsList()) {
                    ServerPublicationEvent publicationEvent = new ServerPublicationEvent();
                    publicationEvent.setChannel(channel);
                    publicationEvent.setData(publication.getData().toByteArray());
                    publicationEvent.setTags(publication.getTagsMap());
                    ClientInfo info = ClientInfo.fromProtocolClientInfo(publication.getInfo());
                    publicationEvent.setInfo(info);
                    publicationEvent.setOffset(publication.getOffset());
                    if (publication.getOffset() > 0) {
                        serverSub.setLastOffset(publication.getOffset());
                    }
                    this.listener.onPublication(this, publicationEvent);
                }
            } else {
                serverSub.setLastOffset(subResult.getOffset());
            }
        }

        Iterator<Map.Entry<String, ServerSubscription>> it = this.serverSubs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ServerSubscription> entry = it.next();
            if (!result.getSubsMap().containsKey(entry.getKey())) {
                this.listener.onUnsubscribed(this, new ServerUnsubscribedEvent(entry.getKey()));
                it.remove();
            }
        }

        this.reconnectAttempts = 0;

        for (Map.Entry<Integer, Protocol.Command> entry : this.connectCommands.entrySet()) {
            Protocol.Command cmd = entry.getValue();
            boolean sent = this.ws.send(ByteString.of(this.serializeCommand(cmd)));
            if (!sent) {
                CompletableFuture<Protocol.Reply> f = this.futures.get(cmd.getId());
                if (f != null) {
                    f.completeExceptionally(new IOException());
                }
            }
        }

        this.connectCommands.clear();
        this.flushRpcQueue();

        for (Map.Entry<Integer, Protocol.Command> entry : this.connectAsyncCommands.entrySet()) {
            Protocol.Command cmd = entry.getValue();
            CompletableFuture<Protocol.Reply> f = this.futures.get(cmd.getId());
            boolean sent = this.ws.send(ByteString.of(this.serializeCommand(cmd)));
            if (!sent) {
                if (f != null) {
                    f.completeExceptionally(new IOException());
                }
            } else {
                if (f != null) {
                    f.complete(null);
                }
            }
        }
        this.connectAsyncCommands.clear();

        this.pingTask = this.scheduler.schedule(Client.this::waitServerPing, this.pingInterval + this.opts.getMaxServerPingDelay(), TimeUnit.MILLISECONDS);

        if (result.getExpires()) {
            int ttl = result.getTtl();
            this.refreshTask = this.scheduler.schedule(Client.this::sendRefresh, ttl, TimeUnit.SECONDS);
        }
    }

    private void sendRefresh() {
        if (this.opts.getTokenGetter() == null) {
            return;
        }
        this.executor.submit(() -> {
            final WebSocket refreshWs = Client.this.ws;
            Client.this.opts.getTokenGetter().getConnectionToken(new ConnectionTokenEvent(), (err, token) -> Client.this.executor.submit(() -> {
            // If the connection has been replaced since this refresh was
            // issued, the callback (and any token it carries) is stale.
            if (Client.this.ws != refreshWs) {
                return;
            }
            if (Client.this.getState() != ClientState.CONNECTED) {
                return;
            }
            if (err != null) {
                if (err instanceof UnauthorizedException) {
                    Client.this.failUnauthorized();
                    return;
                }
                Client.this.listener.onError(Client.this, new ErrorEvent(new TokenError(err)));
                Client.this.refreshTask = Client.this.scheduler.schedule(
                        Client.this::sendRefresh,
                        Client.this.backoff.duration(0, 10000, 20000),
                        TimeUnit.MILLISECONDS
                );
                return;
            }
            if (token == null || token.equals("")) {
                this.failUnauthorized();
                return;
            }
            Client.this.token = token;
            refreshSynchronized(token, (error, result) -> {
                if (Client.this.ws != refreshWs) {
                    return;
                }
                if (Client.this.getState() != ClientState.CONNECTED) {
                    return;
                }
                if (error != null) {
                    Client.this.listener.onError(Client.this, new ErrorEvent(new RefreshError(error)));
                    if (error instanceof ReplyError) {
                        ReplyError e;
                        e = (ReplyError) error;
                        if (e.isTemporary()) {
                            Client.this.refreshTask = Client.this.scheduler.schedule(
                                    Client.this::sendRefresh,
                                    Client.this.backoff.duration(0, 10000, 20000),
                                    TimeUnit.MILLISECONDS
                            );
                        } else {
                            Client.this.processDisconnect(e.getCode(), e.getMessage(), false);
                        }
                        return;
                    } else {
                        Client.this.refreshTask = Client.this.scheduler.schedule(
                                Client.this::sendRefresh,
                                Client.this.backoff.duration(0, 10000, 20000),
                                TimeUnit.MILLISECONDS
                        );
                    }
                    return;
                }
                if (result.getExpires()) {
                    int ttl = result.getTtl();
                    Client.this.refreshTask = Client.this.scheduler.schedule(Client.this::sendRefresh, ttl, TimeUnit.SECONDS);
                }
            });
        }));
        });
    }

    private void sendConnect() {
        Protocol.ConnectRequest.Builder build = Protocol.ConnectRequest.newBuilder();
        if (this.token.length() > 0) build.setToken(this.token);
        if (this.opts.getName().length() > 0) build.setName(this.opts.getName());
        if (this.opts.getVersion().length() > 0) build.setVersion(this.opts.getVersion());
        if (this.data != null) build.setData(this.data);
        if (this.opts.getInstance().length() > 0) build.setInstance(this.opts.getInstance());
        if (this.serverSubs.size() > 0) {
            for (Map.Entry<String, ServerSubscription> entry : this.serverSubs.entrySet()) {
                Protocol.SubscribeRequest.Builder subReqBuild = Protocol.SubscribeRequest.newBuilder();
                if (entry.getValue().getRecoverable()) {
                    subReqBuild.setEpoch(entry.getValue().getEpoch());
                    subReqBuild.setOffset(entry.getValue().getOffset());
                    subReqBuild.setRecover(true);
                }
                build.putSubs(entry.getKey(), subReqBuild.build());
            }
        }
        // Advertise the capabilities this client supports; the server echoes the ones it accepts.
        build.setFlag(Extensions.RPC_OPTIONS_FLAG);
        Protocol.ConnectRequest req = build.build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setConnect(req)
                .build();

        final WebSocket connectWs = this.ws;
        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        this.futures.put(cmd.getId(), f);
        f.thenAccept(reply -> {
            this.futures.remove(cmd.getId());
            try {
                this.handleConnectReply(reply);
            } catch (Exception e) {
                // Should never happen.
                e.printStackTrace();
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            // A timeout completes the future on the CompletableFuture delay
            // scheduler thread - hop to the executor like other command failures.
            this.executor.submit(() -> {
                Client.this.futures.remove(cmd.getId());
                // Failed because the connection was already torn down (disconnect
                // called or transport replaced) - nothing left to report or close.
                // Matches centrifuge-js, whose _connectError returns unless connecting.
                if (Client.this.ws != connectWs || Client.this.getState() != ClientState.CONNECTING) {
                    return;
                }
                Client.this.handleConnectionError(e);
                Client.this.ws.close(NORMAL_CLOSURE_STATUS, "");
            });
            return null;
        });

        this.ws.send(ByteString.of(this.serializeCommand(cmd)));
    }

    private void failUnauthorized() {
        this.processDisconnect(DISCONNECTED_UNAUTHORIZED, "unauthorized", false);
    }

    // A reply without a server time keeps the last offset.
    private void observeServerTime(Protocol.Reply reply) {
        Long offset = Extensions.serverTimeOffsetFrom(reply.getTime(), System.currentTimeMillis());
        if (offset != null) {
            this.serverTimeOffset = offset;
        }
    }

    // Records the identity the server reports for the connection. A connect always reports
    // it, so the listener hears every connect; a refresh reports it only when it changed.
    private void applyAuth(String user, String authClass, boolean always) {
        boolean changed = !user.equals(this.currentUser) || !authClass.equals(this.currentAuthClass);
        this.currentUser = user;
        this.currentAuthClass = authClass;
        if (always || changed) {
            this.listener.onAuth(this, new AuthEvent(user, authClass));
        }
    }

    private void processReply(Protocol.Reply reply) throws Exception {
        this.observeServerTime(reply);
        if (reply.getId() > 0) {
            CompletableFuture<Protocol.Reply> cf = this.futures.get(reply.getId());
            if (cf != null) cf.complete(reply);
        } else {
            if (reply.hasPush()) {
                this.handlePush(reply.getPush());
            } else {
                this.handlePing();
            }
        }
    }

    void handlePub(String channel, Protocol.Publication pub) throws Exception {
        this.handlePub(channel, pub, 0);
    }

    void handlePub(String channel, Protocol.Publication pub, long id) throws Exception {
        ClientInfo info = ClientInfo.fromProtocolClientInfo(pub.getInfo());
        Subscription sub = this.getSubForPush(channel, id);
        if (sub != null) {
            sub.handlePublication(pub);
        } else if (id > 0) {
            // Compacted push with unknown ID (e.g. already unsubscribed) — drop.
            // Server-side subscriptions never negotiate compaction.
            return;
        } else {
            ServerSubscription serverSub = this.getServerSub(channel);
            if (serverSub != null) {
                ServerPublicationEvent event = new ServerPublicationEvent();
                event.setChannel(channel);
                event.setData(pub.getData().toByteArray());
                event.setInfo(info);
                event.setOffset(pub.getOffset());
                event.setTags(pub.getTagsMap());
                if (pub.getOffset() > 0) {
                    serverSub.setLastOffset(pub.getOffset());
                }
                this.listener.onPublication(this, event);
            }
        }
    }

    private void handleSubscribe(String channel, Protocol.Subscribe sub) {
        ServerSubscription serverSub = new ServerSubscription(sub.getRecoverable(), sub.getOffset(), sub.getEpoch());
        this.serverSubs.put(channel, serverSub);
        serverSub.setRecoverable(sub.getRecoverable());
        serverSub.setLastEpoch(sub.getEpoch());
        serverSub.setLastOffset(sub.getOffset());
        byte[] data = null;
        if (sub.getData() != null) {
            data = sub.getData().toByteArray();
        }
        this.listener.onSubscribed(this, new ServerSubscribedEvent(channel, false, false, sub.getPositioned(), sub.getRecoverable(), sub.getPositioned() || sub.getRecoverable() ? new StreamPosition(sub.getOffset(), sub.getEpoch()) : null, data));
    }

    private void handleUnsubscribe(String channel, Protocol.Unsubscribe unsubscribe) {
        Subscription sub = this.getSub(channel);
        if (sub != null) {
            if (unsubscribe.getCode() < 2500) {
                sub.moveToUnsubscribed(false, unsubscribe.getCode(), unsubscribe.getReason());
            } else {
                if (unsubscribe.getCode() == UNSUBSCRIBED_STATE_INVALIDATED) {
                    // State invalidated: drop the subscription token and cached state so
                    // the resubscribe below obtains a fresh token and re-syncs.
                    sub.invalidateState();
                }
                sub.moveToSubscribing(unsubscribe.getCode(), unsubscribe.getReason());
                sub.resubscribeIfNecessary();
            }
        } else {
            ServerSubscription serverSub = this.getServerSub(channel);
            if (serverSub != null) {
                this.serverSubs.remove(channel);
                this.listener.onUnsubscribed(this, new ServerUnsubscribedEvent(channel));
            }
        }
    }

    private void handleJoin(String channel, Protocol.Join join, long id) {
        ClientInfo info = ClientInfo.fromProtocolClientInfo(join.getInfo());
        Subscription sub = this.getSubForPush(channel, id);
        if (sub != null) {
            JoinEvent event = new JoinEvent();
            event.setInfo(info);
            sub.getListener().onJoin(sub, event);
        } else if (id > 0) {
            // Compacted push with unknown ID — drop.
            return;
        } else {
            ServerSubscription serverSub = this.getServerSub(channel);
            if (serverSub != null) {
                this.listener.onJoin(this, new ServerJoinEvent(channel, info));
            }
        }
    }

    private void handleLeave(String channel, Protocol.Leave leave, long id) {
        LeaveEvent event = new LeaveEvent();
        ClientInfo info = ClientInfo.fromProtocolClientInfo(leave.getInfo());

        Subscription sub = this.getSubForPush(channel, id);
        if (sub != null) {
            event.setInfo(info);
            sub.getListener().onLeave(sub, event);
        } else if (id > 0) {
            // Compacted push with unknown ID — drop.
            return;
        } else {
            ServerSubscription serverSub = this.getServerSub(channel);
            if (serverSub != null) {
                this.listener.onLeave(this, new ServerLeaveEvent(channel, info));
            }
        }
    }

    private void handleMessage(Protocol.Message msg) {
        MessageEvent event = new MessageEvent();
        event.setData(msg.getData().toByteArray());
        this.listener.onMessage(this, event);
    }

    private void handleDisconnect(Protocol.Disconnect disconnect) {
        int code = disconnect.getCode();
        boolean reconnect = code < 3500 || code >= 5000 || (code >= 4000 && code < 4500);
        if (Client.this.getState() != ClientState.DISCONNECTED) {
            Client.this.processDisconnect(code, disconnect.getReason(), reconnect);
        }
    }

    private void handlePush(Protocol.Push push) throws Exception {
        String channel = push.getChannel();
        // Channel compaction: pub/join/leave pushes may carry a numeric channel ID
        // instead of the channel name. Other push types always carry the channel.
        long id = push.getId();
        if (push.hasPub()) {
            this.handlePub(channel, push.getPub(), id);
        } else if (push.hasSubscribe()) {
            this.handleSubscribe(channel, push.getSubscribe());
        } else if (push.hasJoin()) {
            this.handleJoin(channel, push.getJoin(), id);
        } else if (push.hasLeave()) {
            this.handleLeave(channel, push.getLeave(), id);
        } else if (push.hasUnsubscribe()) {
            this.handleUnsubscribe(channel, push.getUnsubscribe());
        } else if (push.hasMessage()) {
            this.handleMessage(push.getMessage());
        } else if (push.hasDisconnect()) {
            this.handleDisconnect(push.getDisconnect());
        }
    }

    private void handlePing() {
        if (this.pingTask != null) {
            this.pingTask.cancel(true);
        }
        this.pingTask = this.scheduler.schedule(Client.this::waitServerPing, this.pingInterval + this.opts.getMaxServerPingDelay(), TimeUnit.MILLISECONDS);
        if (this.sendPong) {
            // Empty command as a ping.
            Protocol.Command cmd = Protocol.Command.newBuilder().build();
            this.ws.send(ByteString.of(this.serializeCommand(cmd)));
        }
    }

    /**
     * Send asynchronous message with data to server. Callback successfully completes if data
     * written to connection. No reply from server expected in this case.
     *
     * @param data: custom data to publish.
     * @param cb:   will be called as soon as data sent to the connection or error happened.
     */
    public void send(byte[] data, CompletionCallback cb) {
        this.executor.submit(() -> Client.this.sendSynchronized(data, cb));
    }

    private void sendSynchronized(byte[] data, CompletionCallback cb) {
        Protocol.SendRequest req = Protocol.SendRequest.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setSend(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        this.futures.put(cmd.getId(), f);
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            cb.onDone(null);
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                this.cleanCommandFuture(cmd);
                cb.onDone(e);
            });
            return null;
        });

        if (this.getState() != ClientState.CONNECTED) {
            this.connectAsyncCommands.put(cmd.getId(), cmd);
        } else {
            boolean sent = this.ws.send(ByteString.of(this.serializeCommand(cmd)));
            if (!sent) {
                f.completeExceptionally(new IOException());
            } else {
                f.complete(null);
            }
        }
    }

    private void enqueueCommandFuture(Protocol.Command cmd, CompletableFuture<Protocol.Reply> f) {
        this.futures.put(cmd.getId(), f);
        if (this.getState() != ClientState.CONNECTED) {
            this.connectCommands.put(cmd.getId(), cmd);
        } else {
            boolean sent = this.ws.send(ByteString.of(this.serializeCommand(cmd)));
            if (!sent) {
                f.completeExceptionally(new IOException());
            }
        }
    }

    ReplyError getReplyError(Protocol.Reply reply) {
        Protocol.Error error = reply.getError();
        return new ReplyError(
                error.getCode(),
                error.getMessage(),
                error.getTemporary(),
                error.getAppCode(),
                error.getAppType(),
                error.getMetadataMap()
        );
    }

    private void cleanCommandFuture(Protocol.Command cmd) {
        this.futures.remove(cmd.getId());
        if (this.connectCommands.get(cmd.getId()) != null) {
            this.connectCommands.remove(cmd.getId());
        }
        if (Client.this.connectAsyncCommands.get(cmd.getId()) != null) {
            Client.this.connectAsyncCommands.remove(cmd.getId());
        }
    }

    /**
     * Send RPC with method to server, process result in callback.
     *
     * @param method: of RPC call.
     * @param data:   a custom payload for RPC call.
     * @param cb:     will be called as soon as rpc response received or error happened.
     */
    public void rpc(String method, byte[] data, ResultCallback<RPCResult> cb) {
        this.rpcCall(method, data, null, cb);
    }

    /**
     * Send RPC with method and per-call options to server, process result in callback.
     * Headers in the options need a server which confirms RPC options, see
     * {@link #getCapabilities()}. Without it the callback gets error code
     * {@link ReplyError#UNSUPPORTED} and the call is not sent.
     *
     * @param method:  of RPC call.
     * @param data:    a custom payload for RPC call.
     * @param options: per-call options, null or without headers for a plain call.
     * @param cb:      will be called as soon as rpc response received or error happened.
     */
    public void rpc(String method, byte[] data, RpcOptions options, ResultCallback<RPCResult> cb) {
        this.rpcCall(method, data, options, cb);
    }

    /**
     * Send RPC with method to server and get the call back, to cancel it with
     * {@link RpcCall#cancel()}. Same as {@link #rpc(String, byte[], ResultCallback)}.
     *
     * @param method: of RPC call.
     * @param data:   a custom payload for RPC call.
     * @param cb:     will be called as soon as rpc response received or error happened.
     * @return the call.
     */
    public RpcCall rpcCall(String method, byte[] data, ResultCallback<RPCResult> cb) {
        return this.rpcCall(method, data, null, cb);
    }

    /**
     * Send RPC with method and per-call options to server and get the call back, to cancel it
     * with {@link RpcCall#cancel()}. Same as {@link #rpc(String, byte[], RpcOptions, ResultCallback)}.
     * With a server which confirmed RPC options, a call which is not answered is kept across a
     * reconnect and sent again with the same msg_id, so the server runs it once.
     *
     * @param method:  of RPC call.
     * @param data:    a custom payload for RPC call.
     * @param options: per-call options, null for a plain call.
     * @param cb:      will be called as soon as rpc response received or error happened.
     * @return the call.
     */
    public RpcCall rpcCall(String method, byte[] data, RpcOptions options, ResultCallback<RPCResult> cb) {
        Map<String, String> headers = options != null ? options.getHeaders() : Collections.<String, String>emptyMap();
        long timeout = options != null && options.getTimeout() > 0 ? options.getTimeout() : this.opts.getTimeout();
        RpcCall call = new RpcCall(this, method, data, headers, timeout, cb);
        this.executor.submit(() -> Client.this.rpcSynchronized(call));
        return call;
    }

    private void rpcSynchronized(RpcCall call) {
        if (this.refreshInFlight) {
            this.pendingAfterRefresh.add(() -> Client.this.rpcSynchronized(call));
            return;
        }
        // The deadline starts here and holds across resends. The callback runs on the
        // executor, also when the call was cancelled before it started.
        call.future.orTimeout(call.timeout, TimeUnit.MILLISECONDS)
                .whenComplete((reply, error) -> this.executor.submit(() -> this.finishRpc(call, reply, error)));
        if (call.future.isDone()) {
            return;
        }
        call.msgId = this.nextMsgId();
        this.enqueueRpc(call);
    }

    private void enqueueRpc(RpcCall call) {
        if (this.getState() == ClientState.CONNECTED) {
            this.writeRpc(call, false);
        } else if (!this.rpcQueue.offer(call)) {
            call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls wait for the connection", false));
        }
    }

    // Writes the call on the current transport. A call with headers on a connection without
    // RPC options fails here with the reply error a server would give, and nothing is written.
    // A call with a msg_id which did not reach the server waits for the next connection.
    private void writeRpc(RpcCall call, boolean retransmit) {
        if (!this.rpcOptionsSupported && !call.headers.isEmpty()) {
            call.future.complete(Extensions.unsupportedRpcReply(call.commandId));
            return;
        }
        int id = this.getNextId();
        call.commandId = id;
        boolean tracked = this.rpcOptionsSupported;
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(id)
                .setRpc(this.buildRpcRequest(call, retransmit))
                .build();
        this.futures.put(id, call.future);
        this.rpcsOnWire.put(id, call);
        boolean sent = this.ws.send(ByteString.of(this.serializeCommand(cmd)));
        if (sent) {
            call.everSent = true;
            call.wireTracked = tracked;
            return;
        }
        this.futures.remove(id);
        this.rpcsOnWire.remove(id);
        if (!tracked || !this.rpcQueue.offer(call)) {
            call.future.completeExceptionally(new IOException());
        }
    }

    private Protocol.RPCRequest buildRpcRequest(RpcCall call, boolean retransmit) {
        Protocol.RPCRequest.Builder req = Protocol.RPCRequest.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(call.data))
                .setMethod(call.method)
                .putAllHeaders(call.headers);
        if (this.rpcOptionsSupported) {
            req.setMsgId(call.msgId)
                    .setRetransmit(retransmit)
                    .addAllAck(this.pendingAcks);
            this.pendingAcks.clear();
        }
        return req.build();
    }

    // Sends the rpc calls that waited for the connection, once its connect reply is in. Calls
    // lost with an earlier transport are sent again in msg_id order, with retransmit set, and
    // keep their deadline. Without RPC options on this connection they fail instead.
    private void flushRpcQueue() {
        for (RpcCall call : this.rpcQueue.drain()) {
            if (call.cancelPending) {
                if (this.rpcOptionsSupported) {
                    this.writeCancel(call.msgId);
                }
                continue;
            }
            if (call.future.isDone()) {
                continue;
            }
            if (call.everSent && !this.rpcOptionsSupported) {
                call.future.completeExceptionally(new IOException());
                continue;
            }
            this.writeRpc(call, call.everSent);
        }
    }

    // Sends a cancel for a msg_id: an RPCRequest without method and data. Its reply is not awaited.
    private void writeCancel(long msgId) {
        Protocol.RPCRequest req = Protocol.RPCRequest.newBuilder()
                .setCancel(msgId)
                .addAllAck(this.pendingAcks)
                .build();
        this.pendingAcks.clear();
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setRpc(req)
                .build();
        this.ws.send(ByteString.of(this.serializeCommand(cmd)));
    }

    void cancelRpc(RpcCall call) {
        this.executor.submit(() -> Client.this.cancelRpcSynchronized(call));
    }

    private void cancelRpcSynchronized(RpcCall call) {
        if (call.future.isDone()) {
            return;
        }
        if (this.getState() == ClientState.CONNECTED && !this.rpcOptionsSupported) {
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel is not supported by the server", false));
            return;
        }
        if (!call.everSent) {
            this.rpcQueue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false));
            return;
        }
        if (this.getState() == ClientState.CONNECTED) {
            this.writeCancel(call.msgId);
        } else {
            // The call was lost with its transport and waits in the queue: the cancel goes out
            // with the next connection, or is dropped when that one has no RPC options.
            call.cancelPending = true;
            this.rpcQueue.keep(call);
        }
        call.future.completeExceptionally(new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false));
    }

    private void finishRpc(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (call.commandId != 0) {
            this.futures.remove(call.commandId);
            this.rpcsOnWire.remove(call.commandId);
        }
        if (!call.cancelPending) {
            this.rpcQueue.remove(call);
        }
        if (reply == null) {
            Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
            call.callback.onDone(cause, null);
            return;
        }
        if (call.wireTracked) {
            this.pendingAcks.add(call.msgId);
        }
        if (reply.getError().getCode() != 0) {
            call.callback.onDone(getReplyError(reply), null);
            return;
        }
        Protocol.RPCResult rpcResult = reply.getRpc();
        RPCResult result = new RPCResult();
        result.setData(rpcResult.getData().toByteArray());
        result.setCached(rpcResult.getCached());
        call.callback.onDone(null, result);
    }

    // msg_id of the next rpc call, see Extensions.nextMsgId. Used on the executor thread only.
    private long nextMsgId() {
        this.lastMsgId = Extensions.nextMsgId(this.serverTimeMillis(), this.lastMsgId);
        return this.lastMsgId;
    }

    /**
     * Publish data to channel without being subscribed to it. Publish option should be
     * enabled in Centrifuge/Centrifugo server configuration.
     *
     * @param channel: to publish into.
     * @param data:    to publish.
     * @param cb:      will be called as soon as publish response received or error happened.
     */
    public void publish(String channel, byte[] data, ResultCallback<PublishResult> cb) {
        this.executor.submit(() -> Client.this.publishSynchronized(channel, data, cb));
    }

    private void publishSynchronized(String channel, byte[] data, ResultCallback<PublishResult> cb) {
        Protocol.PublishRequest req = Protocol.PublishRequest.newBuilder()
                .setChannel(channel)
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setPublish(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                PublishResult result = new PublishResult();
                cb.onDone(null, result);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }

    /**
     * History can get channel publication history (useful for server-side subscriptions).
     *
     * @param channel: to get history for.
     * @param opts:    for history request.
     * @param cb:      will be called as soon as response receive or error happened.
     */
    public void history(String channel, HistoryOptions opts, ResultCallback<HistoryResult> cb) {
        this.executor.submit(() -> Client.this.historySynchronized(channel, opts, cb));
    }

    private void historySynchronized(String channel, HistoryOptions opts, ResultCallback<HistoryResult> cb) {
        Protocol.HistoryRequest.Builder builder = Protocol.HistoryRequest.newBuilder()
                .setChannel(channel)
                .setReverse(opts.getReverse())
                .setLimit(opts.getLimit());
        if (opts.getSince() != null) {
            builder.setSince(opts.getSince().toProto());
        }
        Protocol.HistoryRequest req = builder.build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setHistory(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                Protocol.HistoryResult replyResult = reply.getHistory();
                HistoryResult result = new HistoryResult();
                List<Protocol.Publication> protoPubs = replyResult.getPublicationsList();
                List<Publication> pubs = new ArrayList<>();
                for (int i = 0; i < protoPubs.size(); i++) {
                    Protocol.Publication protoPub = protoPubs.get(i);
                    Publication pub = new Publication();
                    pub.setData(protoPub.getData().toByteArray());
                    pub.setOffset(protoPub.getOffset());
                    pub.setInfo(ClientInfo.fromProtocolClientInfo(protoPub.getInfo()));
                    pubs.add(pub);
                }
                result.setPublications(pubs);
                result.setOffset(replyResult.getOffset());
                result.setEpoch(replyResult.getEpoch());
                cb.onDone(null, result);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }

    public void presence(String channel, ResultCallback<PresenceResult> cb) {
        this.executor.submit(() -> Client.this.presenceSynchronized(channel, cb));
    }

    private void presenceSynchronized(String channel, ResultCallback<PresenceResult> cb) {
        Protocol.PresenceRequest req = Protocol.PresenceRequest.newBuilder()
                .setChannel(channel)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setPresence(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                Protocol.PresenceResult replyResult = reply.getPresence();
                Map<String, Protocol.ClientInfo> protoPresence = replyResult.getPresenceMap();
                Map<String, ClientInfo> presence = new HashMap<>();
                for (Map.Entry<String, Protocol.ClientInfo> entry : protoPresence.entrySet()) {
                    Protocol.ClientInfo protoClientInfo = entry.getValue();
                    presence.put(entry.getKey(), ClientInfo.fromProtocolClientInfo(protoClientInfo));
                }
                PresenceResult result = new PresenceResult(presence);
                cb.onDone(null, result);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }

    public void presenceStats(String channel, ResultCallback<PresenceStatsResult> cb) {
        this.executor.submit(() -> Client.this.presenceStatsSynchronized(channel, cb));
    }

    private void presenceStatsSynchronized(String channel, ResultCallback<PresenceStatsResult> cb) {
        Protocol.PresenceStatsRequest req = Protocol.PresenceStatsRequest.newBuilder()
                .setChannel(channel)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setPresenceStats(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                Protocol.PresenceStatsResult replyResult = reply.getPresenceStats();
                PresenceStatsResult result = new PresenceStatsResult();
                result.setNumClients(replyResult.getNumClients());
                result.setNumUsers(replyResult.getNumUsers());
                cb.onDone(null, result);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }

    /**
     * Refresh the connection with a new token now, without waiting for the server to ask for one.
     * On success the callback gets the identity the server reports after the refresh, and the
     * refresh timer is rescheduled from the new TTL. An error leaves the connection and its
     * identity as they were. Calls to {@link #rpc} issued while the refresh is in flight are sent
     * after its reply, so they run under the new identity. Refresh calls are sent one after
     * another, each with its own token, in call order.
     *
     * @param token: new connection token, must not be null.
     * @param cb:    will be called when the refresh reply is received or an error happened.
     */
    public void refresh(String token, ResultCallback<AuthResult> cb) {
        if (token == null) {
            throw new NullPointerException("token must not be null");
        }
        this.executor.submit(() -> Client.this.refreshRequestSynchronized(token, cb));
    }

    private void refreshRequestSynchronized(String token, ResultCallback<AuthResult> cb) {
        if (this.refreshInFlight) {
            this.pendingAfterRefresh.add(() -> Client.this.refreshRequestSynchronized(token, cb));
            return;
        }
        this.refreshInFlight = true;
        this.token = token;
        this.refreshSynchronized(token, (error, result) -> this.executor.submit(() -> this.finishRefresh(error, result, cb)));
    }

    private void finishRefresh(Throwable error, Protocol.RefreshResult result, ResultCallback<AuthResult> cb) {
        this.refreshInFlight = false;
        if (error == null && this.getState() == ClientState.CONNECTED) {
            if (this.refreshTask != null) {
                this.refreshTask.cancel(true);
                this.refreshTask = null;
            }
            if (result.getExpires()) {
                this.refreshTask = this.scheduler.schedule(Client.this::sendRefresh, result.getTtl(), TimeUnit.SECONDS);
            }
        }
        this.drainAfterRefresh();
        if (error != null) {
            cb.onDone(error, null);
        } else {
            cb.onDone(null, new AuthResult(result.getUser(), result.getAuthClass(), result.getExpires(), result.getTtl()));
        }
    }

    // Runs the calls that waited for a refresh, in order, until one starts another refresh.
    private void drainAfterRefresh() {
        while (!this.refreshInFlight && !this.pendingAfterRefresh.isEmpty()) {
            Runnable next = this.pendingAfterRefresh.poll();
            next.run();
        }
    }

    private void refreshSynchronized(String token, ResultCallback<Protocol.RefreshResult> cb) {
        Protocol.RefreshRequest req = Protocol.RefreshRequest.newBuilder()
                .setToken(token)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setRefresh(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                Protocol.RefreshResult replyResult = reply.getRefresh();
                this.applyAuth(replyResult.getUser(), replyResult.getAuthClass(), false);
                cb.onDone(null, replyResult);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }

    void subRefreshSynchronized(String channel, String token, ResultCallback<Protocol.SubRefreshResult> cb) {
        Protocol.SubRefreshRequest req = Protocol.SubRefreshRequest.newBuilder()
                .setToken(token)
                .setChannel(channel)
                .build();

        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(this.getNextId())
                .setSubRefresh(req)
                .build();

        CompletableFuture<Protocol.Reply> f = new CompletableFuture<>();
        f.thenAccept(reply -> {
            this.cleanCommandFuture(cmd);
            if (reply.getError().getCode() != 0) {
                cb.onDone(getReplyError(reply), null);
            } else {
                Protocol.SubRefreshResult replyResult = reply.getSubRefresh();
                cb.onDone(null, replyResult);
            }
        }).orTimeout(this.opts.getTimeout(), TimeUnit.MILLISECONDS).exceptionally(e -> {
            this.executor.submit(() -> {
                Client.this.cleanCommandFuture(cmd);
                cb.onDone(e, null);
            });
            return null;
        });

        this.enqueueCommandFuture(cmd, f);
    }
}
