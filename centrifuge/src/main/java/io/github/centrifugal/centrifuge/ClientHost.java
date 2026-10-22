package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;
import okhttp3.WebSocket;


/**
 * The part of a {@link Client} which the rpc session and the token refreshes use. They reach the
 * client through this class only, so the client needs no hook of its own for them. Used on the
 * client's executor thread, except where a method says otherwise.
 */
final class ClientHost {
    private final Client client;


    ClientHost(Client client) {
        this.client = client;
    }


    ClientState state() {
        return client.getState();
    }


    long serverTimeMillis() {
        return client.serverTimeMillis();
    }


    ScheduledExecutorService scheduler() {
        return client.getScheduler();
    }


    ExecutorService executor() {
        return client.getExecutor();
    }


    int timeoutMillis() {
        return client.getOpts().getTimeout();
    }


    ConnectionTokenGetter tokenGetter() {
        return client.getOpts().getTokenGetter();
    }


    boolean isRefreshRequired() {
        return client.refreshRequired;
    }


    void setRefreshRequired(boolean refreshRequired) {
        client.refreshRequired = refreshRequired;
    }


    Options options() {
        return client.getOpts();
    }


    /** Handles the connect reply once a token exchange completed it: the connected state follows. */
    void resumeConnect(Protocol.Reply reply) {
        client.handleConnectReply(reply);
    }


    /** Sets the ping settings of a connect reply which is held by a token exchange, as the connected reply sets them. */
    void applyPing(Protocol.ConnectResult result) {
        client.pingInterval = result.getPing() * 1000;
        client.sendPong = result.getPong();
    }


    /** Moves on with each refresh() call and each accepted token, so a change since an exchange started is seen. */
    long refreshGeneration() {
        return client.auth.refreshGeneration();
    }


    WebSocket transport() {
        return client.ws;
    }


    /**
     * Cancels the transport, whose peer may be unresponsive: a graceful close waits for the peer's close frame, which a
     * dead peer never sends, and the reconnect would wait for it too. The client reconnects when the transport fails.
     */
    void closeTransport() {
        WebSocket transport = transport();
        if (transport != null) {
            transport.cancel();
        }
    }


    boolean hasInstance() {
        return !client.instance.isEmpty();
    }


    boolean hasRpcOptions() {
        return client.rpcOptionsSupported;
    }


    boolean isRefreshing() {
        return client.auth.isInFlight();
    }


    int nextCommandId() {
        return client.getNextId();
    }


    /**
     * Sends the command on the current transport. Returns whether the transport took it. Throws when the
     * command cannot be encoded, so that only the call of the command fails. The frame is the one
     * Client.serializeCommand writes, without the copies of its stream: see {@link FrameEncoder}.
     */
    boolean sendCommand(Protocol.Command cmd) throws IOException {
        return client.ws.send(FrameEncoder.encode(cmd));
    }


    void addFuture(int id, CompletableFuture<Protocol.Reply> future) {
        client.futures.put(id, future);
    }


    void removeFuture(int id) {
        client.futures.remove(id);
    }


    void enqueueCommand(Protocol.Command cmd, CompletableFuture<Protocol.Reply> future) {
        client.enqueueCommandFuture(cmd, future);
    }


    void cleanCommand(Protocol.Command cmd) {
        client.cleanCommandFuture(cmd);
    }


    ReplyError replyError(Protocol.Reply reply) {
        return client.getReplyError(reply);
    }


    /**
     * Reports a failure to the listener as it was thrown. The report ends nothing: a listener which throws on it, or
     * throws an Error, has its failure printed here, so the handling which reports goes on and the failure is not lost.
     */
    void reportError(Throwable cause) {
        try {
            client.listener.onError(client, new ErrorEvent(cause));
        } catch (Throwable failure) {
            failure.printStackTrace();
        }
    }


    /** Reports a failure of the protocol handling as an error event, wrapped in an {@link UnclassifiedError}. */
    void reportUnclassified(Throwable cause) {
        reportError(new UnclassifiedError(cause));
    }


    void reportAuth(AuthEvent event) {
        client.listener.onAuth(client, event);
    }


    String connectionToken() {
        return client.token;
    }


    void storeToken(String token) {
        client.token = token;
    }


    void failUnauthorized() {
        client.failUnauthorized();
    }


    void disconnect(int code, String reason) {
        client.processDisconnect(code, reason, false);
    }


    void flushRpc() {
        client.rpcSession.flush();
    }


    /**
     * The task which reports a failure of its own to the listener, instead of letting it go: the executor
     * would drop it, and nothing else would tell the caller. The failure is wrapped in an {@link UnclassifiedError}
     * as {@link #reportUnclassified} does: an exception escaping an app callback is reported the same way in every step.
     */
    Runnable reporting(final Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable failure) {
                reportUnclassified(failure);
            }
        };
    }


    /** Runs the task on the executor, or inline once the client closed, see {@link AuthRefresh#runOnExecutorOrInline}. */
    void runOrInline(Runnable task) {
        client.auth.runOnExecutorOrInline(task);
    }
}
