package io.github.centrifugal.centrifuge;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;
import okhttp3.WebSocket;
import okio.ByteString;


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


    WebSocket transport() {
        return client.ws;
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
     * command cannot be encoded, so that only the call of the command fails. The encoding mirrors
     * Client.serializeCommand, which swallows the failure: a copy here, so the failure reaches the call.
     */
    boolean sendCommand(Protocol.Command cmd) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        cmd.writeDelimitedTo(stream);
        return client.ws.send(ByteString.of(stream.toByteArray()));
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


    void reportError(Throwable cause) {
        client.listener.onError(client, new ErrorEvent(cause));
    }


    void reportUnclassified(Throwable cause) {
        Extensions.reportUnclassified(client.listener, client, cause);
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
     * would drop it, and nothing else would tell the caller. The failure goes to the listener as it was
     * thrown, not wrapped in an {@link UnclassifiedError} as {@link #reportUnclassified} does: the task runs
     * the listener's own callbacks, and their exceptions keep their type for the listener.
     */
    Runnable reporting(final Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (RuntimeException failure) {
                reportError(failure);
            }
        };
    }
}
