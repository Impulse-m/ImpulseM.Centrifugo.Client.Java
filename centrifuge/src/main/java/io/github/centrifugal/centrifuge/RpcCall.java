package io.github.centrifugal.centrifuge;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;
import java8.util.concurrent.CompletableFuture;


/**
 * One rpc call made with {@link Client#rpcCall}. {@link #cancel()} stops the call on the
 * server when the server supports it, and rejects the call locally with
 * {@link ReplyError#CANCELLED}.
 */
public final class RpcCall {
    private final Client client;
    final String method;
    final byte[] data;
    final Map<String, String> headers;
    final long timeout;
    final ResultCallback<RPCResult> callback;
    // The deadline of the call on the nano clock. It starts with the call, so it bounds the resends too.
    final long deadlineNanos;

    // The fields below are the state of the call on the client's executor thread.
    // The future is created with the call, so a cancel can complete it before the call starts.
    final CompletableFuture<Protocol.Reply> future = new CompletableFuture<>();
    long msgId;
    int commandId;
    // Written to a transport at least once.
    boolean everSent;
    // The last write carried msg_id, so the server knows the call and may answer a resend.
    boolean wireTracked;
    // The msg_id was refused, so the next write carries a new one, which the server has not seen:
    // that write is not a retransmit. Cleared when the write goes out.
    boolean resendAsNew;
    // Resends so far: after a refused msg_id, and after a temporary refusal.
    int newIdResends;
    int laterResends;
    // The resend which waits for its delay, cancelled with the call.
    ScheduledFuture<?> resendTask;


    RpcCall(
            Client client,
            String method,
            byte[] data,
            Map<String, String> headers,
            long timeout,
            ResultCallback<RPCResult> callback
    ) {
        this.client = client;
        this.method = method;
        this.data = data;
        this.headers = headers;
        this.timeout = timeout;
        this.callback = callback;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }


    /**
     * Cancels the call and rejects it with {@link ReplyError#CANCELLED}. A call which was not
     * sent yet is never sent. A call which was sent is cancelled on the server over the current
     * connection, or over the next one when the connection is down. On a connected server which
     * does not support RPC options, or for a client without an instance, cancelling a call rejects it
     * with {@link ReplyError#UNSUPPORTED} and nothing is sent. While the client is connecting or disconnected the capability is not known
     * yet, so a call which was not sent is rejected with {@link ReplyError#CANCELLED} and is never sent,
     * and a call which was sent gets its cancel with the next connection which confirms RPC options.
     * Has no effect once the call is done.
     */
    public void cancel() {
        if (future.isDone()) {
            return;
        }
        this.client.cancelRpc(this);
    }


    // The time left until the deadline, in nanoseconds. Zero or less when the deadline has passed.
    long remainingNanos() {
        return deadlineNanos - System.nanoTime();
    }
}
