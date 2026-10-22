package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;
import java8.util.concurrent.CompletableFuture;


/**
 * One rpc call made with {@link Client#rpcCall}. {@link #cancel()} stops the call on the
 * server when the server supports it, and rejects the call locally with
 * {@link ReplyError#CANCELLED}.
 */
public final class RpcCall {
    private final RpcSession session;
    final String method;
    // Dropped when the call settles, since a late reply needs only the msg_id of the call.
    byte[] data;
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
    // Holds a slot of the bound on the calls of the client, taken at submit and given back when the call
    // settles, on whichever thread settles it. A call refused by the bound holds none.
    final AtomicBoolean holdsSlot = new AtomicBoolean();
    // Written to a transport at least once.
    boolean everSent;
    // The last write carried msg_id, so the server knows the call and may answer a resend.
    boolean wireTracked;
    // The msg_id was refused, so the next write carries a new one, which the server has not seen:
    // that write is not a retransmit. Cleared when the write goes out.
    boolean resendAsNew;
    // The server gave its final answer to the call, a reply which settles the call or a late one, so no cancel is due.
    boolean answered;
    // Resends so far: after a refused msg_id, and after a temporary refusal.
    int newIdResends;
    int laterResends;
    // The deadline of the call, a timer of the client which completes the future with a TimeoutException.
    // Cancelled once the call is finished on the executor.
    ScheduledFuture<?> deadlineTask;
    // The attempt on the wire: its future completes with the answer of the transport, and the executor
    // clears the field once it has decided on the answer.
    CompletableFuture<Protocol.Reply> attempt;
    // The resend which waits for its delay, cancelled with the call. The generation tells the timer of
    // a resend from the timer of a later one.
    ScheduledFuture<?> resendTask;
    int resendGeneration;
    // The acks the last write carried. They go back to the pending acks if the write is lost with its transport.
    List<Long> carriedAcks = Collections.emptyList();
    // The forgetting of the attempt of a call which settled locally, while a late reply is still acked.
    ScheduledFuture<?> lateTask;


    RpcCall(
            RpcSession session,
            String method,
            byte[] data,
            Map<String, String> headers,
            long timeout,
            ResultCallback<RPCResult> callback
    ) {
        this.session = session;
        this.method = method;
        this.data = data;
        this.headers = headers;
        this.timeout = timeout;
        this.callback = callback;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }


    /**
     * Cancels the call and rejects it with {@link ReplyError#CANCELLED}. A call which was not
     * sent yet is never sent, and is rejected with {@link ReplyError#CANCELLED} whatever the connection
     * state. A call which was sent is cancelled on the server over the current connection, or over the
     * next one when the connection is down. For a client without an instance, cancelling a call which was
     * sent rejects it with {@link ReplyError#UNSUPPORTED} whatever the connection state, and nothing is sent.
     * On a connected server which does not support RPC options, cancelling a call which was sent rejects it
     * with {@link ReplyError#UNSUPPORTED} too. While the client is connecting or disconnected the capability
     * is not known yet, and a call which was sent gets its cancel with the next connection which confirms
     * RPC options. Has no effect once the call is done.
     */
    public void cancel() {
        if (future.isDone()) {
            return;
        }
        this.session.requestCancel(this);
    }


    // The time left until the deadline, in nanoseconds. Zero or less when the deadline has passed. The deadline
    // timer gives it, so the resends and the deadline run on one clock.
    long remainingNanos() {
        if (deadlineTask != null) {
            return deadlineTask.getDelay(TimeUnit.NANOSECONDS);
        }
        return deadlineNanos - System.nanoTime();
    }
}
