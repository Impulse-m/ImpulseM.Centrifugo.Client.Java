package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletableFuture;


/**
 * The exactly-once rpc calls of one client. A call waits in the queue until it is written to a
 * connection which confirms RPC options. A call with a msg_id which is lost with its transport
 * stays in the queue and is sent again, with retransmit set, after the next connect. A call which
 * the server refused with the retry signal is sent again while its attempts and its deadline last.
 * Replies are acknowledged with the next rpc request, and cancels go out as rpc requests of their
 * own. Used on the client's executor thread only, except {@link #submit}.
 */
final class RpcSession {
    // A call whose msg_id the server refused is sent again this many times, each time with a new msg_id.
    private static final int NEW_ID_RESENDS = 2;
    // A call the server refused for now is sent again this many times: after 250 ms, 500 ms and 1 s.
    private static final int LATER_RESENDS = 3;
    private static final long LATER_BASE_NANOS = TimeUnit.MILLISECONDS.toNanos(250L);

    private final Client client;
    // Calls which wait for the connection or for a refresh, and calls lost with their transport.
    private final RpcQueue queue;
    // Calls written to the current transport, by the id of their command.
    private final Map<Integer, RpcCall> onWire = new HashMap<>();
    // Calls whose resend waits for its delay. A transport loss moves them to the queue.
    private final Set<RpcCall> resendWaiting = new HashSet<>();
    // msg_ids of the replies received, sent as the ack of the next rpc request.
    private final List<Long> pendingAcks = new ArrayList<>();
    // msg_ids of calls cancelled, or past their deadline, while no connection which confirms RPC
    // options could carry the cancel. Not counted against the queue bound: a cancel needs no resend of
    // its call. A connection which confirms RPC options takes the whole list, so the list is bounded by
    // the sent calls made since the last such connection, not by the queue bound. It is not capped,
    // since a dropped cancel would leave the call running on the server.
    private final List<Long> pendingCancels = new ArrayList<>();
    private final MsgIdGenerator msgIds = new MsgIdGenerator();


    RpcSession(Client client, int capacity) {
        this.client = client;
        this.queue = new RpcQueue(capacity);
    }


    /**
     * Starts the deadline of the call and hands the call to the executor. Called on the caller's
     * thread. The deadline starts here, so it holds for a call waiting for a refresh, and across
     * resends. The callback runs on the executor, also when the call was cancelled before it started.
     */
    void submit(RpcCall call) {
        // The executor rejects the call once the client is closed. Nothing is registered then, so
        // the deadline cannot fire later and call back on another thread.
        client.getExecutor().submit(() -> start(call));
        call.future.orTimeout(call.timeout, TimeUnit.MILLISECONDS)
                .whenComplete((Protocol.Reply reply, Throwable error) -> finishOnExecutor(call, reply, error));
    }


    /**
     * Cancels the call. A call which was never sent is never sent. On a connection whose server
     * does not support RPC options, a cancel is rejected with UNSUPPORTED whether the call was sent
     * or not, since the server cannot cancel anything. While the capability is not known, the call
     * which was not sent is rejected with CANCELLED.
     */
    void cancel(RpcCall call) {
        if (call.future.isDone()) {
            return;
        }
        stopResend(call);
        if (client.getState() == ClientState.CONNECTED && !client.hasRpcOptions()) {
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel is not supported by the server", false));
            return;
        }
        if (client.getState() == ClientState.CONNECTED && !client.hasInstance()) {
            // Without an instance the calls are plain: the server has no record of them to cancel.
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel needs the instance option", false));
            return;
        }
        if (!call.everSent) {
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false));
            return;
        }
        queue.remove(call);
        sendCancel(call.msgId);
        call.future.completeExceptionally(new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false));
    }


    /**
     * Writes the calls which wait for the connection, once its connect reply is in and no refresh
     * is in flight: the pending cancels first, then the calls in resend order.
     */
    void flush() {
        if (client.getState() != ClientState.CONNECTED || client.isRefreshing()) {
            return;
        }
        if (!client.hasRpcOptions()) {
            // A server without RPC options keeps no record of the calls, so there is nothing to cancel.
            pendingCancels.clear();
        }
        List<Long> cancels = new ArrayList<>(pendingCancels);
        pendingCancels.clear();
        for (long msgId : cancels) {
            if (client.hasRpcOptions() && !writeCancel(msgId)) {
                pendingCancels.add(msgId);
            }
        }
        for (RpcCall call : queue.drain()) {
            if (call.future.isDone()) {
                continue;
            }
            if (call.everSent && !client.hasRpcOptions()) {
                call.future.completeExceptionally(new IOException());
                continue;
            }
            write(call);
        }
    }


    /**
     * The transport is gone. Calls with a msg_id are kept for a resend when the client reconnects,
     * the others fail. Without a reconnect every call fails. A kept call is never failed for the
     * queue bound, since the server may have run it: the queue may exceed the bound, and new calls
     * wait until it drains below the bound.
     */
    void onTransportLost(boolean keep) {
        List<RpcCall> lost = new ArrayList<>();
        for (Map.Entry<Integer, RpcCall> entry : onWire.entrySet()) {
            RpcCall call = entry.getValue();
            client.removeFuture(entry.getKey());
            if (keep && call.wireTracked) {
                lost.add(call);
            } else {
                call.future.completeExceptionally(new IOException());
            }
        }
        onWire.clear();
        // A resend which waits for its delay is a call lost with its transport: it goes out with the reconnect.
        for (RpcCall call : new ArrayList<>(resendWaiting)) {
            stopResend(call);
            if (keep && call.wireTracked) {
                lost.add(call);
            } else {
                call.future.completeExceptionally(new IOException());
            }
        }
        lost.sort((RpcCall first, RpcCall second) -> Long.compare(first.msgId, second.msgId));
        for (RpcCall call : lost) {
            queue.add(call);
        }
        if (!keep) {
            for (RpcCall call : queue.drain()) {
                call.future.completeExceptionally(new IOException());
            }
        }
    }


    // Finishes the call on the executor. Once the client is closed the executor rejects the task:
    // the session state went with the executor, so only the callback of the call runs, on the
    // calling thread, exactly once.
    private void finishOnExecutor(RpcCall call, Protocol.Reply reply, Throwable error) {
        try {
            client.getExecutor().submit(() -> finish(call, reply, error));
        } catch (RejectedExecutionException e) {
            deliver(call, reply, error);
        }
    }


    // Writes the call now when the connection is up and no refresh is in flight. Otherwise the call
    // waits in the queue, unless the queue is full: the bound applies to the calls issued here.
    private void start(RpcCall call) {
        if (call.future.isDone()) {
            return;
        }
        if (client.getState() == ClientState.CONNECTED && !client.isRefreshing()) {
            write(call);
        } else if (queue.isFull()) {
            call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls wait for the connection", false));
        } else {
            queue.add(call);
        }
    }


    // Writes the call now when the connection is up and no refresh is in flight, and queues it otherwise.
    private void writeOrQueue(RpcCall call) {
        if (client.getState() == ClientState.CONNECTED && !client.isRefreshing()) {
            write(call);
        } else {
            queue.add(call);
        }
    }


    // Writes the call on the current transport. A call with headers on a connection without RPC
    // options fails with the reply error the server would give. A call is tracked when the connection
    // confirms RPC options and the client has an instance: only a tracked call carries a msg_id. A call
    // which was not sent takes a new msg_id on each attempt, since nothing reached the server. A call
    // which was sent keeps its msg_id, and is marked as a resend.
    private void write(RpcCall call) {
        boolean supported = client.hasRpcOptions();
        if (!supported && !call.headers.isEmpty()) {
            call.future.complete(Extensions.unsupportedRpcReply(0));
            return;
        }
        boolean tracked = supported && client.hasInstance();
        if (tracked && !call.everSent) {
            try {
                call.msgId = msgIds.next(client.serverTimeMillis());
            } catch (IllegalStateException e) {
                call.future.completeExceptionally(e);
                return;
            }
        }
        int id = client.nextCommandId();
        call.commandId = id;
        List<Long> acks = tracked ? takeAcks() : Collections.<Long>emptyList();
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(id)
                .setRpc(buildRpcRequest(call, tracked, acks))
                .build();
        // Each attempt has a future of its own, which the transport settles. A refusal which is sent
        // again settles the attempt only, not the call.
        CompletableFuture<Protocol.Reply> attempt = new CompletableFuture<>();
        attempt.whenComplete((Protocol.Reply reply, Throwable error) -> settleAttempt(call, reply, error));
        client.addFuture(id, attempt);
        onWire.put(id, call);
        if (client.sendCommand(cmd)) {
            call.everSent = true;
            call.resendAsNew = false;
            call.wireTracked = tracked;
            return;
        }
        client.removeFuture(id);
        onWire.remove(id);
        pendingAcks.addAll(0, acks);
        if (!tracked) {
            call.future.completeExceptionally(new IOException());
        } else if (!call.everSent && queue.isFull()) {
            call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls wait for the connection", false));
        } else {
            queue.add(call);
        }
    }


    private static Protocol.RPCRequest buildRpcRequest(RpcCall call, boolean tracked, List<Long> acks) {
        Protocol.RPCRequest.Builder req = Protocol.RPCRequest.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(call.data))
                .setMethod(call.method)
                .putAllHeaders(call.headers);
        if (tracked) {
            req.setMsgId(call.msgId)
                    .setRetransmit(call.everSent && !call.resendAsNew)
                    .addAllAck(acks);
        }
        return req.build();
    }


    // Sends the cancel of a msg_id now when the connection can carry it, and keeps it for the next
    // connection which confirms RPC options otherwise.
    private void sendCancel(long msgId) {
        if (client.getState() != ClientState.CONNECTED || !client.hasRpcOptions() || !writeCancel(msgId)) {
            pendingCancels.add(msgId);
        }
    }


    // Sends a cancel for a msg_id: an rpc request without method and data, whose reply is not
    // awaited. Returns false when the command was not sent, and its acks wait for the next command.
    private boolean writeCancel(long msgId) {
        List<Long> acks = takeAcks();
        Protocol.RPCRequest req = Protocol.RPCRequest.newBuilder()
                .setCancel(msgId)
                .addAllAck(acks)
                .build();
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(client.nextCommandId())
                .setRpc(req)
                .build();
        if (client.sendCommand(cmd)) {
            return true;
        }
        pendingAcks.addAll(0, acks);
        return false;
    }


    private List<Long> takeAcks() {
        List<Long> acks = new ArrayList<>(pendingAcks);
        pendingAcks.clear();
        return acks;
    }


    // Starts the resend of a call whose error reply carries the retry signal, while the attempts of
    // its kind and its deadline last. Returns false when the error is delivered as it is.
    private boolean resend(RpcCall call, Protocol.Error error) {
        String signal = Extensions.retrySignal(error);
        long remainingNanos = call.remainingNanos();
        if (signal == null || remainingNanos <= 0L) {
            return false;
        }
        if (Extensions.RPC_RETRY_NEW_MSG_ID.equals(signal) && call.newIdResends < NEW_ID_RESENDS) {
            call.newIdResends++;
            resendWithNewId(call);
            return true;
        }
        if (Extensions.RPC_RETRY_LATER.equals(signal) && call.laterResends < LATER_RESENDS) {
            long delayNanos = Math.min(LATER_BASE_NANOS << call.laterResends, remainingNanos);
            call.laterResends++;
            scheduleResend(call, delayNanos);
            return true;
        }
        return false;
    }


    // Sends the refused call again with a new msg_id. The generator is re-synced to the server time
    // of the refusal first. The refused msg_id was never recorded by the server, so it is not acked.
    private void resendWithNewId(RpcCall call) {
        msgIds.resync(client.serverTimeMillis());
        try {
            call.msgId = msgIds.next(client.serverTimeMillis());
        } catch (IllegalStateException e) {
            call.future.completeExceptionally(e);
            return;
        }
        call.resendAsNew = true;
        writeOrQueue(call);
    }


    // Sends the refused call again with the same msg_id once the delay is over, on the executor.
    private void scheduleResend(RpcCall call, long delayNanos) {
        resendWaiting.add(call);
        call.resendTask = client.getScheduler().schedule(() -> client.getExecutor().submit(() -> resendLater(call)), delayNanos, TimeUnit.NANOSECONDS);
    }


    // The delay of a resend is over. The call goes out as a retransmit, unless it settled meanwhile.
    private void resendLater(RpcCall call) {
        if (!resendWaiting.contains(call) || call.future.isDone()) {
            return;
        }
        stopResend(call);
        writeOrQueue(call);
    }


    // Forgets the resend of the call which waits for its delay, and cancels the delay.
    private void stopResend(RpcCall call) {
        resendWaiting.remove(call);
        if (call.resendTask != null) {
            call.resendTask.cancel(false);
            call.resendTask = null;
        }
    }


    private void finish(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (call.commandId != 0) {
            client.removeFuture(call.commandId);
            onWire.remove(call.commandId);
        }
        queue.remove(call);
        stopResend(call);
        if (reply == null) {
            if (call.wireTracked && Extensions.unwrapCompletion(error) instanceof TimeoutException) {
                // The server may still run a sent call, so the deadline cancels it as cancel() does.
                sendCancel(call.msgId);
            }
        } else if (call.wireTracked) {
            pendingAcks.add(call.msgId);
        }
        deliver(call, reply, error);
    }


    // Runs when the transport settles the attempt of the call. The decision is taken on the executor,
    // and a closed executor completes the call directly.
    private void settleAttempt(RpcCall call, Protocol.Reply reply, Throwable error) {
        try {
            client.getExecutor().submit(() -> finishAttempt(call, reply, error));
        } catch (RejectedExecutionException e) {
            completeCall(call, reply, error);
        }
    }


    // Decides on the answer of an attempt. A refusal which carries the retry signal is sent again
    // and settles nothing; any other answer settles the call. An attempt which the transport lost
    // first is ignored: the call was kept for a resend or failed with the transport.
    private void finishAttempt(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (call.future.isDone() || onWire.get(call.commandId) != call) {
            return;
        }
        client.removeFuture(call.commandId);
        onWire.remove(call.commandId);
        if (reply != null && reply.getError().getCode() != 0 && call.wireTracked && resend(call, reply.getError())) {
            return;
        }
        completeCall(call, reply, error);
    }


    private static void completeCall(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (reply != null) {
            call.future.complete(reply);
        } else {
            call.future.completeExceptionally(error);
        }
    }


    // Hands the outcome of the call to its callback. Reads no session state, so it is safe once the
    // session is gone with the executor.
    private void deliver(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (reply == null) {
            call.callback.onDone(Extensions.unwrapCompletion(error), null);
            return;
        }
        if (reply.getError().getCode() != 0) {
            call.callback.onDone(client.getReplyError(reply), null);
            return;
        }
        Protocol.RPCResult rpcResult = reply.getRpc();
        RPCResult result = new RPCResult();
        result.setData(rpcResult.getData().toByteArray());
        result.setCached(rpcResult.getCached());
        call.callback.onDone(null, result);
    }
}
