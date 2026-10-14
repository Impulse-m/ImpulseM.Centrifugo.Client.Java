package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletionException;


/**
 * The exactly-once rpc calls of one client. A call waits in the queue until it is written to a
 * connection which confirmed RPC options. A call with a msg_id which is lost with its transport
 * stays in the queue and is sent again, with retransmit set, after the next connect. Replies are
 * acknowledged with the next rpc request, and cancels go out as rpc requests of their own.
 * Used on the client's executor thread only, except {@link #submit}.
 */
final class RpcSession {
    private final Client client;
    // Calls which wait for the connection or for a refresh, and calls lost with their transport.
    private final RpcQueue queue;
    // Calls written to the current transport, by the id of their command.
    private final Map<Integer, RpcCall> onWire = new HashMap<>();
    // msg_ids of the replies received, sent as the ack of the next rpc request.
    private final List<Long> pendingAcks = new ArrayList<>();
    // msg_ids of calls cancelled while no connection could carry the cancel. Not counted against
    // the queue bound: a cancel needs no resend of its call.
    private final List<Long> pendingCancels = new ArrayList<>();
    private long lastMsgId = 0;


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
        call.future.orTimeout(call.timeout, TimeUnit.MILLISECONDS)
                .whenComplete((Protocol.Reply reply, Throwable error) -> runOnExecutor(() -> finish(call, reply, error)));
        client.getExecutor().submit(() -> start(call));
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
        if (client.getState() == ClientState.CONNECTED && !client.hasRpcOptions()) {
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel is not supported by the server", false));
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


    // Runs the task on the executor. Once the client is closed the executor rejects tasks, and the
    // task then runs on the calling thread, so the callback of the call still runs exactly once.
    private void runOnExecutor(Runnable task) {
        try {
            client.getExecutor().submit(task);
        } catch (RejectedExecutionException e) {
            task.run();
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


    // Writes the call on the current transport. A call with headers on a connection without RPC
    // options fails with the reply error the server would give. A call which was not sent takes a
    // new msg_id on each attempt, since nothing reached the server. A call which was sent keeps
    // its msg_id, and is marked as a resend.
    private void write(RpcCall call) {
        boolean supported = client.hasRpcOptions();
        if (!supported && !call.headers.isEmpty()) {
            call.future.complete(Extensions.unsupportedRpcReply(0));
            return;
        }
        if (supported && !call.everSent) {
            long msgId = Extensions.nextMsgId(client.serverTimeMillis(), lastMsgId);
            if (msgId == Extensions.NO_MSG_ID) {
                call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls in one second", false));
                return;
            }
            lastMsgId = msgId;
            call.msgId = msgId;
        }
        int id = client.nextCommandId();
        call.commandId = id;
        List<Long> acks = supported ? takeAcks() : Collections.<Long>emptyList();
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(id)
                .setRpc(buildRpcRequest(call, supported, acks))
                .build();
        client.addFuture(id, call.future);
        onWire.put(id, call);
        if (client.sendCommand(cmd)) {
            call.everSent = true;
            call.wireTracked = supported;
            return;
        }
        client.removeFuture(id);
        onWire.remove(id);
        pendingAcks.addAll(0, acks);
        if (!supported) {
            call.future.completeExceptionally(new IOException());
        } else if (!call.everSent && queue.isFull()) {
            call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls wait for the connection", false));
        } else {
            queue.add(call);
        }
    }


    private static Protocol.RPCRequest buildRpcRequest(RpcCall call, boolean supported, List<Long> acks) {
        Protocol.RPCRequest.Builder req = Protocol.RPCRequest.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(call.data))
                .setMethod(call.method)
                .putAllHeaders(call.headers);
        if (supported) {
            req.setMsgId(call.msgId)
                    .setRetransmit(call.everSent)
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


    private void finish(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (call.commandId != 0) {
            client.removeFuture(call.commandId);
            onWire.remove(call.commandId);
        }
        queue.remove(call);
        if (reply == null) {
            Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
            if (call.wireTracked && cause instanceof TimeoutException) {
                // The server may still run a sent call, so the deadline cancels it as cancel() does.
                sendCancel(call.msgId);
            }
            call.callback.onDone(cause, null);
            return;
        }
        if (call.wireTracked) {
            pendingAcks.add(call.msgId);
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
