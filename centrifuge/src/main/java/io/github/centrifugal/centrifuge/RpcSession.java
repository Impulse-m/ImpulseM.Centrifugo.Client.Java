package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
    // A cancel command on the wire: the msg_id it cancels, and the acks it carries, which go back to
    // the pending acks when the command is lost with its transport.
    private static final class CancelCommand {
        final long msgId;
        final List<Long> acks;
        // Forgets the cancel when its reply is late. Stopped when the reply comes or the transport is lost.
        ScheduledFuture<?> timer;


        CancelCommand(long msgId, List<Long> acks) {
            this.msgId = msgId;
            this.acks = acks;
        }


        void stopTimer() {
            if (timer != null) {
                timer.cancel(false);
                timer = null;
            }
        }
    }


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
    // options could carry the cancel, and of cancels which got no reply before their transport was lost.
    // Capped at maxPending, see keepCancel. A connection which confirms RPC options takes the whole list.
    private final List<Long> pendingCancels = new ArrayList<>();
    // The cancel commands which wait for their reply, by the id of the command: the msg_id each one cancels.
    private final Map<Integer, CancelCommand> cancelCommands = new TreeMap<>();
    private final MsgIdGenerator msgIds = new MsgIdGenerator();
    // The bound of the calls which are not settled, see submit.
    private final int maxPending;
    // The slots of the bound taken by the calls which are not settled: taken at submit, given back at delivery.
    private final AtomicInteger pending = new AtomicInteger();


    RpcSession(Client client, int maxPending) {
        this.client = client;
        this.queue = new RpcQueue();
        this.maxPending = maxPending;
    }


    /**
     * Starts the deadline of the call and hands the call to the executor. Called on the caller's
     * thread. The deadline starts here, so it holds for a call waiting for a refresh, and across
     * resends. The callback runs on the executor, also when the call was cancelled before it started.
     * A client with an instance refuses a call beyond the bound at once, with LIMIT_EXCEEDED: the call
     * is not registered and the bound counts every call which is not settled.
     */
    void submit(RpcCall call) {
        if (!reserve(call)) {
            // Refused at once. The call settles on the caller's thread, and the callback runs through the
            // same path as the other calls, so the future of the call is never an orphan.
            call.future.completeExceptionally(new ReplyError(ReplyError.LIMIT_EXCEEDED, "too many rpc calls are pending", false));
        } else {
            try {
                // The deadline is a timer of the client, so the clock of the client drives it. The scheduler and the
                // executor reject the call once the client is closed. Nothing is registered then, so the deadline
                // cannot fire later and call back on another thread.
                call.deadlineTask = client.getScheduler().schedule(() -> call.future.completeExceptionally(new TimeoutException()), call.timeout, TimeUnit.MILLISECONDS);
                client.getExecutor().submit(() -> start(call));
            } catch (RejectedExecutionException e) {
                cancelDeadline(call);
                release(call);
                throw e;
            }
        }
        call.future.whenComplete((Protocol.Reply reply, Throwable error) -> finishOnExecutor(call, reply, error));
    }


    /**
     * Cancels the call. A call which was never sent is never sent, and is rejected with CANCELLED
     * whatever the connection state: nothing reached the server. A sent call of a client without an
     * instance is rejected with UNSUPPORTED whatever the connection state: its calls are plain, so the
     * server has no record of them. A sent call on a connected server which does not support RPC options
     * is rejected with UNSUPPORTED too, since the server cannot cancel anything. While the capability is
     * not known, a sent tracked call is rejected with CANCELLED at once, and its cancel is kept for the next
     * connection which confirms RPC options. A call whose current msg_id was never written, a new msg_id still
     * queued after a refusal, is rejected with CANCELLED and nothing is sent for that id.
     */
    void cancel(RpcCall call) {
        if (call.future.isDone()) {
            return;
        }
        stopResend(call);
        if (!call.everSent) {
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.CANCELLED, "rpc call cancelled", false));
            return;
        }
        if (!client.hasInstance()) {
            // Without an instance the calls are plain: the server has no record of them to cancel.
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel needs the instance option", false));
            return;
        }
        if (client.getState() == ClientState.CONNECTED && !client.hasRpcOptions()) {
            queue.remove(call);
            call.future.completeExceptionally(new ReplyError(ReplyError.UNSUPPORTED, "rpc cancel is not supported by the server", false));
            return;
        }
        queue.remove(call);
        // A call whose current msg_id was never written (a new msg_id still queued) has nothing to cancel on the server.
        if (call.wireTracked) {
            sendCancel(call.msgId);
        }
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
                keepCancel(msgId);
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
            // A call which fails to write fails alone: the calls behind it are still written, and the
            // connect path which flushes them goes on.
            try {
                write(call);
            } catch (RuntimeException e) {
                call.future.completeExceptionally(e);
            }
        }
    }


    /**
     * The client is closed: the futures still pending fail, and so do the calls which wait for a
     * connection, as a transport loss without a reconnect does.
     */
    void failOnClose(Collection<CompletableFuture<Protocol.Reply>> pendingFutures) {
        for (CompletableFuture<Protocol.Reply> f : pendingFutures) {
            f.completeExceptionally(new IOException());
        }
        onTransportLost(false);
    }


    /**
     * The transport is gone. Calls with a msg_id are kept for a resend when the client reconnects,
     * the others fail. Without a reconnect every call fails. A kept call is never failed for the
     * queue bound, since the server may have run it: the queue may exceed the bound, and new calls
     * wait until it drains below the bound. The acks carried by a command which was lost with the
     * transport go back to the pending acks: the server may not have seen them, and a repeated ack
     * is harmless. A call which settled locally and waits for a late reply has no reply to wait for
     * any more: its wait ends here.
     */
    void onTransportLost(boolean keep) {
        List<RpcCall> lost = new ArrayList<>();
        for (Map.Entry<Integer, RpcCall> entry : onWire.entrySet()) {
            RpcCall call = entry.getValue();
            client.removeFuture(entry.getKey());
            stopLate(call);
            pendingAcks.addAll(0, call.carriedAcks);
            call.carriedAcks = Collections.emptyList();
            if (call.future.isDone()) {
                continue;
            }
            if (keep && call.wireTracked) {
                lost.add(call);
            } else {
                call.future.completeExceptionally(new IOException());
            }
        }
        onWire.clear();
        // A cancel which got no reply is kept for the next connection which confirms RPC options.
        for (Map.Entry<Integer, CancelCommand> entry : cancelCommands.entrySet()) {
            client.removeFuture(entry.getKey());
            entry.getValue().stopTimer();
            keepCancel(entry.getValue().msgId);
            pendingAcks.addAll(0, entry.getValue().acks);
        }
        cancelCommands.clear();
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


    // Takes a slot of the bound for the call. A client with an instance refuses the call when every slot is
    // taken, and a refused call takes none: only the accepted calls hold slots. A client without an instance
    // has no bound.
    private boolean reserve(RpcCall call) {
        if (!client.hasInstance()) {
            return true;
        }
        int current = pending.get();
        while (current < maxPending) {
            if (pending.compareAndSet(current, current + 1)) {
                call.holdsSlot = true;
                return true;
            }
            current = pending.get();
        }
        return false;
    }


    // Gives back the slot the call holds, if it holds one.
    private void release(RpcCall call) {
        if (call.holdsSlot) {
            call.holdsSlot = false;
            pending.decrementAndGet();
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
    // waits in the queue.
    private void start(RpcCall call) {
        if (call.future.isDone()) {
            return;
        }
        // A call submitted before close but started after it: nothing will ever write it, so it fails now.
        if (client.getState() == ClientState.CLOSED) {
            call.future.completeExceptionally(new IOException());
            return;
        }
        writeOrQueue(call);
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
            call.future.complete(Extensions.unsupportedRpcReply());
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
        call.attempt = attempt;
        client.addFuture(id, attempt);
        onWire.put(id, call);
        if (client.sendCommand(cmd)) {
            call.everSent = true;
            call.resendAsNew = false;
            call.wireTracked = tracked;
            call.carriedAcks = acks;
            return;
        }
        client.removeFuture(id);
        onWire.remove(id);
        pendingAcks.addAll(0, acks);
        if (!tracked) {
            call.future.completeExceptionally(new IOException());
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
            keepCancel(msgId);
        }
    }


    // Keeps a cancel for the next connection which confirms RPC options. The list is capped at the
    // bound: when it is full, the oldest cancel is dropped, since the server's window expires its record anyway.
    private void keepCancel(long msgId) {
        if (pendingCancels.size() >= maxPending) {
            pendingCancels.remove(0);
        }
        pendingCancels.add(msgId);
    }


    // Sends a cancel for a msg_id: an rpc request without method and data. Its reply is not awaited for
    // the call, only for the cancel command, which the client timeout bounds: a cancel without a reply in
    // time is forgotten, and not sent again. Returns false when the command was not sent, and its acks
    // wait for the next command.
    private boolean writeCancel(long msgId) {
        List<Long> acks = takeAcks();
        Protocol.RPCRequest req = Protocol.RPCRequest.newBuilder()
                .setCancel(msgId)
                .addAllAck(acks)
                .build();
        int id = client.nextCommandId();
        Protocol.Command cmd = Protocol.Command.newBuilder()
                .setId(id)
                .setRpc(req)
                .build();
        CompletableFuture<Protocol.Reply> reply = new CompletableFuture<>();
        reply.whenComplete((Protocol.Reply answer, Throwable error) -> forgetCancelOnExecutor(id));
        client.addFuture(id, reply);
        CancelCommand command = new CancelCommand(msgId, acks);
        cancelCommands.put(id, command);
        if (client.sendCommand(cmd)) {
            // The reply is handled on the executor, after this task, so the timer is set before it can come.
            command.timer = client.getScheduler().schedule(() -> forgetCancelOnExecutor(id), client.getOpts().getTimeout(), TimeUnit.MILLISECONDS);
            return true;
        }
        client.removeFuture(id);
        cancelCommands.remove(id);
        pendingAcks.addAll(0, acks);
        return false;
    }


    // Ends the wait for the reply of a cancel command, from its reply or its timeout. Runs on the
    // executor. A closed executor has forgotten the cancel with the connection.
    private void forgetCancelOnExecutor(int id) {
        Extensions.submitOrIgnore(client.getExecutor(), () -> forgetCancel(id));
    }


    private void forgetCancel(int id) {
        client.removeFuture(id);
        CancelCommand command = cancelCommands.remove(id);
        if (command != null) {
            command.stopTimer();
        }
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
            long delayNanos = LATER_BASE_NANOS << call.laterResends;
            // A resend which would run at or past the deadline is not scheduled: the deadline is not a
            // time to retransmit, and the error is delivered as it is.
            if (delayNanos >= remainingNanos) {
                return false;
            }
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
        // The new msg_id is not on the wire yet: until it is, a timeout sends no cancel for it.
        call.wireTracked = false;
        writeOrQueue(call);
    }


    // Sends the refused call again with the same msg_id once the delay is over, on the executor.
    private void scheduleResend(RpcCall call, long delayNanos) {
        resendWaiting.add(call);
        call.resendGeneration++;
        final int generation = call.resendGeneration;
        call.resendTask = client.getScheduler().schedule(() -> Extensions.submitOrIgnore(client.getExecutor(), () -> resendLater(call, generation)), delayNanos, TimeUnit.NANOSECONDS);
    }


    // The delay of a resend is over. The call goes out as a retransmit, unless it settled meanwhile, its
    // deadline passed, or a resend scheduled after this one replaced it: the deadline then delivers the call.
    private void resendLater(RpcCall call, int generation) {
        if (generation != call.resendGeneration || !resendWaiting.contains(call) || call.future.isDone() || call.remainingNanos() <= 0L) {
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
        queue.remove(call);
        stopResend(call);
        cancelDeadline(call);
        if (reply == null && call.wireTracked && onWire.get(call.commandId) == call) {
            // Settled locally while its attempt is on the wire: a late reply is still acked for one call timeout.
            keepLate(call);
        } else {
            forgetAttempt(call);
        }
        if (reply == null) {
            if (call.wireTracked && !call.answered && Extensions.unwrapCompletion(error) instanceof TimeoutException && !answerQueued(call)) {
                // The server may still run a sent call, so the deadline cancels it as cancel() does.
                sendCancel(call.msgId);
            }
        } else if (call.wireTracked) {
            pendingAcks.add(call.msgId);
        }
        deliver(call, reply, error);
    }


    // Whether the server answered the last attempt of the call, and the answer waits for the executor. The
    // server ran or refused the call then, so a deadline sends no cancel for it.
    private static boolean answerQueued(RpcCall call) {
        return call.attempt != null && call.attempt.isDone() && !call.attempt.isCompletedExceptionally();
    }


    // Forgets the attempt of the call: its future and its entry on the wire.
    private void forgetAttempt(RpcCall call) {
        if (call.commandId != 0) {
            client.removeFuture(call.commandId);
            onWire.remove(call.commandId);
        }
    }


    // Keeps the attempt of a call which settled locally, so that a late reply is acked. The attempt is
    // forgotten one call timeout later, or with the transport.
    private void keepLate(RpcCall call) {
        call.lateTask = client.getScheduler().schedule(() -> Extensions.submitOrIgnore(client.getExecutor(), () -> forgetLate(call)), call.timeout, TimeUnit.MILLISECONDS);
    }


    private void forgetLate(RpcCall call) {
        call.lateTask = null;
        forgetAttempt(call);
    }


    // Cancels the deadline timer of the call, if it is still set.
    private static void cancelDeadline(RpcCall call) {
        if (call.deadlineTask != null) {
            call.deadlineTask.cancel(false);
            call.deadlineTask = null;
        }
    }


    // Cancels the forgetting of a late attempt, if one is kept.
    private static void stopLate(RpcCall call) {
        if (call.lateTask != null) {
            call.lateTask.cancel(false);
            call.lateTask = null;
        }
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
    // first is ignored: the call was kept for a resend or failed with the transport. The answer to
    // a call which settled locally is a late reply: it is acked, and its attempt is forgotten.
    private void finishAttempt(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (onWire.get(call.commandId) != call) {
            return;
        }
        if (call.future.isDone()) {
            if (reply != null && call.wireTracked) {
                pendingAcks.add(call.msgId);
            }
            stopLate(call);
            forgetAttempt(call);
            return;
        }
        client.removeFuture(call.commandId);
        onWire.remove(call.commandId);
        call.attempt = null;
        if (reply != null && reply.getError().getCode() != 0 && call.wireTracked && resend(call, reply.getError())) {
            return;
        }
        call.answered = reply != null;
        if (!completeCall(call, reply, error) && reply != null && call.wireTracked) {
            // The deadline settled the call before this reply came to the executor: the reply is acked as a late one.
            pendingAcks.add(call.msgId);
        }
    }


    // Settles the call with the reply or the error. Returns false when the call was already done, the deadline first.
    private static boolean completeCall(RpcCall call, Protocol.Reply reply, Throwable error) {
        if (reply != null) {
            return call.future.complete(reply);
        }
        return call.future.completeExceptionally(error);
    }


    // Hands the outcome of the call to its callback. Reads no session state, so it is safe once the
    // session is gone with the executor.
    private void deliver(RpcCall call, Protocol.Reply reply, Throwable error) {
        release(call);
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
