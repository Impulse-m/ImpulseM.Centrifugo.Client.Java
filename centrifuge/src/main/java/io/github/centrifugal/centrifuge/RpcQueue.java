package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


/**
 * The rpc calls that wait for the connection: calls issued while it is down or a refresh is in
 * flight, and calls lost with their transport. The capacity bounds the calls issued, see
 * {@link #isFull}; a call which was sent already is kept whatever the size. Used on the client's
 * executor thread only.
 */
final class RpcQueue {
    private final int capacity;
    // Identity semantics: two calls are the same only when they are the same object.
    private final Set<RpcCall> calls = new LinkedHashSet<>();


    RpcQueue(int capacity) {
        this.capacity = capacity;
    }


    /** Whether the queue holds as many calls as its capacity, so a call issued now is refused. */
    boolean isFull() {
        return calls.size() >= capacity;
    }


    /** Adds the call. The capacity does not apply, so a call lost with its transport is kept. */
    void add(RpcCall call) {
        calls.add(call);
    }


    void remove(RpcCall call) {
        calls.remove(call);
    }


    /**
     * Removes every call and returns them in resend order: the calls with a msg_id first, in
     * msg_id order, then the calls which were never sent, in the order they were added.
     */
    List<RpcCall> drain() {
        List<RpcCall> drained = new ArrayList<>(calls);
        calls.clear();
        Collections.sort(drained, (RpcCall first, RpcCall second) -> Long.compare(resendKey(first), resendKey(second)));
        return drained;
    }


    // A call without a msg_id sorts after every call with one. Collections.sort is stable, so
    // calls with equal keys keep the order they were added in.
    private static long resendKey(RpcCall call) {
        return call.msgId != 0 ? call.msgId : Long.MAX_VALUE;
    }
}
