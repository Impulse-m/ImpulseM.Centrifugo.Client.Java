package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


/**
 * The rpc calls that wait for the connection: calls issued while it is down, calls lost
 * with their transport, and cancels still to send. Ordered by msg_id and bounded by the
 * capacity. Used on the client's executor thread only.
 */
final class RpcQueue {
    private final int capacity;
    private final TreeMap<Long, RpcCall> calls = new TreeMap<>();


    RpcQueue(int capacity) {
        this.capacity = capacity;
    }


    /** Adds the call unless the queue is full. Returns false when the call was not added. */
    boolean offer(RpcCall call) {
        if (calls.size() >= capacity) {
            return false;
        }
        calls.put(call.msgId, call);
        return true;
    }


    /** Adds a call that is already counted by the queue, without the bound. */
    void keep(RpcCall call) {
        calls.put(call.msgId, call);
    }


    void remove(RpcCall call) {
        calls.remove(call.msgId, call);
    }


    /** Removes every call and returns them in msg_id order. */
    List<RpcCall> drain() {
        List<RpcCall> drained = new ArrayList<>(calls.values());
        calls.clear();
        return drained;
    }
}
