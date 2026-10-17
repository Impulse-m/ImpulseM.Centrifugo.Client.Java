package io.github.centrifugal.centrifuge;


/**
 * The calls of one endpoint key which wait for a connection with room, in order. The list runs through the calls
 * themselves, so a call leaves the queue in constant time, whatever its place in it. Used on the pool's thread only.
 */
final class RpcPoolQueue {
    private RpcPoolCall first;
    private RpcPoolCall last;
    private int size;


    boolean isEmpty() {
        return size == 0;
    }


    RpcPoolCall first() {
        return first;
    }


    void addLast(RpcPoolCall call) {
        call.queue = this;
        call.previous = last;
        call.next = null;
        if (last != null) {
            last.next = call;
        } else {
            first = call;
        }
        last = call;
        size++;
    }


    void addFirst(RpcPoolCall call) {
        call.queue = this;
        call.previous = null;
        call.next = first;
        if (first != null) {
            first.previous = call;
        } else {
            last = call;
        }
        first = call;
        size++;
    }


    // Unlinks the call when it is in this queue; a call of another queue is left alone.
    void remove(RpcPoolCall call) {
        if (call.queue != this) {
            return;
        }
        if (call.previous != null) {
            call.previous.next = call.next;
        } else {
            first = call.next;
        }
        if (call.next != null) {
            call.next.previous = call.previous;
        } else {
            last = call.previous;
        }
        call.queue = null;
        call.previous = null;
        call.next = null;
        size--;
    }
}
