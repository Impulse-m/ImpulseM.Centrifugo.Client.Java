package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class RpcQueueTest {
    @Test
    public void isFullWhenTheCapacityIsReached() {
        RpcQueue queue = new RpcQueue(2);

        assertFalse(queue.isFull());
        queue.add(call(1L));
        assertFalse(queue.isFull());
        queue.add(call(2L));
        assertTrue(queue.isFull());
    }


    @Test
    public void addKeepsCallsBeyondTheCapacity() {
        RpcQueue queue = new RpcQueue(1);

        queue.add(call(1L));
        queue.add(call(2L));

        assertTrue(queue.isFull());
        assertEquals(2, queue.drain().size());
    }


    @Test
    public void removingACallFreesARoomInTheQueue() {
        RpcQueue queue = new RpcQueue(1);
        RpcCall first = call(1L);
        queue.add(first);

        queue.remove(first);

        assertFalse(queue.isFull());
    }


    @Test
    public void drainReturnsTheCallsInMsgIdOrderAndEmptiesTheQueue() {
        RpcQueue queue = new RpcQueue(10);
        RpcCall third = call(30L);
        RpcCall first = call(10L);
        RpcCall second = call(20L);
        queue.add(third);
        queue.add(first);
        queue.add(second);

        List<RpcCall> drained = queue.drain();

        assertEquals(3, drained.size());
        assertSame(first, drained.get(0));
        assertSame(second, drained.get(1));
        assertSame(third, drained.get(2));
        assertTrue(queue.drain().isEmpty());
    }


    @Test
    public void removeOnlyRemovesTheSameCall() {
        RpcQueue queue = new RpcQueue(10);
        RpcCall kept = call(5L);
        queue.add(kept);

        queue.remove(call(5L));

        assertEquals(Collections.singletonList(kept), queue.drain());
    }


    private static RpcCall call(long msgId) {
        RpcCall call = new RpcCall(null, "m", new byte[0], Collections.<String, String>emptyMap(), 1000L, null);
        call.msgId = msgId;
        return call;
    }
}
