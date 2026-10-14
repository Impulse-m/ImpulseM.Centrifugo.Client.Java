package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class RpcQueueTest {
    @Test
    public void addKeepsEveryCall() {
        RpcQueue queue = new RpcQueue();

        queue.add(call(1L));
        queue.add(call(2L));

        assertEquals(2, queue.drain().size());
    }


    @Test
    public void drainReturnsTheCallsInMsgIdOrderAndEmptiesTheQueue() {
        RpcQueue queue = new RpcQueue();
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
    public void callsWithoutMsgIdFollowTheCallsWithOne() {
        RpcQueue queue = new RpcQueue();
        RpcCall unsent = call(0L);
        RpcCall sent = call(5L);
        queue.add(unsent);
        queue.add(sent);

        List<RpcCall> drained = queue.drain();

        assertSame(sent, drained.get(0));
        assertSame(unsent, drained.get(1));
    }


    @Test
    public void removeOnlyRemovesTheSameCall() {
        RpcQueue queue = new RpcQueue();
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
