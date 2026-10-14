package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Exactly-once rpc calls over the fork protocol, checked against the in-process fake server.
 * The fake server keeps the record a dedup store would: a call runs once per msg_id, and a
 * resend gets the stored answer.
 */
public class RpcExactlyOnceTest extends FakeServerTestBase {
    // msg_id -> payload of the calls the fake server ran.
    private final Map<Long, byte[]> records = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    // While set, the fake server holds the answers to calls, so they are lost with the transport.
    private final AtomicBoolean holdCalls = new AtomicBoolean(false);
    // While set, the fake server holds the connect reply.
    private final AtomicBoolean holdConnect = new AtomicBoolean(false);
    private final Semaphore callArrivals = new Semaphore(0);
    private final Semaphore connectArrivals = new Semaphore(0);


    @Test
    public void droppedCallRunsOnceAndTheResendGetsTheSameReply() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("pay", EMPTY_PAYLOAD, into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));

        holdCalls.set(false);
        server.closeConnection();

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("{}", new String(result.getData(), StandardCharsets.UTF_8));
        assertTrue("answered from the record", result.isCached());
        assertEquals(1, executions.get());
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertEquals(requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertFalse(requests.get(0).getRetransmit());
        assertTrue(requests.get(1).getRetransmit());
    }


    @Test
    public void lostCallsAreSentAgainInMsgIdOrder() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            futures.add(future);
            c.rpcCall("step" + i, EMPTY_PAYLOAD, into(future));
        }
        for (int i = 0; i < 3; i++) {
            assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        }

        holdCalls.set(false);
        server.closeConnection();
        for (CompletableFuture<RPCResult> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(6, requests.size());
        List<Long> firstAttempts = new ArrayList<>();
        List<Long> resends = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            firstAttempts.add(requests.get(i).getMsgId());
            assertFalse(requests.get(i).getRetransmit());
            resends.add(requests.get(i + 3).getMsgId());
            assertTrue(requests.get(i + 3).getRetransmit());
        }
        Collections.sort(firstAttempts);
        assertEquals(firstAttempts, resends);
    }


    @Test
    public void callsBeyondTheQueueBoundFailWithLimitExceeded() throws Exception {
        Options options = new Options();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        answerRpcs("ok");
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        CompletableFuture<RPCResult> overflow = new CompletableFuture<>();
        c.rpcCall("a", EMPTY_PAYLOAD, into(first));
        c.rpcCall("b", EMPTY_PAYLOAD, into(second));
        c.rpcCall("c", EMPTY_PAYLOAD, into(overflow));

        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(overflow).getCode());
        connect(c);
        assertEquals("ok", new String(first.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("ok", new String(second.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void repliesAreAckedWithTheNextCall() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        c.rpcCall("a", EMPTY_PAYLOAD, into(first));
        first.get(5, TimeUnit.SECONDS);
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        c.rpcCall("b", EMPTY_PAYLOAD, into(second));
        second.get(5, TimeUnit.SECONDS);

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(0, requests.get(0).getAckCount());
        assertEquals(Collections.singletonList(requests.get(0).getMsgId()), requests.get(1).getAckList());
    }


    @Test
    public void deadlineRejectsACallLostWhileDisconnected() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        holdConnect.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(300).build(), into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));

        server.closeConnection();

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("expected the deadline to reject the call");
        } catch (ExecutionException e) {
            assertTrue("timeout", e.getCause() instanceof TimeoutException);
        }
        assertEquals(1, sentRpcRequests().size());
    }


    @Test
    public void cancelBeforeSendNeverSendsTheCall() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        CompletableFuture<RPCResult> cancelled = new CompletableFuture<>();
        RpcCall call = c.rpcCall("never", EMPTY_PAYLOAD, into(cancelled));

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(cancelled).getCode());
        connect(c);
        CompletableFuture<RPCResult> marker = new CompletableFuture<>();
        c.rpcCall("marker", EMPTY_PAYLOAD, into(marker));
        marker.get(5, TimeUnit.SECONDS);
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertFalse("the cancelled call is not sent", "never".equals(request.getMethod()));
        }
    }


    @Test
    public void cancelAfterSendStopsTheCallOnTheServer() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("long", EMPTY_PAYLOAD, into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        long msgId = sentRpcRequests().get(0).getMsgId();

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
        Protocol.RPCRequest cancel = awaitCancel(msgId);
        assertEquals("", cancel.getMethod());
        assertEquals(0, cancel.getData().size());
    }


    @Test
    public void cancelAfterCompletionHasNoEffect() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("quick", EMPTY_PAYLOAD, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));

        call.cancel();

        CompletableFuture<RPCResult> marker = new CompletableFuture<>();
        c.rpcCall("marker", EMPTY_PAYLOAD, into(marker));
        marker.get(5, TimeUnit.SECONDS);
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertEquals("no cancel is sent", 0, request.getCancel());
        }
    }


    @Test
    public void cancelWhileDisconnectedIsSentWithTheNextConnect() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        connectArrivals.drainPermits();
        holdCalls.set(true);
        holdConnect.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("long", EMPTY_PAYLOAD, into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        long msgId = sentRpcRequests().get(0).getMsgId();
        server.closeConnection();
        assertTrue("reconnect started", connectArrivals.tryAcquire(5, TimeUnit.SECONDS));

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
        holdConnect.set(false);
        server.releaseDeferredCommands();
        assertTrue("reconnected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        Protocol.RPCRequest cancel = awaitCancel(msgId);
        assertEquals(msgId, cancel.getCancel());
    }


    @Test
    public void legacyServerGetsPlainCallsAndRejectsHeaders() throws Exception {
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> headed = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, headerOptions(), into(headed));
        assertEquals(ReplyError.UNSUPPORTED, replyErrorOf(headed).getCode());

        CompletableFuture<RPCResult> plain = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(plain));
        plain.get(5, TimeUnit.SECONDS);

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(1, requests.size());
        assertEquals(0, requests.get(0).getMsgId());
        assertEquals(0, requests.get(0).getAckCount());
        assertFalse(requests.get(0).getRetransmit());
    }


    @Test
    public void legacyServerRejectsCancelOfASentCall() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);

        call.cancel();

        assertEquals(ReplyError.UNSUPPORTED, replyErrorOf(future).getCode());
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertEquals("no cancel is sent", 0, request.getCancel());
        }
    }


    @Test
    public void legacyCallLostWithItsTransportFails() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);

        server.closeConnection();

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("expected the call to fail with its transport");
        } catch (ExecutionException e) {
            assertTrue("transport closed", e.getCause() instanceof IOException);
        }
        assertEquals(1, sentRpcRequests().size());
    }


    @Test
    public void msgIdsStrictlyIncreaseAndFitFiftyThreeBits() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        for (int i = 0; i < 50; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            c.rpcCall("m", EMPTY_PAYLOAD, into(future));
            future.get(5, TimeUnit.SECONDS);
        }

        long nowSeconds = System.currentTimeMillis() / 1000L;
        long previous = 0;
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(50, requests.size());
        for (Protocol.RPCRequest request : requests) {
            long msgId = request.getMsgId();
            assertTrue("msg_id increases", msgId > previous);
            assertTrue("msg_id fits 53 bits", msgId <= Extensions.MSG_ID_MAX);
            assertTrue("time part is the unix second", Math.abs((msgId >> Extensions.MSG_ID_SEQUENCE_BITS) - nowSeconds) <= 5);
            previous = msgId;
        }
    }


    @Test
    public void configuredInstanceIsSentOnConnect() throws Exception {
        Options options = new Options();
        options.setInstance("abcdefghijklmnopqrstuv");
        Client c = newClient(options);
        connect(c);

        assertEquals("abcdefghijklmnopqrstuv", connectRequest().getInstance());
    }


    @Test
    public void unsetInstanceIsNotSentOnConnect() throws Exception {
        Client c = newClient();
        connect(c);

        assertEquals("", connectRequest().getInstance());
    }


    // The fake server answers like a server with a dedup store: a msg_id runs once, and a
    // msg_id it has seen answers from the record with cached set. Calls are held while
    // holdCalls is set, and connects while holdConnect is set.
    private void serveDedup() {
        server.onCommand = cmd -> {
            if (cmd.hasConnect()) {
                connectArrivals.release();
                return null;
            }
            if (!cmd.hasRpc() || cmd.getRpc().getCancel() != 0) {
                return null;
            }
            long msgId = cmd.getRpc().getMsgId();
            boolean cached = records.containsKey(msgId);
            if (!cached) {
                executions.incrementAndGet();
                records.put(msgId, cmd.getRpc().getData().toByteArray());
            }
            // The hold is read before the arrival is signalled, so a test which clears it
            // after the arrival does not change how this call is answered.
            boolean hold = holdCalls.get();
            callArrivals.release();
            if (hold) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRpc(Protocol.RPCResult.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFrom(records.get(msgId)))
                            .setCached(cached))
                    .build();
        };
        // onCommand answers every call which is not held, so a call reaching here is held.
        server.deferCommand = cmd -> {
            if (cmd.hasConnect()) {
                return holdConnect.get();
            }
            return cmd.hasRpc() && cmd.getRpc().getCancel() == 0;
        };
    }


    private Protocol.ConnectRequest connectRequest() {
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                return cmd.getConnect();
            }
        }
        throw new AssertionError("no connect request was sent");
    }


    private Protocol.RPCRequest awaitCancel(long msgId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            for (Protocol.RPCRequest request : sentRpcRequests()) {
                if (request.getCancel() == msgId) {
                    return request;
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no cancel was sent for msg_id " + msgId);
    }


    private void awaitRequestCount(int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (sentRpcRequests().size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(count, sentRpcRequests().size());
    }


    private static RpcOptions headerOptions() {
        return new RpcOptions.Builder().header("x-request-source", "test").build();
    }
}
