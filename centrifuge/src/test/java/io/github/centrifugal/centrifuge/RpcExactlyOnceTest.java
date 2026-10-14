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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Exactly-once rpc calls over the fork protocol, checked against the in-process fake server.
 * The fake server keeps the record a dedup store would: a call runs once per msg_id, and a
 * resend gets the stored answer.
 */
public class RpcExactlyOnceTest extends RpcTestBase {
    // Server error for a msg_id outside its window. The code is server-configurable, the test
    // only pins that the client surfaces it as a reply error.
    private static final int MSG_ID_INVALID = 107;
    // Server error of a call the server refused for now, such as a store which is unavailable.
    private static final int TEMPORARY = 100;
    // Largest msg_id a JSON number carries exactly: 53 bits.
    private static final long MSG_ID_MAX = (1L << 53) - 1;

    // msg_id -> payload of the calls the fake server ran.
    private final Map<Long, byte[]> records = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    // When the fake server received each refused or answered call, by System.nanoTime.
    private final List<Long> rpcArrivalsNanos = Collections.synchronizedList(new ArrayList<Long>());
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
        Options options = instanceOptions();
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
    public void callQueuedWhileDisconnectedIsCompletedByClose() throws Exception {
        enableRpcOptions();
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        drainExecutor();

        c.close(1000);

        assertTrue("close fails the queued call", errorOf(future) instanceof IOException);
        assertTrue(sentRpcRequests().isEmpty());
    }


    @Test
    public void rpcAfterCloseThrowsAndItsCallbackNeverFires() throws Exception {
        Client c = newClient();
        c.close(1000);
        AtomicInteger callbacks = new AtomicInteger();

        try {
            c.rpcCall("late", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(100).build(), (Throwable error, RPCResult result) -> callbacks.incrementAndGet());
            fail("expected the closed client to reject the call");
        } catch (RejectedExecutionException expected) {
            // The call was never registered.
        }

        // Three times the deadline of the call: a registered deadline would have fired by now.
        Thread.sleep(300L);
        assertEquals(0, callbacks.get());
    }


    @Test
    public void cancelAfterCloseDoesNotThrow() throws Exception {
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        drainExecutor();
        c.close(1000);
        assertTrue("close fails the queued call", errorOf(future) instanceof IOException);

        call.cancel();
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
    public void deadlineOfASentCallCancelsItOnTheServer() throws Exception {
        enableRpcOptions();
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRpc();
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(200).build(), into(future));
        awaitRequestCount(1);
        long msgId = sentRpcRequests().get(0).getMsgId();

        assertTrue(errorOf(future) instanceof TimeoutException);
        assertEquals(msgId, awaitCancel(msgId).getCancel());
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
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRpc();
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
    public void legacyServerRejectsCancelOfAQueuedCallToo() throws Exception {
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = new CompletableFuture<>();
        c.refresh("token-2", into(refreshFuture));
        syncWithServer();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, into(future));

        call.cancel();

        assertEquals(ReplyError.UNSUPPORTED, replyErrorOf(future).getCode());
        server.releaseDeferredCommands();
        refreshFuture.get(5, TimeUnit.SECONDS);
        syncWithServer();
        assertEquals("the queued call is never sent", 0, sentRpcRequests().size());
    }


    @Test
    public void legacyCallLostWithItsTransportFails() throws Exception {
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRpc();
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
    public void capabilityWithoutInstanceSendsPlainCalls() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient(new Options());
        connect(c);

        callAndWait(c);
        callAndWait(c);

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        for (Protocol.RPCRequest request : requests) {
            assertEquals("no msg_id", 0, request.getMsgId());
            assertEquals("no ack", 0, request.getAckCount());
            assertFalse(request.getRetransmit());
        }
    }


    @Test
    public void capabilityWithoutInstanceStillSendsHeaders() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient(new Options());
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();

        c.rpcCall("m", EMPTY_PAYLOAD, headerOptions(), into(future));

        future.get(5, TimeUnit.SECONDS);
        assertEquals("test", sentRpcRequests().get(0).getHeadersMap().get("x-request-source"));
    }


    @Test
    public void lostCallWithoutInstanceFailsWithItsTransport() throws Exception {
        enableRpcOptions();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client c = newClient(new Options());
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);

        server.closeConnection();

        assertTrue("transport closed", errorOf(future) instanceof IOException);
        assertEquals("the call is not sent again", 1, sentRpcRequests().size());
    }


    @Test
    public void cancelWithoutInstanceIsRejectedAndSendsNothing() throws Exception {
        enableRpcOptions();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client c = newClient(new Options());
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);

        call.cancel();

        ReplyError error = replyErrorOf(future);
        assertEquals(ReplyError.UNSUPPORTED, error.getCode());
        assertEquals("rpc cancel needs the instance option", error.getMessage());
        syncWithServer();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertEquals("no cancel is sent", 0, request.getCancel());
        }
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
            assertTrue("msg_id fits 53 bits", msgId <= MSG_ID_MAX);
            assertTrue("time part is the unix second", Math.abs((msgId >> MsgIdGenerator.SEQUENCE_BITS) - nowSeconds) <= 5);
            previous = msgId;
        }
    }


    @Test
    public void configuredInstanceIsSentOnConnect() throws Exception {
        Options options = new Options();
        options.setInstance("abcdefghijklmnopqrstuv");
        Client c = newClient(options);
        connect(c);

        assertEquals("abcdefghijklmnopqrstuv", lastConnectRequest().getInstance());
    }


    @Test
    public void unsetInstanceIsNotSentOnConnect() throws Exception {
        Client c = newClient(new Options());
        connect(c);

        assertEquals("", lastConnectRequest().getInstance());
    }


    @Test
    public void msgIdUsesTheServerClockForACallQueuedBeforeConnect() throws Exception {
        enableRpcOptions();
        AtomicLong serverTime = new AtomicLong(System.currentTimeMillis() - 40_000L);
        answerWithServerTime(serverTime);
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("early", EMPTY_PAYLOAD, into(future));
        long serverSecond = serverTime.get() / 1000L;

        connect(c);

        future.get(5, TimeUnit.SECONDS);
        long msgId = sentRpcRequests().get(0).getMsgId();
        assertTrue("time part is the server second", Math.abs((msgId >> MsgIdGenerator.SEQUENCE_BITS) - serverSecond) <= 5);
    }


    @Test
    public void lostCallsBeyondTheBoundAreKeptAndResent() throws Exception {
        enableRpcOptions();
        serveDedup();
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        connect(c);
        holdCalls.set(true);
        List<CompletableFuture<RPCResult>> futures = sendHeldCalls(c, 3);

        holdCalls.set(false);
        server.closeConnection();

        for (CompletableFuture<RPCResult> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(6, requests.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(requests.get(i).getMsgId(), requests.get(i + 3).getMsgId());
            assertTrue(requests.get(i + 3).getRetransmit());
        }
        assertEquals("each call ran once", 3, executions.get());
    }


    @Test
    public void newCallsAreRefusedWhileLostCallsAreOverTheBound() throws Exception {
        enableRpcOptions();
        serveDedup();
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        connect(c);
        connectArrivals.drainPermits();
        holdCalls.set(true);
        holdConnect.set(true);
        List<CompletableFuture<RPCResult>> futures = sendHeldCalls(c, 3);
        server.closeConnection();
        assertTrue("reconnect started", connectArrivals.tryAcquire(5, TimeUnit.SECONDS));

        CompletableFuture<RPCResult> refused = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, into(refused));

        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());
        holdCalls.set(false);
        holdConnect.set(false);
        server.releaseDeferredCommands();
        for (CompletableFuture<RPCResult> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        assertEquals("each call ran once", 3, executions.get());
    }


    @Test
    public void cancelledLostCallsDoNotUseTheQueueBound() throws Exception {
        enableRpcOptions();
        serveDedup();
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        connect(c);
        connectArrivals.drainPermits();
        holdCalls.set(true);
        holdConnect.set(true);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        RpcCall firstCall = c.rpcCall("a", EMPTY_PAYLOAD, into(first));
        RpcCall secondCall = c.rpcCall("b", EMPTY_PAYLOAD, into(second));
        assertTrue("first call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        assertTrue("second call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        long firstMsgId = sentRpcRequests().get(0).getMsgId();
        long secondMsgId = sentRpcRequests().get(1).getMsgId();
        server.closeConnection();
        assertTrue("reconnect started", connectArrivals.tryAcquire(5, TimeUnit.SECONDS));

        firstCall.cancel();
        secondCall.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(first).getCode());
        assertEquals(ReplyError.CANCELLED, replyErrorOf(second).getCode());
        CompletableFuture<RPCResult> accepted = new CompletableFuture<>();
        c.rpcCall("c", EMPTY_PAYLOAD, into(accepted));
        holdConnect.set(false);
        holdCalls.set(false);
        server.releaseDeferredCommands();

        accepted.get(5, TimeUnit.SECONDS);
        assertEquals(firstMsgId, awaitCancel(firstMsgId).getCancel());
        assertEquals(secondMsgId, awaitCancel(secondMsgId).getCancel());
    }


    @Test
    public void cancelOfAQueuedCallWhileConnectingIsLocal() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasConnect();
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        c.connect();
        awaitCondition("connect sent", () -> server.received().stream().anyMatch(Protocol.Command::hasConnect));

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
        server.releaseDeferredCommands();
        assertTrue("connected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        callAndWait(c);
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertFalse("the cancelled call is never sent", "queued".equals(request.getMethod()));
        }
    }


    @Test
    public void msgIdRefusalIsResentWithANewMsgId() throws Exception {
        enableRpcOptions();
        AtomicLong serverTime = new AtomicLong(System.currentTimeMillis() - 120_000L);
        refuseCalls(1, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, serverTime);
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, into(future));

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertNotEquals("the resend has a new msg_id", requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertFalse("the new msg_id is not a retransmit", requests.get(1).getRetransmit());
        assertTrue("the resend has the corrected time part", timePart(requests.get(1)) < timePart(requests.get(0)));
    }


    @Test
    public void callAfterAMsgIdRefusalUsesTheCorrectedTimePart() throws Exception {
        enableRpcOptions();
        AtomicLong serverTime = new AtomicLong(System.currentTimeMillis() - 120_000L);
        refuseCalls(1, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, serverTime);
        Client c = newClient();
        connect(c);
        callAndWait(c);

        callAndWait(c);

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(3, requests.size());
        long correctedSecond = (System.currentTimeMillis() - 120_000L) / 1000L;
        assertTrue("the next call follows the corrected clock", Math.abs(timePart(requests.get(2)) - correctedSecond) <= 5);
        assertTrue("the next call has its own msg_id", requests.get(2).getMsgId() > requests.get(1).getMsgId());
    }


    @Test
    public void msgIdRefusedThreeTimesSurfacesTheErrorWithItsMetadata() throws Exception {
        enableRpcOptions();
        refuseCalls(3, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);

        assertEquals(MSG_ID_INVALID, error.getCode());
        assertEquals(Extensions.RPC_RETRY_NEW_MSG_ID, error.getMetadata().get(Extensions.RPC_RETRY_METADATA_KEY));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals("the call and two resends", 3, requests.size());
        for (Protocol.RPCRequest request : requests) {
            assertFalse(request.getRetransmit());
        }
    }


    @Test
    public void laterRefusalIsResentWithTheSameMsgIdAfterTheDelay() throws Exception {
        enableRpcOptions();
        refuseCalls(1, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(future));

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertEquals("the resend keeps the msg_id", requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertFalse(requests.get(0).getRetransmit());
        assertTrue("the resend is a retransmit", requests.get(1).getRetransmit());
        long delayNanos = rpcArrivalsNanos.get(1) - rpcArrivalsNanos.get(0);
        assertTrue("the resend waits 250 ms", delayNanos >= TimeUnit.MILLISECONDS.toNanos(250L));
    }


    @Test
    public void laterRefusedFourTimesSurfacesTheError() throws Exception {
        enableRpcOptions();
        refuseCalls(4, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);

        assertEquals(TEMPORARY, error.getCode());
        assertEquals(Extensions.RPC_RETRY_LATER, error.getMetadata().get(Extensions.RPC_RETRY_METADATA_KEY));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals("the call and three resends", 4, requests.size());
        for (Protocol.RPCRequest request : requests) {
            assertEquals(requests.get(0).getMsgId(), request.getMsgId());
        }
    }


    @Test
    public void msgIdRefusalWithoutTheKeyIsSurfacedAtOnce() throws Exception {
        enableRpcOptions();
        refuseCalls(1, MSG_ID_INVALID, null, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);

        assertEquals(MSG_ID_INVALID, error.getCode());
        syncWithServer();
        assertEquals("the call is rejected, not sent again", 1, sentRpcRequests().size());
    }


    @Test
    public void temporaryRefusalWithoutTheKeyIsSurfacedAtOnce() throws Exception {
        enableRpcOptions();
        refuseCalls(1, TEMPORARY, null, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);

        assertEquals(TEMPORARY, error.getCode());
        syncWithServer();
        assertEquals("the call is not sent again", 1, sentRpcRequests().size());
    }


    @Test
    public void laterDelayAcrossADroppedTransportIsResentOnReconnect() throws Exception {
        enableRpcOptions();
        refuseCalls(1, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);

        server.closeConnection();

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertEquals(requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertTrue(requests.get(1).getRetransmit());
    }


    @Test
    public void deadlineDuringAResendRejectsWithTimeoutAndCancels() throws Exception {
        enableRpcOptions();
        AtomicInteger seen = new AtomicInteger();
        AtomicLong serverTime = new AtomicLong(System.currentTimeMillis());
        // The first call is refused for now. Its resend is held, so the deadline comes while the call waits or is in flight.
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasRpc() || cmd.getRpc().getCancel() != 0) {
                return null;
            }
            if (seen.getAndIncrement() == 0) {
                return refusal(cmd, TEMPORARY, Extensions.RPC_RETRY_LATER, serverTime.get());
            }
            return null;
        };
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && cmd.getRpc().getCancel() == 0;
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(300).build(), into(future));
        awaitCondition("first attempt", () -> !sentRpcRequests().isEmpty());
        long msgId = sentRpcRequests().get(0).getMsgId();

        assertTrue(errorOf(future) instanceof TimeoutException);
        assertEquals(msgId, awaitCancel(msgId).getCancel());
    }


    // Answers connects and rpc calls with the server time the test holds, so the test moves the clock.
    private void answerWithServerTime(AtomicLong serverTime) {
        answerRpcs("ok", serverTime::get);
        Function<Protocol.Command, Protocol.Reply> rpcAnswer = server.onCommand;
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasConnect()) {
                return rpcAnswer.apply(cmd);
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setTime(serverTime.get())
                    .setConnect(server.connectResult)
                    .build();
        };
    }


    // Issues the calls while the fake server holds their answers, and waits until it received them all.
    private List<CompletableFuture<RPCResult>> sendHeldCalls(Client c, int count) throws Exception {
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            futures.add(future);
            c.rpcCall("step" + i, EMPTY_PAYLOAD, into(future));
        }
        for (int i = 0; i < count; i++) {
            assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        }
        return futures;
    }


    // Refuses the first calls with the code and the retry value, and answers the calls after them. The
    // refusals carry the server time, so the client re-syncs to it.
    private void refuseCalls(final int refusals, final int code, final String retry, final AtomicLong serverTime) {
        final AtomicInteger seen = new AtomicInteger();
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasRpc() || cmd.getRpc().getCancel() != 0) {
                return null;
            }
            rpcArrivalsNanos.add(System.nanoTime());
            if (seen.getAndIncrement() < refusals) {
                return refusal(cmd, code, retry, serverTime.get());
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setTime(serverTime.get())
                    .setRpc(Protocol.RPCResult.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8("ok")))
                    .build();
        };
    }


    // The error reply of a call the server refused. The retry value is set only when given.
    private static Protocol.Reply refusal(Protocol.Command cmd, int code, String retry, long serverTime) {
        Protocol.Error.Builder error = Protocol.Error.newBuilder()
                .setCode(code)
                .setMessage("refused");
        if (retry != null) {
            error.putMetadata(Extensions.RPC_RETRY_METADATA_KEY, retry);
        }
        return Protocol.Reply.newBuilder()
                .setId(cmd.getId())
                .setTime(serverTime)
                .setError(error)
                .build();
    }


    private static long timePart(Protocol.RPCRequest request) {
        return request.getMsgId() >> MsgIdGenerator.SEQUENCE_BITS;
    }


    private static void callAndWait(Client c) throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        future.get(5, TimeUnit.SECONDS);
    }


    // The fake server answers like a server with a dedup store: a msg_id runs once, and a
    // msg_id it has seen answers from the record with cached set. Calls are held while
    // holdCalls is set, and connects while holdConnect is set.
    private void serveDedup() {
        server.onCommand = (Protocol.Command cmd) ->{
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
        server.deferCommand = (Protocol.Command cmd) ->{
            if (cmd.hasConnect()) {
                return holdConnect.get();
            }
            return cmd.hasRpc() && cmd.getRpc().getCancel() == 0;
        };
    }


    private static RpcOptions headerOptions() {
        return new RpcOptions.Builder().header("x-request-source", "test").build();
    }
}
