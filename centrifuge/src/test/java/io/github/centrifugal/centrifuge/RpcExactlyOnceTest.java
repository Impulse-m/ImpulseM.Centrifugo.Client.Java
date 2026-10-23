package io.github.centrifugal.centrifuge;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

import okio.ByteString;

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


    // A refused call takes no slot, so once the call which holds the slot settles, the next call is accepted.
    @Test
    public void aRefusedCallHoldsNoSlotOfTheBound() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Options options = instanceOptions();
        options.setMaxPendingRpc(1);
        Client c = newClient(options);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        c.rpcCall("first", EMPTY_PAYLOAD, into(first));
        CompletableFuture<RPCResult> refused = new CompletableFuture<>();
        c.rpcCall("refused", EMPTY_PAYLOAD, into(refused));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());

        connect(c);
        first.get(5, TimeUnit.SECONDS);
        CompletableFuture<RPCResult> later = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(later));

        later.get(5, TimeUnit.SECONDS);
        assertEquals("the refused call is never sent", 2, sentRpcRequests().size());
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

        assertEquals("close cancels the queued call, which was never written", ReplyError.CANCELLED, replyErrorOf(future).getCode());
        assertTrue(sentRpcRequests().isEmpty());
    }


    @Test
    public void callQueuedBeforeConnectIsCancelledByDisconnectForGood() throws Exception {
        enableRpcOptions();
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        drainExecutor();

        c.disconnect();

        assertEquals("disconnect for good cancels the call, which was never written", ReplyError.CANCELLED, replyErrorOf(future).getCode());
        connect(c);
        assertTrue("the cancelled call is not sent with a later connect", sentRpcRequests().isEmpty());
    }


    @Test
    public void writtenCallRejectedByDisconnectForGoodKeepsItsCancelForTheNextConnect() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("long", EMPTY_PAYLOAD, into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        long msgId = sentRpcRequests().get(0).getMsgId();

        c.disconnect();

        assertTrue("the written call fails with the transport", errorOf(future) instanceof IOException);
        connect(c);
        Protocol.RPCRequest cancel = awaitCancel(msgId);
        assertEquals(msgId, cancel.getCancel());
        long calls = sentRpcRequests().stream().filter((Protocol.RPCRequest request) -> request.getCancel() == 0).count();
        assertEquals("the written call is not sent again", 1, calls);
    }


    // A cancel whose send throws is kept for the next connection, and the cancels and the calls behind it still go out.
    @Test
    public void cancelWhoseSendThrowsIsKeptAndTheRestOfTheFlushGoesOut() throws Exception {
        enableRpcOptions();
        serveDedup();
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        Client c = newClientWithFailingSend(reported, true);
        connect(c);
        Set<Long> msgIds = new HashSet<>(sendHeldCallsAndDisconnect(c, 2));
        CompletableFuture<RPCResult> queuedFuture = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(queuedFuture));

        // The transport fails on the first cancel sent after the connect reply: the flush of the cancels sends first.
        connectWithFailingSend(c, new IllegalStateException("transport failed inside the send"), true);

        RPCResult queued = queuedFuture.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals("the call queued for the connect is answered", "{}", new String(queued.getData(), StandardCharsets.UTF_8));
        awaitCondition("one cancel sent with the connect", () -> cancelsSent().size() == 1);
        c.disconnect();
        connect(c);
        awaitCondition("the kept cancel sent with the next connect", () -> cancelsSent().size() == 2);
        assertEquals("both cancels are sent", msgIds, new HashSet<>(cancelsSent()));
        Throwable failure = reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the failed send is reported", failure instanceof UnclassifiedError);
        Throwable cause = ((UnclassifiedError) failure).getError();
        assertTrue("the cause is the transport's failure", cause instanceof IllegalStateException);
        assertEquals("the failed send is the transport's", "transport failed inside the send", cause.getMessage());
    }


    // An Error from the send of a cancel is handled as a refused cancel: the cancel is kept, and the flush goes on
    // with the other cancels and the calls queued for the connect.
    @Test
    public void cancelWhoseSendFailsWithAnErrorIsKeptAndTheRestOfTheFlushGoesOut() throws Exception {
        enableRpcOptions();
        serveDedup();
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        Client c = newClientWithFailingSend(reported, false);
        connect(c);
        Set<Long> msgIds = new HashSet<>(sendHeldCallsAndDisconnect(c, 3));
        CompletableFuture<RPCResult> queuedFuture = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(queuedFuture));

        connectWithFailingSend(c, new NoClassDefFoundError("transport linkage failed inside the send"), true);

        RPCResult queued = queuedFuture.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals("the call queued for the connect is answered", "{}", new String(queued.getData(), StandardCharsets.UTF_8));
        awaitCondition("the two cancels which did not fail are sent with the connect", () -> cancelsSent().size() == 2);
        Throwable failure = reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the failed send is reported", failure instanceof UnclassifiedError);
        assertTrue("the cause is the transport's Error", ((UnclassifiedError) failure).getError() instanceof NoClassDefFoundError);
        c.disconnect();
        connect(c);
        awaitCondition("the kept cancel sent with the next connect", () -> cancelsSent().size() == 3);
        assertEquals("every cancel is sent", msgIds, new HashSet<>(cancelsSent()));
    }


    // A failed cancel send is reported after the connect reply is handled: the report sees the expiry timer which the reply
    // armed, and a disconnect from the report leaves no timer armed.
    @Test
    public void failedCancelSendIsReportedAfterTheConnectReplyArmsItsExpiryTimer() throws Exception {
        enableRpcOptions();
        final int ttlSeconds = 60;
        final long expiryMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
        server.connectResult = server.connectResult.toBuilder().setExpires(true).setTtl(ttlSeconds).build();
        serveDedup();
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        BlockingQueue<Integer> armedAtReport = new LinkedBlockingQueue<>();
        ManualScheduler clock = new ManualScheduler();
        EventListener signalling = signallingListener();
        Client c = new Client(server.url(), instanceOptions(), new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                signalling.onConnected(client, event);
            }


            @Override
            public void onDisconnected(Client client, DisconnectedEvent event) {
                signalling.onDisconnected(client, event);
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
                armedAtReport.add(clock.pendingWithDelay(expiryMillis));
                client.disconnect();
            }
        }, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock, maximumDraws());
        client = c;
        connect(c);
        sendHeldCallsAndDisconnect(c, 1);

        connectWithFailingSend(c, new IllegalStateException("transport failed inside the send"), true);
        assertTrue("the failed send is reported", reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS) != null);
        Integer armed = armedAtReport.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the report runs after the connect reply armed its expiry timer", armed != null && armed > 0);
        awaitCondition("the report disconnects the client", () -> c.getState() == ClientState.DISCONNECTED);
        drainExecutor();
        assertEquals("the disconnect leaves no expiry timer armed", 0, clock.pendingWithDelay(expiryMillis));
    }


    // An Error from the send of a call fails that call alone: the calls behind it in the flush are still written.
    @Test
    public void callWhoseSendFailsWithAnErrorFailsAloneAndTheRestOfTheFlushGoesOut() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClientWithFailingSend(new LinkedBlockingQueue<Throwable>(), false);
        connect(c);
        c.disconnect();
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        CompletableFuture<RPCResult> third = new CompletableFuture<>();
        c.rpcCall("first", EMPTY_PAYLOAD, into(first));
        c.rpcCall("second", EMPTY_PAYLOAD, into(second));
        c.rpcCall("third", EMPTY_PAYLOAD, into(third));

        // The send of the first call fails inside the connect's flush, which writes the calls queued behind it.
        connectWithFailingSend(c, new NoClassDefFoundError("transport linkage failed inside the send"), false);

        try {
            first.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
            fail("the call whose send failed completes with the failure");
        } catch (ExecutionException e) {
            assertTrue("the failure is the send's", e.getCause() instanceof IOException);
            assertTrue("the cause is the transport's Error", e.getCause().getCause() instanceof NoClassDefFoundError);
        }
        assertEquals("the second call is answered", "{}", new String(second.get(WAIT_MILLIS, TimeUnit.MILLISECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("the third call is answered", "{}", new String(third.get(WAIT_MILLIS, TimeUnit.MILLISECONDS).getData(), StandardCharsets.UTF_8));
    }


    // Connects a client whose first send of a cancel, or of a call, after this connect fails with the given throwable.
    private void connectWithFailingSend(Client c, Throwable failure, boolean cancelSend) throws Exception {
        c.transportWrap = factory -> failingSends(factory, failure, cancelSend);
        c.connect();
        assertTrue("connected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        c.transportWrap = factory -> factory;
    }


    // A client whose error listener records what it reports. With throwOnReport the error listener throws after the
    // record, and the flush must go on regardless.
    private Client newClientWithFailingSend(BlockingQueue<Throwable> reported, boolean throwOnReport) {
        EventListener signalling = signallingListener();
        return newClient(instanceOptions(), new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                signalling.onConnected(client, event);
            }


            @Override
            public void onDisconnected(Client client, DisconnectedEvent event) {
                signalling.onDisconnected(client, event);
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
                if (throwOnReport) {
                    throw new IllegalStateException("listener failed on the report");
                }
            }
        });
    }


    // Sends the calls which the server holds, waits until they arrive, and disconnects. Returns the msg_ids of the calls.
    private List<Long> sendHeldCallsAndDisconnect(Client c, int count) throws Exception {
        holdCalls.set(true);
        for (int i = 0; i < count; i++) {
            c.rpcCall("held", EMPTY_PAYLOAD, into(new CompletableFuture<RPCResult>()));
        }
        for (int i = 0; i < count; i++) {
            assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));
        }
        List<Long> msgIds = new ArrayList<>();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            msgIds.add(request.getMsgId());
        }
        c.disconnect();
        holdCalls.set(false);
        return msgIds;
    }


    // The msg_ids of the cancels the client sent.
    private List<Long> cancelsSent() {
        List<Long> msgIds = new ArrayList<>();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            if (request.getCancel() != 0) {
                msgIds.add(request.getCancel());
            }
        }
        return msgIds;
    }


    // A transport factory which wraps the transports of the client, so that the first send of a cancel, or of a call, throws
    // as a transport can fail inside a send. The command is matched by its payload, so the sends of subscribes or connect
    // commands do not take the failure. The wrapper is the transport the client holds, and the listener of each transport
    // gets the wrapper as its transport too, so the replies still match the transport the client holds.
    private static WebSocket.Factory failingSends(WebSocket.Factory factory, Throwable failure, boolean cancelSend) {
        AtomicBoolean armed = new AtomicBoolean(true);
        return (Request request, WebSocketListener listener) -> {
            AtomicReference<WebSocket> real = new AtomicReference<>();
            InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
                if (method.getName().equals("send") && isTargetPayload(args[0], cancelSend) && armed.getAndSet(false)) {
                    throw failure;
                }
                try {
                    return method.invoke(real.get(), args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            };
            WebSocket proxy = (WebSocket) Proxy.newProxyInstance(WebSocket.class.getClassLoader(), new Class<?>[] {WebSocket.class}, handler);
            real.set(factory.newWebSocket(request, new WebSocketListener() {
                @Override
                public void onOpen(WebSocket webSocket, Response response) {
                    listener.onOpen(proxy, response);
                }


                @Override
                public void onMessage(WebSocket webSocket, String text) {
                    listener.onMessage(proxy, text);
                }


                @Override
                public void onMessage(WebSocket webSocket, ByteString bytes) {
                    listener.onMessage(proxy, bytes);
                }


                @Override
                public void onClosing(WebSocket webSocket, int code, String reason) {
                    listener.onClosing(proxy, code, reason);
                }


                @Override
                public void onClosed(WebSocket webSocket, int code, String reason) {
                    listener.onClosed(proxy, code, reason);
                }


                @Override
                public void onFailure(WebSocket webSocket, Throwable cause, Response response) {
                    listener.onFailure(proxy, cause, response);
                }
            }));
            return proxy;
        };
    }


    // Whether a sent payload is an rpc command of the kind the send fails on: a cancel, or a call. A payload which is not
    // a command is neither.
    private static boolean isTargetPayload(Object payload, boolean cancel) {
        if (!(payload instanceof ByteString)) {
            return false;
        }
        Protocol.Command cmd;
        try {
            cmd = Protocol.Command.parseDelimitedFrom(new ByteArrayInputStream(((ByteString) payload).toByteArray()));
        } catch (IOException e) {
            return false;
        }
        return cmd != null && cmd.hasRpc() && (cmd.getRpc().getCancel() != 0) == cancel;
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

        // The call was never registered, so no deadline or callback can run for it.
        assertEquals(0, callbacks.get());
    }


    @Test
    public void rpcStartedAfterCloseFailsAtOnce() throws Exception {
        Client c = newClient();
        CountDownLatch release = new CountDownLatch(1);
        // Holds the executor, so that the close task and the start of the call are queued before either runs.
        c.getExecutor().submit(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        c.close(0);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("late", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(60_000).build(), into(future));

        release.countDown();

        assertEquals("the call is cancelled by the close, not failed by its deadline", ReplyError.CANCELLED, replyErrorOf(future).getCode());
    }


    @Test
    public void cancelAfterCloseDoesNotThrow() throws Exception {
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        drainExecutor();
        c.close(1000);
        assertEquals("close cancels the queued call", ReplyError.CANCELLED, replyErrorOf(future).getCode());

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
    public void aSettledCallDropsItsPayload() throws Exception {
        enableRpcOptions();
        serveDedup();
        Client c = newClient();
        connect(c);
        holdCalls.set(true);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("long", EMPTY_PAYLOAD, into(future));
        assertTrue("call arrived", callArrivals.tryAcquire(5, TimeUnit.SECONDS));

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
        awaitCondition("the settled call drops its payload", () -> payloadDropped(c, call));
    }


    // Whether the call has dropped its payload, read on the client's executor: the call drops it there, when it finishes.
    // A failure of the read is rethrown with its cause, so the wait does not report it as a plain timeout.
    private static boolean payloadDropped(Client c, RpcCall call) {
        try {
            return c.getExecutor().submit(() -> call.data == null).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("the read of the payload was interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("the payload could not be read", e);
        }
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
    public void nullMethodOrDataIsRefusedAtTheCallAndTheQueuedCallsStillGoOut() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        CompletableFuture<RPCResult> before = new CompletableFuture<>();
        CompletableFuture<RPCResult> after = new CompletableFuture<>();
        c.rpcCall("first", EMPTY_PAYLOAD, into(before));
        assertNullArgumentRefused(c, null, EMPTY_PAYLOAD);
        assertNullArgumentRefused(c, "m", null);
        c.rpcCall("second", EMPTY_PAYLOAD, into(after));

        connect(c);

        before.get(5, TimeUnit.SECONDS);
        after.get(5, TimeUnit.SECONDS);
        assertEquals("only the two valid calls are sent", 2, sentRpcRequests().size());
    }


    private void assertNullArgumentRefused(Client c, String method, byte[] data) {
        try {
            c.rpcCall(method, data, into(new CompletableFuture<RPCResult>()));
            fail("a null argument is refused at the call");
        } catch (NullPointerException expected) {
            // Refused on the caller's thread: nothing was queued.
        }
    }


    @Test
    public void cancelsLostWithTheTransportAreCappedAtTheBound() throws Exception {
        enableRpcOptions();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client c = newClient();
        connect(c);
        for (int i = 0; i < 40; i++) {
            final int index = i;
            RpcCall call = c.rpcCall("m" + i, EMPTY_PAYLOAD, into(new CompletableFuture<RPCResult>()));
            awaitCondition("call " + index + " sent", () -> sentRpcRequests().size() > 2 * index);
            call.cancel();
        }
        awaitCondition("all cancels sent", () -> sentRpcRequests().size() == 80);
        List<Long> msgIds = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            msgIds.add(sentRpcRequests().get(2 * i).getMsgId());
        }

        server.deferCommand = null;
        server.closeConnection();

        assertTrue("reconnected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        syncWithServer();
        List<Protocol.RPCRequest> cancels = sentCancels();
        assertEquals("the forty cancels, then the kept ones", 72, cancels.size());
        for (int i = 0; i < 32; i++) {
            assertEquals("the newest cancels are kept", msgIds.get(8 + i).longValue(), cancels.get(40 + i).getCancel());
        }
    }


    @Test
    public void unansweredCancelIsDroppedAfterTheTimeoutAndNotSentAgain() throws Exception {
        enableRpcOptions();
        Options options = instanceOptions();
        options.setTimeout(200);
        options.setMinReconnectDelay(1);
        options.setMaxReconnectDelay(1);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(options, clock);
        connect(c);
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, into(new CompletableFuture<RPCResult>()));
        awaitRequestCount(1);
        long msgId = sentRpcRequests().get(0).getMsgId();

        call.cancel();
        awaitCancel(msgId);
        drainExecutor();
        // The cancel waits for its reply one timeout, and then it is forgotten.
        clock.advance(200);
        drainExecutor();

        server.deferCommand = null;
        dropTransportOnClock(clock);
        reconnectOnClock(clock);
        syncWithServer();
        assertEquals("the unanswered cancel is not sent again", 1, sentCancels().size());
    }


    @Test
    public void deadlineOfASentCallFiresOnTheClientClockAndCancelsIt() throws Exception {
        enableRpcOptions();
        Options options = instanceOptions();
        options.setTimeout(300);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(options, clock);
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);
        long msgId = sentRpcRequests().get(0).getMsgId();
        assertTrue("the deadline is a timer of the client", clock.requestedDelays().contains(300L));

        clock.advance(299);
        assertFalse("not yet at the deadline", future.isDone());
        clock.advance(1);

        assertTrue("the deadline gives a timeout", errorOf(future) instanceof TimeoutException);
        awaitCancel(msgId);
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
    public void legacyServerCancelsACallNeverSentLocally() throws Exception {
        answerRpcs("ok");
        Client c = newClient();
        connect(c);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = new CompletableFuture<>();
        c.refresh("token-2", completeInto(refreshFuture));
        syncWithServer();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, into(future));

        call.cancel();

        // Nothing reached the server, so there is nothing to cancel on the server: the call is cancelled locally.
        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
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
    public void aCallbackWhichThrowsOnTheExecutorIsReportedToTheListener() throws Exception {
        enableRpcOptions();
        answerRpcs("ok", System::currentTimeMillis);
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        EventListener listener = new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                connectedSignals.release();
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
            }
        };
        Client c = newClient(instanceOptions(), listener);
        connect(c);
        c.rpcCall("m", EMPTY_PAYLOAD, (Throwable error, RPCResult result) -> {
            throw new IllegalStateException("callback failed");
        });

        Throwable failure = reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the failure of the callback is reported", failure instanceof UnclassifiedError && ((UnclassifiedError) failure).getError() instanceof IllegalStateException);
    }


    @Test
    public void msgIdsIncreaseWhileTheClockIsSteadyAndFitFiftyThreeBits() throws Exception {
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
    public void lostCallKeepsItsSlotAcrossTheReconnect() throws Exception {
        enableRpcOptions();
        serveDedup();
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        connect(c);
        connectArrivals.drainPermits();
        holdCalls.set(true);
        holdConnect.set(true);
        List<CompletableFuture<RPCResult>> futures = sendHeldCalls(c, 2);
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
        assertEquals("each call ran once", 2, executions.get());
        callAndWait(c);
    }


    @Test
    public void thirtyThirdCallWhileDisconnectedIsRefusedAtOnce() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        List<CompletableFuture<RPCResult>> held = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            held.add(future);
            c.rpcCall("held" + i, EMPTY_PAYLOAD, into(future));
        }
        CompletableFuture<RPCResult> refused = new CompletableFuture<>();

        c.rpcCall("refused", EMPTY_PAYLOAD, into(refused));

        drainExecutor();
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());
        for (CompletableFuture<RPCResult> future : held) {
            assertFalse("held while disconnected", future.isDone());
        }
        connect(c);
        for (CompletableFuture<RPCResult> future : held) {
            future.get(5, TimeUnit.SECONDS);
        }
        assertEquals(32, sentRpcRequests().size());
    }


    @Test
    public void settledCallsFreeTheirSlots() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClient(options);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        c.rpcCall("a", EMPTY_PAYLOAD, into(first));
        c.rpcCall("b", EMPTY_PAYLOAD, into(second));
        CompletableFuture<RPCResult> refused = new CompletableFuture<>();
        c.rpcCall("c", EMPTY_PAYLOAD, into(refused));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());

        connect(c);
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        callAndWait(c);

        assertEquals(3, sentRpcRequests().size());
    }


    @Test
    public void aSettledCallFreesItsSlotBeforeTheExecutorDeliversIt() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Options options = instanceOptions();
        options.setMaxPendingRpc(2);
        Client c = newClientOnClock(options, clock);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        c.rpcCall("a", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(100).build(), into(first));
        c.rpcCall("b", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(100).build(), into(second));
        // The executor is busy with a long callback, so the delivery of the two calls waits behind it.
        CountDownLatch busy = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        c.getExecutor().submit(() -> {
            busy.countDown();
            try {
                release.await(WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        busy.await(WAIT_MILLIS, TimeUnit.MILLISECONDS);

        clock.advance(100);
        CompletableFuture<RPCResult> late = new CompletableFuture<>();
        try {
            c.rpcCall("c", EMPTY_PAYLOAD, into(late));
            assertFalse("the settled calls gave their slots back before the executor delivered them", late.isDone());
        } finally {
            release.countDown();
        }

        assertTrue(errorOf(first) instanceof TimeoutException);
        assertTrue(errorOf(second) instanceof TimeoutException);
    }


    @Test
    public void legacyCallHoldsItsSlotUntilItSettles() throws Exception {
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Options options = instanceOptions();
        options.setMaxPendingRpc(1);
        Client c = newClient(options);
        connect(c);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        c.rpcCall("first", EMPTY_PAYLOAD, into(first));
        awaitRequestCount(1);
        CompletableFuture<RPCResult> refused = new CompletableFuture<>();

        c.rpcCall("second", EMPTY_PAYLOAD, into(refused));

        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());
        server.deferCommand = null;
        server.releaseDeferredCommands();
        first.get(5, TimeUnit.SECONDS);
        callAndWait(c);
    }


    // A call which was never written, queued before the client first connected, goes out as a first send.
    @Test
    public void aCallQueuedBeforeTheFirstConnectIsSentAsAFirstSend() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Client c = newClient();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(future));
        drainExecutor();
        connect(c);

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(1, requests.size());
        assertFalse("a call never written is not a retransmit", requests.get(0).getRetransmit());
    }


    @Test
    public void callsWithoutAnInstanceHaveNoBound() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Options options = new Options();
        options.setMaxPendingRpc(1);
        Client c = newClient(options);
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            futures.add(future);
            c.rpcCall("m" + i, EMPTY_PAYLOAD, into(future));
        }

        drainExecutor();
        for (CompletableFuture<RPCResult> future : futures) {
            assertFalse("accepted and held, not refused", future.isDone());
        }
        connect(c);
        for (CompletableFuture<RPCResult> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
    }


    @Test
    public void cancelledCallsFreeTheirSlots() throws Exception {
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


    // The refusal is answered and the transport is lost, both queued behind the gate. The executor then runs the
    // reply first, and the loss second: the answer to the attempt comes after the loss in the queue.
    @Test
    public void refusalAnsweredBeforeTheTransportLossIsResentWithANewMsgId() throws Exception {
        enableRpcOptions();
        final AtomicBoolean holdFirst = new AtomicBoolean(true);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && cmd.getRpc().getCancel() == 0 && holdFirst.getAndSet(false);
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(future));
        awaitRequestCount(1);
        Protocol.Command held = null;
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRpc() && cmd.getRpc().getCancel() == 0) {
                held = cmd;
            }
        }
        final Protocol.Reply refused = refusal(held, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, System.currentTimeMillis());
        final CountDownLatch gate = new CountDownLatch(1);
        c.getExecutor().submit(() -> awaitLatch(gate));
        Future<?> answered = c.getExecutor().submit(() -> processReply(c, refused));
        Future<?> lost = c.getExecutor().submit(() -> c.processDisconnect(1000, "bye", true));
        gate.countDown();
        answered.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        lost.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        server.closeConnection();

        future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertNotEquals("the refused msg_id is replaced", requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertFalse("the new msg_id is not a retransmit", requests.get(1).getRetransmit());
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
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(instanceOptions(), clock);
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(future));
        awaitCondition("the resend is scheduled", () -> clock.requestedDelays().contains(250L));

        clock.advance(249);
        syncWithServer();
        assertEquals("the resend waits 250 ms", 1, sentRpcRequests().size());
        clock.advance(1);

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(2, requests.size());
        assertEquals("the resend keeps the msg_id", requests.get(0).getMsgId(), requests.get(1).getMsgId());
        assertFalse(requests.get(0).getRetransmit());
        assertTrue("the resend is a retransmit", requests.get(1).getRetransmit());
    }


    @Test
    public void cancelReplyTimerEndsWithTheTransport() throws Exception {
        enableRpcOptions();
        Options options = instanceOptions();
        options.setTimeout(777);
        options.setMinReconnectDelay(1);
        options.setMaxReconnectDelay(1);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(options, clock);
        connect(c);
        RpcCall call = c.rpcCall("m", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(60_000).build(), into(new CompletableFuture<RPCResult>()));
        awaitRequestCount(1);
        long msgId = sentRpcRequests().get(0).getMsgId();

        call.cancel();
        awaitCancel(msgId);
        drainExecutor();
        assertEquals("the reply of the cancel is timed", 1, clock.pendingWithDelay(777));

        dropTransportOnClock(clock);

        assertEquals("the transport ended the reply timer of the cancel", 0, clock.pendingWithDelay(777));
    }


    @Test
    public void laterResendWithTheLowestDrawIsSentAtOnce() throws Exception {
        enableRpcOptions();
        refuseCalls(1, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(instanceOptions(), clock, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, lowestDraws());
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("later", EMPTY_PAYLOAD, into(future));
        awaitCondition("the resend is scheduled without a delay", () -> clock.requestedDelays().contains(0L));

        clock.advance(0);

        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
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
        // The resend waits its whole 250 ms bound, so the transport drops while the delay runs. A jittered draw
        // near zero would resend on the old transport first and count a third request.
        Client c = newClientWithMaximumDraws(instanceOptions());
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
        // The first call is refused for now. Its resend is sent at 250 ms, and the server holds it without an
        // answer, so the deadline at 1000 ms finds the resend on the wire and cancels it.
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
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(instanceOptions(), clock);
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));
        awaitCondition("the resend is scheduled", () -> clock.requestedDelays().contains(250L));
        long msgId = sentRpcRequests().get(0).getMsgId();

        clock.advance(250);
        awaitRequestCount(2);
        clock.advance(750);

        assertTrue(errorOf(future) instanceof TimeoutException);
        assertEquals(msgId, awaitCancel(msgId).getCancel());
        assertEquals("the resend keeps the msg_id", msgId, sentRpcRequests().get(1).getMsgId());
        assertTrue("the resend is a retransmit", sentRpcRequests().get(1).getRetransmit());
    }


    // A later refusal whose resend would run past the deadline is capped at the deadline: no resend is made
    // after it, and the deadline rejects the call with a timeout and cancels it.
    @Test
    public void laterRefusalWhoseResendWouldReachTheDeadlineIsCappedAtTheDeadline() throws Exception {
        enableRpcOptions();
        refuseCalls(2, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(instanceOptions(), clock);
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        // The first resend is due at 250 ms, and the second, 500 ms later, would be past the deadline at 300 ms.
        c.rpcCall("later", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(300).build(), into(future));
        awaitCondition("the first resend is scheduled", () -> clock.requestedDelays().contains(250L));
        clock.advance(250);
        awaitRequestCount(2);
        awaitCondition("the second resend waits for the deadline", () -> clock.pendingWithDelay(50L) == 1);

        clock.advance(50);

        assertTrue("the deadline rejects the call", errorOf(future) instanceof TimeoutException);
        awaitCondition("the deadline cancels the sent call", () -> sentCancels().size() == 1);
        assertEquals("no second resend is made", 2, sentRpcRequests().size() - sentCancels().size());
    }


    @Test
    public void laterRefusalWhoseDelayWouldReachTheDeadlineIsCappedAtTheDeadline() throws Exception {
        enableRpcOptions();
        refuseCalls(1, TEMPORARY, Extensions.RPC_RETRY_LATER, new AtomicLong(System.currentTimeMillis()));
        ManualScheduler clock = new ManualScheduler();
        Client c = newClientOnClock(instanceOptions(), clock);
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        // The deadline is shorter than the 250 ms delay: the resend is capped at the remaining deadline, so the
        // deadline rejects the call with a timeout, and the sent call gets its cancel.
        c.rpcCall("later", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(100).build(), into(future));
        awaitCondition("the resend is scheduled at the deadline", () -> clock.pendingWithDelay(100L) == 2);
        assertFalse("no resend waits for the 250 ms delay", clock.requestedDelays().contains(250L));

        clock.advance(100);

        assertTrue("the deadline rejects the call", errorOf(future) instanceof TimeoutException);
        awaitCondition("the deadline cancels the sent call", () -> sentCancels().size() == 1);
        assertEquals("the call is sent once, and never resent", 1, sentRpcRequests().size() - sentCancels().size());
    }


    @Test
    public void cancelOfACallNeverSentIsCancelledOnALegacyConnection() throws Exception {
        Client c = newClient();
        connect(c);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        c.refresh("token-2", (Throwable error, AuthResult result) -> {
        });
        awaitCondition("refresh sent", () -> hasRefreshCommand());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("queued", EMPTY_PAYLOAD, into(future));

        call.cancel();

        assertEquals(ReplyError.CANCELLED, replyErrorOf(future).getCode());
        server.releaseDeferredCommands();
        syncWithServer();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            assertFalse("the cancelled call is never sent", "queued".equals(request.getMethod()));
        }
    }


    @Test
    public void instanceIsReadWhenTheClientIsCreated() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        Options options = instanceOptions();
        Client c = newClient(options);
        options.setInstance(null);
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("method", EMPTY_PAYLOAD, into(future));
        future.get(5, TimeUnit.SECONDS);

        assertEquals(TEST_INSTANCE, lastConnectRequest().getInstance());
        assertNotEquals("the call is tracked with the instance it was created with", 0L, sentRpcRequests().get(0).getMsgId());
    }


    @Test
    public void lateReplyToASettledCallIsAckedWithTheNextCall() throws Exception {
        enableRpcOptions();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && !cmd.getRpc().getMethod().isEmpty();
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(200).build(), into(future));
        awaitRequestCount(1);
        long msgId = sentRpcRequests().get(0).getMsgId();

        assertTrue(errorOf(future) instanceof TimeoutException);
        assertEquals(msgId, awaitCancel(msgId).getCancel());

        // The server answers the call after the deadline: the answer is late, and it is acked.
        server.deferCommand = null;
        server.releaseDeferredCommands();
        syncWithServer();
        CompletableFuture<RPCResult> next = new CompletableFuture<>();
        c.rpcCall("next", EMPTY_PAYLOAD, into(next));
        next.get(5, TimeUnit.SECONDS);

        List<Protocol.RPCRequest> requests = sentRpcRequests();
        Protocol.RPCRequest last = requests.get(requests.size() - 1);
        assertEquals("next", last.getMethod());
        assertTrue("the late reply is acked", last.getAckList().contains(msgId));
    }


    @Test
    public void resendWithANewMsgIdThatTimesOutWhileQueuedSendsNoCancel() throws Exception {
        enableRpcOptions();
        // Holds the first call and the refresh, so the refused call waits for the refresh when it is refused.
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh() || (cmd.hasRpc() && !cmd.getRpc().getMethod().isEmpty());
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));
        awaitRequestCount(1);
        Protocol.Command original = null;
        for (Protocol.Command command : server.received()) {
            if (command.hasRpc() && "slow".equals(command.getRpc().getMethod())) {
                original = command;
            }
        }
        c.refresh("token-2", (Throwable error, AuthResult result) -> {
        });
        awaitCondition("refresh sent", () -> hasRefreshCommand());

        server.sendReply(refusal(original, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, System.currentTimeMillis()));

        assertTrue("the resend waits behind the refresh until the deadline", errorOf(future) instanceof TimeoutException);
        assertEquals("the resend was never sent", 1, sentRpcRequests().size());
        assertTrue("a resend which was never sent has no cancel to send", sentCancels().isEmpty());
    }


    @Test
    public void cancelOfAResendQueuedBehindARefreshSendsNoCancel() throws Exception {
        enableRpcOptions();
        // Holds the first call and the refresh, so the refused call is queued behind the refresh when it is refused.
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh() || (cmd.hasRpc() && !cmd.getRpc().getMethod().isEmpty());
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        RpcCall call = c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));
        awaitRequestCount(1);
        Protocol.Command original = null;
        for (Protocol.Command command : server.received()) {
            if (command.hasRpc() && "slow".equals(command.getRpc().getMethod())) {
                original = command;
            }
        }
        c.refresh("token-2", (Throwable error, AuthResult result) -> {
        });
        awaitCondition("refresh sent", () -> hasRefreshCommand());
        server.sendReply(refusal(original, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, System.currentTimeMillis()));
        // The marker is answered after the refusal, so the refusal is handled once the executor has drained.
        syncWithServer();
        drainExecutor();

        call.cancel();

        assertEquals(ReplyError.CANCELLED, ((ReplyError) errorOf(future)).getCode());
        answerRefreshWith(Protocol.Reply.newBuilder().setRefresh(Protocol.RefreshResult.newBuilder()));
        server.releaseDeferredCommands();
        syncWithServer();
        assertTrue("the id of the resend never reached the server, so it has no cancel", sentCancels().isEmpty());
        assertEquals("the resend was never sent", 1, sentRpcRequests().size());
    }


    @Test
    public void cancelOfAResendQueuedWhileALegacyConnectionConnectsIsCancelledLocally() throws Exception {
        enableRpcOptions();
        // Holds the first call and the refresh, so the refused call is queued behind the refresh when it is refused.
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh() || (cmd.hasRpc() && !cmd.getRpc().getMethod().isEmpty());
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        // The deadline is far beyond the scenario: the call must end by its cancel, never by a slow runner's timeout.
        RpcCall call = c.rpcCall("slow", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(10_000).build(), into(future));
        awaitRequestCount(1);
        Protocol.Command original = null;
        for (Protocol.Command command : server.received()) {
            if (command.hasRpc() && "slow".equals(command.getRpc().getMethod())) {
                original = command;
            }
        }
        c.refresh("token-2", (Throwable error, AuthResult result) -> {
        });
        awaitCondition("refresh sent", () -> hasRefreshCommand());
        server.sendReply(refusal(original, MSG_ID_INVALID, Extensions.RPC_RETRY_NEW_MSG_ID, System.currentTimeMillis()));
        syncWithServer();
        drainExecutor();
        // The connection is lost, and the next one is a legacy server. Its connect reply is held, so a refresh
        // starts while the client connects, and the flush of the queued call waits behind it.
        server.connectResult = Protocol.ConnectResult.newBuilder().setClient("fake-client").setVersion("0.0.0").setPing(25).build();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect() || cmd.hasRefresh() || (cmd.hasRpc() && !cmd.getRpc().getMethod().isEmpty());
        server.closeConnection();
        awaitCondition("reconnect started", () -> server.received().stream().filter(Protocol.Command::hasConnect).count() >= 2);
        c.refresh("token-3", (Throwable error, AuthResult result) -> {
        });

        server.releaseDeferredCommands();
        assertTrue("reconnected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        drainExecutor();
        call.cancel();

        assertEquals("a resend queued on a legacy connection is cancelled, not refused", ReplyError.CANCELLED, replyErrorOf(future).getCode());
        assertTrue("the id of the resend never reached the server, so it has no cancel", sentCancels().isEmpty());
        assertEquals("the resend was never sent", 1, sentRpcRequests().size());
    }


    @Test
    public void acksOfACommandLostWithItsTransportAreSentAgain() throws Exception {
        enableRpcOptions();
        Client c = newClient();
        connect(c);
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        c.rpcCall("first", EMPTY_PAYLOAD, into(first));
        first.get(5, TimeUnit.SECONDS);
        long firstMsgId = sentRpcRequests().get(0).getMsgId();
        AtomicInteger seen = new AtomicInteger();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && "second".equals(cmd.getRpc().getMethod()) && seen.incrementAndGet() == 1;
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        c.rpcCall("second", EMPTY_PAYLOAD, into(second));
        awaitCondition("second held by the server", () -> seen.get() == 1);

        server.closeConnection();

        second.get(5, TimeUnit.SECONDS);
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        Protocol.RPCRequest retransmit = requests.get(requests.size() - 1);
        assertTrue(retransmit.getRetransmit());
        assertTrue("the ack of the first reply is sent again with the retransmit", retransmit.getAckList().contains(firstMsgId));
    }


    // Drops the transport of a client whose reconnect delay runs on the clock, and waits until the reconnect is scheduled.
    private void dropTransportOnClock(ManualScheduler clock) throws Exception {
        int scheduled = clock.requestedDelays().size();
        server.closeConnection();
        awaitCondition("reconnect scheduled", () -> clock.requestedDelays().size() > scheduled);
        drainExecutor();
    }


    // Moves the clock past the reconnect delay, which the caller set to one millisecond, and waits for the connect.
    private void reconnectOnClock(ManualScheduler clock) throws Exception {
        clock.advance(2);
        assertTrue("reconnected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
    }


    private boolean hasRefreshCommand() {
        for (Protocol.Command command : server.received()) {
            if (command.hasRefresh()) {
                return true;
            }
        }
        return false;
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


    // Delivers the reply to the client as its transport does, on the executor.
    private static void processReply(Client c, Protocol.Reply reply) {
        try {
            c.processReply(reply);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    // The cancel requests the client sent, in order.
    private List<Protocol.RPCRequest> sentCancels() {
        List<Protocol.RPCRequest> cancels = new ArrayList<>();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            if (request.getCancel() != 0) {
                cancels.add(request);
            }
        }
        return cancels;
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
