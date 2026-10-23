package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import okhttp3.OkHttpClient;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class RpcPoolTest extends RpcTestBase {
    private static final String AFFINITY_HEADER = "x-example-affinity";
    // A port nothing listens on: its connections are refused, and never connect.
    private static final String UNREACHABLE_URL = "ws://127.0.0.1:1/connection/websocket";

    private final List<RpcPool> pools = new ArrayList<>();
    private final List<String> errorEndpoints = new ArrayList<>();


    @After
    public void closePools() {
        for (RpcPool pool : pools) {
            pool.close();
        }
    }


    // A callback which asks the pool for its stats and closes it runs to its end: the pool's thread answers both at once.
    @Test
    public void callbackMayCallStatsAndCloseWithoutHanging() throws Exception {
        answerRpcs("payload");
        RpcPool pool = build(builder());
        CompletableFuture<Void> callbackDone = new CompletableFuture<>();
        CompletableFuture<RPCResult> answered = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, (Throwable err, RPCResult res) -> {
            try {
                pool.stats();
                pool.close();
                callbackDone.complete(null);
            } catch (RuntimeException e) {
                callbackDone.completeExceptionally(e);
            }
            into(answered).onDone(err, res);
        });

        callbackDone.get(5, TimeUnit.SECONDS);
        assertEquals("payload", new String(answered.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
    }


    @Test
    public void aCallIsAnsweredOverAPlainConnection() throws Exception {
        answerRpcs("payload");
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("payload", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> requests = sentRpcRequests();
        assertEquals(1, requests.size());
        assertEquals("a plain call carries no msg_id", 0L, requests.get(0).getMsgId());
    }


    @Test
    public void theQueueWaitsForTheWindowOfTheConnection() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(8));
        List<CompletableFuture<RPCResult>> futures = submitMany(pool, 3);
        awaitCondition("one call on the wire and two waiting", () -> {
            RpcPool.Stats stats = pool.stats();
            return stats.getConnections() == 1 && stats.getInflight() == 1 && stats.getQueued() == 2;
        });
        awaitAll(futures, true);
    }


    @Test
    public void aCallOverTheQueueBoundIsRejectedWithLimitExceeded() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(1));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(second).getCode());
        awaitAll(Collections.singletonList(first), true);
    }


    @Test
    public void aDrainedKeyLeavesNoQueueBehind() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(64));
        CompletableFuture<RPCResult> onWire = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("key-0").build(), into(onWire));
        awaitCondition("the first call on the wire", () -> pool.stats().getInflight() == 1);
        List<CompletableFuture<RPCResult>> waiting = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            waiting.add(future);
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("key-" + i).build(), into(future));
        }
        awaitCondition("one waiting call per key", () -> pool.stats().getQueued() == 20);
        assertEquals("each waiting key holds a queue", 20, queueKeysOf(pool));
        waiting.add(onWire);
        awaitAll(waiting, true);
        assertEquals("a key whose queue drained holds no queue", 0, queueKeysOf(pool));
    }


    @Test
    public void poolConnectionsShareOneOkHttpClient() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(2).maxInflightPerConnection(1));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        awaitCondition("two connections", () -> pool.stats().getConnections() == 2);
        List<RpcPoolConnection> connections = connectionsOf(pool);
        OkHttpClient firstClient = connections.get(0).client.getOpts().getOkHttpClient();
        OkHttpClient secondClient = connections.get(1).client.getOpts().getOkHttpClient();
        assertNotNull("each connection is given the shared client", firstClient);
        assertSame("the connections share one client, with its dispatcher and pool", firstClient, secondClient);
        awaitAll(Arrays.asList(first, second), true);
    }


    @Test
    public void connectionsOfTwoPoolsInOneProcessAreNotCappedByTheDispatcher() throws Exception {
        // Guards the lifted dispatcher limit of the shared client against a future shared dispatcher with OkHttp's default
        // total limit of 64 requests: each websocket holds a dispatcher slot for its life, so 66 connections of two pools
        // would stall on such a dispatcher. Each connection had its own dispatcher before the sharing, so this passes there too.
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool first = build(builder().maxConnectionsPerEndpoint(33));
        RpcPool second = build(builder().maxConnectionsPerEndpoint(33));
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        for (RpcPool pool : Arrays.asList(first, second)) {
            for (int i = 0; i < 33; i++) {
                CompletableFuture<RPCResult> future = new CompletableFuture<>();
                futures.add(future);
                pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
            }
        }
        awaitRequestCount(66);
        awaitAll(futures, true);
    }


    @Test
    public void anAffinityGoesOutAsTheConfiguredHeader() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        RpcPool pool = build(builder().affinityHeader(AFFINITY_HEADER));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().affinity("hint-1").build(), into(future));
        future.get(5, TimeUnit.SECONDS);
        assertEquals("hint-1", sentRpcRequests().get(0).getHeadersMap().get(AFFINITY_HEADER));
    }


    @Test
    public void anAffinityHeaderNameIsSentInLowerCase() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        RpcPool pool = build(builder().affinityHeader("X-Example-Affinity"));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().affinity("hint-1").build(), into(future));
        future.get(5, TimeUnit.SECONDS);
        assertEquals("hint-1", sentRpcRequests().get(0).getHeadersMap().get(AFFINITY_HEADER));
    }


    @Test
    public void anAffinityWithoutTheHeaderOptionIsBadConfiguration() throws Exception {
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().affinity("hint-1").build(), into(future));
        assertEquals(RpcPool.BAD_CONFIGURATION, replyErrorOf(future).getCode());
        assertEquals("nothing is sent for a refused call", 0, sentRpcRequests().size());
    }


    @Test
    public void theAffinityHeaderAsAPlainHeaderIsBadConfigurationWithoutAnAffinity() throws Exception {
        RpcPool pool = build(builder().affinityHeader(AFFINITY_HEADER));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().header(AFFINITY_HEADER, "hint-1").build(), into(future));
        assertEquals(RpcPool.BAD_CONFIGURATION, replyErrorOf(future).getCode());
        assertEquals("nothing is sent for a refused call", 0, sentRpcRequests().size());
    }


    @Test
    public void anEndpointKeyWhichResolvesToNothingIsBadConfiguration() throws Exception {
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.<String>emptyList()).random(maximumDraws()));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals(RpcPool.BAD_CONFIGURATION, replyErrorOf(future).getCode());
    }


    @Test
    public void aLaterRefusalIsSentAgainAndAnswered() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if (calls.incrementAndGet() == 1) {
                return laterReply(cmd.getId());
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void aLaterRefusalOverTheRetriesSurfacesTheError() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() ? laterReply(cmd.getId()) : null;
        RpcPool pool = build(builder().retries(2));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        ReplyError error = replyErrorOf(future);
        assertEquals(111, error.getCode());
        assertEquals("one send and two resends", 3, sentRpcRequests().size());
    }


    @Test
    public void aCallWhoseConnectionIsLostIsSentAgain() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if (calls.incrementAndGet() == 1) {
                server.closeConnection();
                return null;
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder().reconnectDelays(0, 0));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void aRerouteSendsTheCallAgainUnderTheKeyTheServerNamed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if (calls.incrementAndGet() == 1) {
                return moveReply(cmd.getId(), "other-key");
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder().reroute(error -> error.getMetadata().get("x-example-relocate")));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void aRerouteWhichAlwaysMovesIsBoundedByTheRetries() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() ? moveReply(cmd.getId(), "other-key") : null;
        RpcPool pool = build(builder().retries(2).reroute(error -> "other-key"));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals(111, replyErrorOf(future).getCode());
        assertEquals("one send and two moves", 3, sentRpcRequests().size());
    }


    @Test
    public void poolRoutesMoveBeforeLater() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<String> keys = Collections.synchronizedList(new ArrayList<>());
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if (calls.incrementAndGet() == 1) {
                return laterAndMoveReply(cmd.getId(), "other-key");
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> {
            keys.add(endpointKey);
            return Collections.singletonList(server.url());
        })
                .random(maximumDraws())
                .reroute(error -> error.getMetadata().get("x-example-relocate"))
                .errorListener((endpoint, error) -> {
                }));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertTrue("the call was moved to the key the server named, not resent under its own key", keys.contains("other-key"));
        assertEquals("one send and one move", 2, sentRpcRequests().size());
    }


    @Test
    public void poolLostCallOnMessageSizeCloseIsNotResent() throws Exception {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if ("svc.write".equals(cmd.getRpc().getMethod())) {
                server.disconnect(1009, "message too big");
                return null;
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder().errorListener((endpoint, error) -> {
            synchronized (errorEndpoints) {
                errorEndpoints.add(endpoint);
            }
        }));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.write", EMPTY_PAYLOAD, null, into(future));
        assertTrue("the call fails", failsWithIOException(future));
        synchronized (errorEndpoints) {
            assertEquals("the close is reported before the call settles", 1, errorEndpoints.size());
        }
        CompletableFuture<RPCResult> next = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(next));
        assertEquals("ok", new String(next.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("the failed call is not sent again: the next request is the next call", Arrays.asList("svc.write", "svc.read"), sentMethods());
    }


    @Test
    public void aMessageSizeCloseOpensNoReplacementForTheCallOnTheWire() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        server.onCommand = cmd -> {
            if (cmd.hasConnect()) {
                connects.incrementAndGet();
                return null;
            }
            if (!cmd.hasRpc()) {
                return null;
            }
            if ("svc.write".equals(cmd.getRpc().getMethod())) {
                server.disconnect(1009, "message too big");
                return null;
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.write", EMPTY_PAYLOAD, null, into(future));
        assertTrue("the call fails", failsWithIOException(future));
        assertEquals("the call is settled by the close, and sent once", 1, sentRpcRequests().size());
        assertEquals("the close opens no replacement connection", 1, connects.get());
    }


    @Test
    public void aCallOverItsDeadlineIsNotSentAgain() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() && "svc.read".equals(cmd.getRpc().getMethod()) ? rpcReply(cmd.getId(), "ok") : null;
        server.deferCommand = cmd -> cmd.hasRpc() && "svc.hold".equals(cmd.getRpc().getMethod());
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> held = new CompletableFuture<>();
        pool.rpc("svc.hold", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(200L).build(), into(held));
        try {
            held.get(5, TimeUnit.SECONDS);
            fail("the call must time out");
        } catch (ExecutionException e) {
            assertTrue("a timeout", e.getCause() instanceof TimeoutException);
        }
        CompletableFuture<RPCResult> next = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(next));
        next.get(5, TimeUnit.SECONDS);
        assertEquals("the timed-out call is not sent again: the next request is the next call", Arrays.asList("svc.hold", "svc.read"), sentMethods());
    }


    @Test
    public void aCancelledCallWaitingForAConnectionIsNeverSent() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(8));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        RpcPool.Call call = pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        call.cancel();
        assertEquals(ReplyError.CANCELLED, replyErrorOf(second).getCode());
        awaitAll(Collections.singletonList(first), true);
        assertEquals("the cancelled call was never sent", 1, sentRpcRequests().size());
    }


    @Test
    public void closeRejectsTheWaitingCallsAndTheCallsOnTheWire() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(8));
        CompletableFuture<RPCResult> onWire = new CompletableFuture<>();
        CompletableFuture<RPCResult> waiting = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(onWire));
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(waiting));
        awaitCondition("one call on the wire", () -> pool.stats().getInflight() == 1);
        pool.close();
        assertEquals("the waiting call is rejected with the closed code", RpcPool.CLOSED_CODE, replyErrorOf(waiting).getCode());
        assertTrue("the call on the wire fails with the closed connection", failsWithIOException(onWire));
    }


    @Test
    public void anIdleConnectionIsClosedAfterTheIdleTimeout() throws Exception {
        answerRpcs("ok");
        RpcPool pool = build(builder().idleTimeout(100L));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        future.get(5, TimeUnit.SECONDS);
        awaitCondition("the connection is closed", () -> pool.stats().getConnections() == 0);
    }


    @Test
    public void aConnectionWithACallOnTheWireIsNotClosedByTheIdleTimeout() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().idleTimeout(100L));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        awaitCondition("the call on the wire", () -> pool.stats().getInflight() == 1);
        Thread.sleep(400L);
        assertEquals("the connection of a call on the wire stays open past the idle timeout", 1, pool.stats().getConnections());
        assertTrue("the call is still waiting", !future.isDone());
        awaitAll(Collections.singletonList(future), true);
        awaitCondition("the connection is closed once its call has ended", () -> pool.stats().getConnections() == 0);
    }


    @Test
    public void theIdleCheckIsNotRearmedByEachCallOnTheFallbackScheduler() throws Exception {
        answerRpcs("ok");
        // A timeout of an hour tells the idle check apart from the deadlines of the calls, which last ten seconds.
        RpcPool pool = build(builder().timeout(10_000).removeCancelledTimers(false).idleTimeout(TimeUnit.HOURS.toMillis(1L)));
        for (int i = 0; i < 50; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(10_000L).build(), into(future));
            future.get(5, TimeUnit.SECONDS);
        }
        assertEquals("one connection, kept open", 1, pool.stats().getConnections());
        assertEquals("one idle check for all the calls, not one cancelled timer per call", 1,
                timersLongerThan(pool, TimeUnit.MINUTES.toNanos(30L)));
    }


    @Test
    public void aSettledCallLetsGoOfItsPayloadOnTheFallbackScheduler() throws Exception {
        answerRpcs("ok");
        // The cancelled deadline of the call stays queued on this scheduler until its ten seconds are over.
        RpcPool pool = build(builder().timeout(10_000).removeCancelledTimers(false));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        byte[] payload = new byte[1 << 20];
        WeakReference<byte[]> released = new WeakReference<>(payload);
        pool.rpc("svc.read", payload, RpcPool.CallOptions.builder().timeout(10_000L).build(), into(future));
        future.get(5, TimeUnit.SECONDS);
        payload = null;
        awaitCondition("the payload of the settled call is released", () -> {
            System.gc();
            return released.get() == null;
        });
    }


    private static int timersLongerThan(RpcPool pool, long nanos) {
        int count = 0;
        for (Long delay : pool.timerDelays()) {
            if (delay > nanos) {
                count++;
            }
        }
        return count;
    }


    @Test
    public void aPoolWithoutIdleTimeoutKeepsItsConnection() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        server.onCommand = cmd -> {
            if (cmd.hasConnect()) {
                connects.incrementAndGet();
                return null;
            }
            return cmd.hasRpc() ? rpcReply(cmd.getId(), "ok") : null;
        };
        RpcPool pool = build(builder().idleTimeout(0L));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        first.get(5, TimeUnit.SECONDS);
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        second.get(5, TimeUnit.SECONDS);
        assertEquals("both calls went over the one connection, which was not closed", 1, connects.get());
        assertEquals(1, pool.stats().getConnections());
    }


    // A pool the application never closes is collected once its call settles and its connection closes at idleTimeout.
    @Test
    public void anUnclosedPoolIsCollectedOnceItsConnectionCloses() throws Exception {
        answerRpcs("ok");
        WeakReference<RpcPool> dropped = callOnUnclosedPool();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (dropped.get() != null && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(50L);
        }
        assertNull("the dropped pool is collected", dropped.get());
    }


    // The pool is not kept by the test: only the weak reference to it leaves this method.
    private WeakReference<RpcPool> callOnUnclosedPool() throws Exception {
        RpcPool pool = builder().idleTimeout(100L).build();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        future.get(5, TimeUnit.SECONDS);
        return new WeakReference<>(pool);
    }


    @Test
    public void aQueueWithoutRoomStillOpensAConnectionForACall() throws Exception {
        answerRpcs("ok");
        RpcPool pool = build(builder().maxQueued(0));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
    }


    @Test
    public void aFullQueueForOneKeyDoesNotRefuseACallOfAnotherKeyWithoutAConnection() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList("a".equals(endpointKey) ? server.url() : UNREACHABLE_URL))
                .random(maximumDraws())
                .maxConnectionsPerEndpoint(1)
                .maxQueued(1));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> waiting = new CompletableFuture<>();
        CompletableFuture<RPCResult> refused = new CompletableFuture<>();
        CompletableFuture<RPCResult> other = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(first));
        awaitCondition("the first call on the wire", () -> pool.stats().getInflight() == 1);
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(waiting));
        awaitCondition("the queue of key a is full", () -> pool.stats().getQueued() == 1);
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(refused));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("b").timeout(5_000L).build(), into(other));
        awaitCondition("a connection is opened for the other key", () -> pool.stats().getConnections() == 2);
        assertTrue("the call of the other key is not refused", !other.isDone());
        awaitAll(Collections.singletonList(first), true);
    }


    @Test
    public void aSaturatedKeyDoesNotHoldBackTheQueueOfAnotherKey() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        FakeCentrifugoServer other = new FakeCentrifugoServer();
        other.start();
        try {
            RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList("a".equals(endpointKey) ? server.url() : other.url()))
                    .random(maximumDraws())
                    .maxConnectionsPerEndpoint(1)
                    .maxQueued(4));
            CompletableFuture<RPCResult> onWire = new CompletableFuture<>();
            CompletableFuture<RPCResult> queued = new CompletableFuture<>();
            CompletableFuture<RPCResult> otherKey = new CompletableFuture<>();
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(onWire));
            awaitCondition("the first call of key a is on the wire", () -> pool.stats().getInflight() == 1);
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(queued));
            awaitCondition("the second call of key a waits", () -> pool.stats().getQueued() == 1);
            // Key a is saturated: its connection is at its window and it may open no other. Key b has its own endpoint.
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("b").timeout(5_000L).build(), into(otherKey));
            otherKey.get(5, TimeUnit.SECONDS);
            assertTrue("the waiting call of key a is still waiting", !queued.isDone());
            assertEquals(1, pool.stats().getQueued());
            awaitAll(Arrays.asList(onWire, queued), true);
        } finally {
            other.stop();
        }
    }


    @Test
    public void aConnectionOpenedForCallsWhichTimeOutWhileQueuedIsClosedAfterTheIdleTimeout() throws Exception {
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList(UNREACHABLE_URL))
                .random(maximumDraws())
                .idleTimeout(100L));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(50L).build(), into(future));
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("the call must time out");
        } catch (ExecutionException e) {
            assertTrue("a timeout", e.getCause() instanceof TimeoutException);
        }
        awaitCondition("the connection that never connected is closed", () -> pool.stats().getConnections() == 0);
    }


    @Test
    public void aConnectionTheServerClosesForGoodIsReportedAndNotDialledInALoop() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        server.onCommand = cmd -> {
            if (cmd.hasConnect()) {
                connects.incrementAndGet();
                server.disconnect(3501, "invalid token");
            }
            return null;
        };
        RpcPool pool = build(builder().errorListener((endpoint, error) -> {
            synchronized (errorEndpoints) {
                errorEndpoints.add(endpoint);
            }
        }));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(1_000L).build(), into(future));
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("the call must time out");
        } catch (ExecutionException e) {
            assertTrue("a timeout", e.getCause() instanceof TimeoutException);
        }
        synchronized (errorEndpoints) {
            assertTrue("the close is reported to the error listener", errorEndpoints.size() >= 1);
        }
        assertTrue("the endpoint is held back between the dials, not dialled at once: " + connects.get(), connects.get() < 5);
    }


    @Test
    public void theBuilderRefusesAnOptionOutOfItsRange() {
        try {
            new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url())).maxInflightPerConnection(0);
            fail("a window of zero is refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("maxInflightPerConnection"));
        }
    }


    // T14
    @Test
    public void callWaitsForItsConnectionToConnect() throws Exception {
        // The call is answered by the unhandled hook, so that a held call command is still unanswered when its written state is read.
        server.onUnhandled = cmd -> cmd.hasRpc() ? rpcReply(cmd.getId(), "payload") : null;
        server.deferCommand = cmd -> cmd.hasConnect();
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        awaitCondition("the call waits for its connection", () -> pool.stats().getQueued() == 1);
        awaitHeld(server);
        assertEquals("not written before the reply", 0, pool.stats().getInflight());
        server.deferCommand = cmd -> cmd.hasRpc();
        server.releaseDeferredCommands();
        awaitCondition("the call is written on its connection", () -> pool.stats().getInflight() == 1);
        assertEquals("the written call is no longer queued", 0, pool.stats().getQueued());
        // The written state is set before the server receives the command: wait until the server holds it.
        awaitHeld(server);
        server.releaseDeferredCommands();
        assertEquals("payload", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(1, connectsOf(server));
    }


    // T15
    @Test
    public void connectionsAreSharedByKeysOfOneEndpoint() throws Exception {
        answerRpcs("payload");
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxInflightPerConnection(2));
        CompletableFuture<RPCResult> a = new CompletableFuture<>();
        CompletableFuture<RPCResult> b = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(a));
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("b").build(), into(b));
        assertEquals("payload", new String(a.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("payload", new String(b.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("one socket for both keys", 1, connectsOf(server));
        assertEquals(2, sentRpcRequests().size());
        assertEquals(1, pool.stats().getConnections());
    }


    // T16
    @Test
    public void reroutedCallGoesToTheFrontOfItsNewLine() throws Exception {
        FakeCentrifugoServer other = new FakeCentrifugoServer();
        other.start();
        try {
            server.onCommand = cmd -> cmd.hasRpc() && "svc.a".equals(cmd.getRpc().getMethod()) ? moveReply(cmd.getId(), "b") : null;
            AtomicInteger held = new AtomicInteger();
            other.onUnhandled = cmd -> cmd.hasRpc() ? rpcReply(cmd.getId(), "ok") : null;
            // Only the first call on b is held: it keeps the window of b full while the rerouted call and a second call wait.
            other.deferCommand = cmd -> cmd.hasRpc() && held.incrementAndGet() == 1;
            RpcPool pool = build(new RpcPool.Builder(key -> Collections.singletonList("b".equals(key) ? other.url() : server.url()))
                    .random(maximumDraws())
                    .maxConnectionsPerEndpoint(1)
                    .maxInflightPerConnection(1)
                    .reroute(error -> error.getMetadata().get("x-example-relocate")));
            CompletableFuture<RPCResult> first = submitTo(pool, "svc.first", "b");
            awaitCondition("the first call holds the window of b", () -> rpcMethodsOf(other).size() == 1);
            CompletableFuture<RPCResult> second = submitTo(pool, "svc.second", "b");
            CompletableFuture<RPCResult> a = submitTo(pool, "svc.a", "a");
            // A has been answered with its move and waits at the front of b: the first call is on the wire, and two calls wait.
            awaitCondition("the rerouted call and the second call wait for b", () -> rpcMethodsOf(server).contains("svc.a")
                    && pool.stats().getInflight() == 1 && pool.stats().getQueued() == 2);
            other.releaseDeferredCommandsOnTheirSockets();
            first.get(5, TimeUnit.SECONDS);
            a.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(Arrays.asList("svc.first", "svc.a", "svc.second"), rpcMethodsOf(other));
        } finally {
            other.stop();
        }
    }


    // T31
    @Test
    public void retiredConnectionCloseReportsNoErrorAndHoldsNoOpen() throws Exception {
        FakeCentrifugoServer endpoint = new FakeCentrifugoServer();
        endpoint.start();
        try {
            server.onCommand = cmd -> cmd.hasRefresh() ? refreshAs(cmd, "bob") : connectAs(cmd, "alice", false);
            endpoint.onCommand = cmd -> {
                if (cmd.hasConnect()) {
                    return connectAs(cmd, cmd.getConnect().getToken().equals("bob-token") ? "bob" : "alice", false);
                }
                return cmd.hasRefresh() ? refreshAs(cmd, "alice") : null;
            };
            endpoint.onUnhandled = cmd -> cmd.hasRpc() ? rpcReply(cmd.getId(), "ok") : null;
            // A and B are held until the test answers them; C is answered at once.
            endpoint.deferCommand = cmd -> cmd.hasRpc() && !"svc.c".equals(cmd.getRpc().getMethod());
            Options options = new Options();
            options.setToken("alice-token");
            Client main = new Client(server.url(), options, new EventListener() {
                @Override
                public void onConnected(Client c, ConnectedEvent e) {
                    connectedSignals.release();
                }
            });
            connect(main);
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
            RpcPool pool = build(new RpcPool.Builder(key -> Collections.singletonList(endpoint.url()))
                    .random(maximumDraws())
                    .tokenFrom(main)
                    .maxConnectionsPerEndpoint(1)
                    .maxInflightPerConnection(2)
                    // A hold of a minute: if the stale end held the endpoint, C would not complete in time (2.4, T31).
                    .reconnectDelays(60_000, 60_000)
                    .errorListener((String url, Throwable error) -> errors.add(error)));
            CompletableFuture<RPCResult> a = submitTo(pool, "svc.a", "");
            CompletableFuture<RPCResult> b = submitTo(pool, "svc.b", "");
            awaitCondition("A and B are on the wire", () -> rpcMethodsOf(endpoint).size() == 2);
            CompletableFuture<AuthResult> refreshed = new CompletableFuture<>();
            main.refresh("bob-token", (Throwable error, AuthResult result) -> {
                if (error != null) {
                    refreshed.completeExceptionally(error);
                } else {
                    refreshed.complete(result);
                }
            });
            refreshed.get(5, TimeUnit.SECONDS);
            // The identity changed while A and B are in flight. A's answer frees a slot of the stale connection, which is retired with
            // B still on the wire. The fleet then closes it for good: B settles with the loss, no error is reported, and the one
            // connection the endpoint allows is free for the next call at once.
            endpoint.releaseDeferredMatching(cmd -> cmd.hasRpc() && "svc.a".equals(cmd.getRpc().getMethod()));
            assertEquals("ok", new String(a.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
            awaitCondition("B is still on the wire", () -> pool.stats().getInflight() == 1);
            endpoint.disconnect(3501, "terminal");
            assertEquals("B settles with the loss, the close reason of its connection's client", "terminal", errorOf(b).getMessage());
            CompletableFuture<RPCResult> c = submitTo(pool, "svc.c", "");
            assertEquals("ok", new String(c.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
            assertTrue("no error for the retired connection", errors.isEmpty());
        } finally {
            endpoint.stop();
        }
    }


    // T32
    @Test
    public void terminalCloseHoldsTheEndpointOpens() throws Exception {
        answerRpcs("payload");
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        RpcPool pool = build(builder().errorListener((String url, Throwable error) -> errors.add(error)));
        assertEquals("payload", new String(submitTo(pool, "svc.read", "").get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        server.disconnect(3501, "terminal");
        awaitCondition("the close is reported", () -> !errors.isEmpty());
        CompletableFuture<RPCResult> next = submitTo(pool, "svc.read", "");
        assertEquals("the next open waits the hold", 0, pool.stats().getConnections());
        assertEquals("payload", new String(next.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("the endpoint reopened after the hold", 2, connectsOf(server));
    }


    // T41
    @Test
    public void refusesTimerDurationsAboveTheLimit() throws Exception {
        try {
            builder().idleTimeout(2147483648L).build();
            fail("an idle timeout above the timer limit is refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("idleTimeout"));
        }
        answerRpcs("payload");
        RpcPool pool = build(builder().timeout(5000));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(5001L).build(), into(future));
        assertEquals(RpcPool.BAD_CONFIGURATION, ((ReplyError) errorOf(future)).getCode());
        // A negative call timeout is refused the same way, at the call (3.23), not when the options are built.
        CompletableFuture<RPCResult> negative = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(-1L).build(), into(negative));
        assertEquals(RpcPool.BAD_CONFIGURATION, ((ReplyError) errorOf(negative)).getCode());
        assertEquals("the fleet sees no connect", 0, connectsOf(server));
    }


    // T42
    @Test
    public void saturatedKeyDoesNotHoldBackAnotherKey() throws Exception {
        FakeCentrifugoServer other = new FakeCentrifugoServer();
        other.start();
        try {
            server.onCommand = cmd -> null;
            server.deferCommand = cmd -> cmd.hasRpc();
            other.onUnhandled = cmd -> cmd.hasRpc() ? rpcReply(cmd.getId(), "b") : null;
            other.deferCommand = cmd -> cmd.hasRpc();
            RpcPool pool = build(new RpcPool.Builder(key -> Collections.singletonList("b".equals(key) ? other.url() : server.url()))
                    .random(maximumDraws())
                    .maxConnectionsPerEndpoint(1)
                    .maxInflightPerConnection(1));
            CompletableFuture<RPCResult> a1 = submitTo(pool, "svc.a1", "a");
            awaitCondition("A1 holds the window of a", () -> rpcMethodsOf(server).size() == 1);
            CompletableFuture<RPCResult> a2 = submitTo(pool, "svc.a2", "a");
            assertEquals("A2 waits in its line", 1, pool.stats().getQueued());
            CompletableFuture<RPCResult> b = submitTo(pool, "svc.b", "b");
            awaitCondition("B is written at once", () -> rpcMethodsOf(other).size() == 1);
            assertEquals("A2 stays queued", 1, pool.stats().getQueued());
            long before = pool.keyLookups();
            other.releaseDeferredCommandsOnTheirSockets();
            assertEquals("b", new String(b.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
            assertEquals("the line of a is not looked up at the reply of b", before, pool.keyLookups());
            assertTrue("A2 is still waiting", !a2.isDone());
            server.releaseDeferredCommandsOnTheirSockets();
            a1.get(5, TimeUnit.SECONDS);
        } finally {
            other.stop();
        }
    }


    private static CompletableFuture<RPCResult> submitTo(RpcPool pool, String method, String endpointKey) {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc(method, EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey(endpointKey).build(), into(future));
        return future;
    }


    // The methods of the rpc requests a fake server received, in order.
    private static List<String> rpcMethodsOf(FakeCentrifugoServer endpoint) {
        List<String> methods = new ArrayList<>();
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasRpc()) {
                methods.add(cmd.getRpc().getMethod());
            }
        }
        return methods;
    }


    // The connect commands a fake server received.
    private static int connectsOf(FakeCentrifugoServer endpoint) {
        int count = 0;
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasConnect()) {
                count++;
            }
        }
        return count;
    }


    private static int refreshesOf(FakeCentrifugoServer endpoint) {
        int count = 0;
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasRefresh()) {
                count++;
            }
        }
        return count;
    }


    // A connect reply for the token of the command: the user of the token, the auth class, and the ttl of the connection if asked.
    private static Protocol.Reply connectAs(Protocol.Command cmd, String user, boolean expires) {
        Protocol.ConnectResult.Builder reply = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client")
                .setVersion("0.0.0")
                .setPing(25)
                .setUser(user)
                .setAuthClass("authenticated");
        if (expires) {
            reply.setExpires(true).setTtl(1);
        }
        return Protocol.Reply.newBuilder().setId(cmd.getId()).setConnect(reply.build()).build();
    }


    private static Protocol.Reply refreshAs(Protocol.Command cmd, String user) {
        return Protocol.Reply.newBuilder()
                .setId(cmd.getId())
                .setRefresh(Protocol.RefreshResult.newBuilder().setUser(user).setAuthClass("authenticated").setExpires(false))
                .build();
    }


    private RpcPool.Builder builder() {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .errorListener((endpoint, error) -> {
                });
    }


    // The connections of the pool. The pool's own thread owns its endpoints, so the map is read by reflection on that thread.
    @SuppressWarnings("unchecked")
    private static List<RpcPoolConnection> connectionsOf(RpcPool pool) throws Exception {
        Field field = RpcPool.class.getDeclaredField("endpoints");
        field.setAccessible(true);
        return onPoolThread(pool, () -> {
            List<RpcPoolConnection> all = new ArrayList<>();
            for (RpcPoolEndpoint endpoint : ((Map<String, RpcPoolEndpoint>) field.get(pool)).values()) {
                all.addAll(endpoint.connections());
            }
            return all;
        });
    }


    // The number of key queues the pool holds, read on its own thread by reflection.
    private static int queueKeysOf(RpcPool pool) throws Exception {
        Field field = RpcPool.class.getDeclaredField("queues");
        field.setAccessible(true);
        return onPoolThread(pool, () -> ((Map<?, ?>) field.get(pool)).size());
    }


    // Runs the read on the executor of the pool, the thread which owns its state, and returns what it reads.
    private static <T> T onPoolThread(RpcPool pool, Callable<T> read) throws Exception {
        Field field = RpcPool.class.getDeclaredField("executor");
        field.setAccessible(true);
        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) field.get(pool);
        return executor.submit(read).get();
    }


    private RpcPool build(RpcPool.Builder builder) {
        RpcPool pool = builder.build();
        pools.add(pool);
        return pool;
    }


    private static List<CompletableFuture<RPCResult>> submitMany(RpcPool pool, int count) {
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
            futures.add(future);
        }
        return futures;
    }


    // Answers the held commands until every call has its outcome: a held command is answered on the one socket of the fake server.
    private void awaitAll(List<CompletableFuture<RPCResult>> futures, boolean release) throws InterruptedException {
        awaitCondition("every call answered", () -> {
            if (release) {
                server.releaseDeferredCommands();
            }
            for (CompletableFuture<RPCResult> future : futures) {
                if (!future.isDone()) {
                    return false;
                }
            }
            return true;
        });
    }


    private List<String> sentMethods() {
        List<String> methods = new ArrayList<>();
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            methods.add(request.getMethod());
        }
        return methods;
    }


    private boolean failsWithIOException(CompletableFuture<RPCResult> future) {
        try {
            future.get(5, TimeUnit.SECONDS);
            return false;
        } catch (ExecutionException e) {
            return e.getCause() instanceof IOException;
        } catch (InterruptedException | TimeoutException e) {
            return false;
        }
    }


    private static Protocol.Reply rpcReply(int id, String payload) {
        return Protocol.Reply.newBuilder()
                .setId(id)
                .setRpc(Protocol.RPCResult.newBuilder()
                        .setData(com.google.protobuf.ByteString.copyFromUtf8(payload)))
                .build();
    }


    private static Protocol.Reply laterReply(int id) {
        return errorReply(id, Extensions.RPC_RETRY_METADATA_KEY, Extensions.RPC_RETRY_LATER);
    }


    private static Protocol.Reply moveReply(int id, String key) {
        return errorReply(id, "x-example-relocate", key);
    }


    // An error that carries the later signal and a move: the move wins (2.6, R4.1).
    private static Protocol.Reply laterAndMoveReply(int id, String key) {
        return Protocol.Reply.newBuilder()
                .setId(id)
                .setError(Protocol.Error.newBuilder()
                        .setCode(111)
                        .setMessage("too many requests")
                        .setTemporary(true)
                        .putMetadata(Extensions.RPC_RETRY_METADATA_KEY, Extensions.RPC_RETRY_LATER)
                        .putMetadata("x-example-relocate", key))
                .build();
    }


    private static Protocol.Reply errorReply(int id, String metadataKey, String metadataValue) {
        return Protocol.Reply.newBuilder()
                .setId(id)
                .setError(Protocol.Error.newBuilder()
                        .setCode(111)
                        .setMessage("too many requests")
                        .setTemporary(true)
                        .putMetadata(metadataKey, metadataValue))
                .build();
    }
}
