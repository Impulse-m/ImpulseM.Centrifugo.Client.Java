package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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


    @Test
    public void aCallIsAnsweredOverAPlainConnection() throws Exception {
        answerRpcs("payload");
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(first));
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(second));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(second).getCode());
        awaitAll(Collections.singletonList(first), true);
    }


    @Test
    public void anAffinityGoesOutAsTheConfiguredHeader() throws Exception {
        enableRpcOptions();
        answerRpcs("ok");
        RpcPool pool = build(builder().affinityHeader(AFFINITY_HEADER));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().affinity("hint-1").build(), into(future));
        future.get(5, TimeUnit.SECONDS);
        assertEquals("hint-1", sentRpcRequests().get(0).getHeadersMap().get(AFFINITY_HEADER));
    }


    @Test
    public void anAffinityWithoutTheHeaderOptionIsBadConfiguration() throws Exception {
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().affinity("hint-1").build(), into(future));
        assertEquals(RpcPool.BAD_CONFIGURATION, replyErrorOf(future).getCode());
        assertEquals("nothing is sent for a refused call", 0, sentRpcRequests().size());
    }


    @Test
    public void anEndpointKeyWhichResolvesToNothingIsBadConfiguration() throws Exception {
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.<String>emptyList()).random(maximumDraws()));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void aLaterRefusalOverTheRetriesSurfacesTheError() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() ? laterReply(cmd.getId()) : null;
        RpcPool pool = build(builder().retries(2));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
                return moveReply(cmd.getId(), "eu-2");
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder().reroute(error -> error.getMetadata().get("x-example-relocate")));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals("ok", new String(future.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void aRerouteWhichAlwaysMovesIsBoundedByTheRetries() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() ? moveReply(cmd.getId(), "eu-2") : null;
        RpcPool pool = build(builder().retries(2).reroute(error -> "eu-2"));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        assertEquals(111, replyErrorOf(future).getCode());
        assertEquals("one send and two moves", 3, sentRpcRequests().size());
    }


    @Test
    public void aConnectionClosedForItsMessageSizeSettlesWithoutAResend() throws Exception {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            if ("blob.write".equals(cmd.getRpc().getMethod())) {
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
        pool.rpc("blob.write", EMPTY_PAYLOAD, null, into(future));
        assertTrue("the call fails", failsWithIOException(future));
        synchronized (errorEndpoints) {
            assertEquals("the close is reported before the call settles", 1, errorEndpoints.size());
        }
        CompletableFuture<RPCResult> next = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(next));
        assertEquals("ok", new String(next.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("the failed call is not sent again: the next request is the next call", Arrays.asList("blob.write", "blob.read"), sentMethods());
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
            if ("blob.write".equals(cmd.getRpc().getMethod())) {
                server.disconnect(1009, "message too big");
                return null;
            }
            return rpcReply(cmd.getId(), "ok");
        };
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.write", EMPTY_PAYLOAD, null, into(future));
        assertTrue("the call fails", failsWithIOException(future));
        assertEquals("the call is settled by the close, and sent once", 1, sentRpcRequests().size());
        assertEquals("the close opens no replacement connection", 1, connects.get());
    }


    @Test
    public void aCallOverItsDeadlineIsNotSentAgain() throws Exception {
        server.onCommand = cmd -> cmd.hasRpc() && "blob.read".equals(cmd.getRpc().getMethod()) ? rpcReply(cmd.getId(), "ok") : null;
        server.deferCommand = cmd -> cmd.hasRpc() && "blob.hold".equals(cmd.getRpc().getMethod());
        RpcPool pool = build(builder());
        CompletableFuture<RPCResult> held = new CompletableFuture<>();
        pool.rpc("blob.hold", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(200L).build(), into(held));
        try {
            held.get(5, TimeUnit.SECONDS);
            fail("the call must time out");
        } catch (ExecutionException e) {
            assertTrue("a timeout", e.getCause() instanceof TimeoutException);
        }
        CompletableFuture<RPCResult> next = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(next));
        next.get(5, TimeUnit.SECONDS);
        assertEquals("the timed-out call is not sent again: the next request is the next call", Arrays.asList("blob.hold", "blob.read"), sentMethods());
    }


    @Test
    public void aCancelledCallWaitingForAConnectionIsNeverSent() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        RpcPool pool = build(builder().maxConnectionsPerEndpoint(1).maxQueued(8));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(first));
        RpcPool.Call call = pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(second));
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(onWire));
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(waiting));
        awaitCondition("one call on the wire", () -> pool.stats().getInflight() == 1);
        pool.close();
        assertEquals(ReplyError.CANCELLED, replyErrorOf(waiting).getCode());
        assertTrue("the call on the wire fails with the closed connection", failsWithIOException(onWire));
    }


    @Test
    public void anIdleConnectionIsClosedAfterTheIdleTimeout() throws Exception {
        answerRpcs("ok");
        RpcPool pool = build(builder().idleTimeout(100L));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        future.get(5, TimeUnit.SECONDS);
        awaitCondition("the connection is closed", () -> pool.stats().getConnections() == 0);
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(first));
        first.get(5, TimeUnit.SECONDS);
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(second));
        second.get(5, TimeUnit.SECONDS);
        assertEquals("both calls went over the one connection, which was not closed", 1, connects.get());
        assertEquals(1, pool.stats().getConnections());
    }


    @Test
    public void aQueueWithoutRoomStillOpensAConnectionForACall() throws Exception {
        answerRpcs("ok");
        RpcPool pool = build(builder().maxQueued(0));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(first));
        awaitCondition("the first call on the wire", () -> pool.stats().getInflight() == 1);
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(waiting));
        awaitCondition("the queue of key a is full", () -> pool.stats().getQueued() == 1);
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("a").build(), into(refused));
        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(refused).getCode());
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("b").timeout(5_000L).build(), into(other));
        awaitCondition("a connection is opened for the other key", () -> pool.stats().getConnections() == 2);
        assertTrue("the call of the other key is not refused", !other.isDone());
        awaitAll(Collections.singletonList(first), true);
    }


    @Test
    public void aConnectionOpenedForCallsWhichTimeOutWhileQueuedIsClosedAfterTheIdleTimeout() throws Exception {
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList(UNREACHABLE_URL))
                .random(maximumDraws())
                .idleTimeout(100L));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(50L).build(), into(future));
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
        pool.rpc("blob.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(1_000L).build(), into(future));
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


    private RpcPool.Builder builder() {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .errorListener((endpoint, error) -> {
                });
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
            pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
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
