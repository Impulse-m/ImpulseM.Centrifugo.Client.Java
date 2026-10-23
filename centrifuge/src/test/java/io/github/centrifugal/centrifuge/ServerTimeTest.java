package io.github.centrifugal.centrifuge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * serverTimeMillis() follows the server time carried by replies (Reply.time), and a reply
 * without a server time keeps the last offset. The local clock is injected, so the expected
 * server time is exact.
 */
public class ServerTimeTest extends RpcTestBase {
    private static final long LOCAL_MILLIS = 1_700_000_000_000L;
    private static final long SERVER_AHEAD_MILLIS = 60_000L;
    private static final long DAY_MILLIS = 24L * 60L * 60L * 1000L;


    @Test
    public void serverTimeIsLocalTimeBeforeAnyReplyCarriesIt() throws Exception {
        Client c = connectWithFixedClock(new Options());

        assertEquals(LOCAL_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void serverTimeFollowsReplyTime() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock(new Options());

        rpcAndWait(c);

        assertEquals(LOCAL_MILLIS + SERVER_AHEAD_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void replyWithoutServerTimeKeepsLastOffset() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock(new Options());
        rpcAndWait(c);

        answerRpcs("", 0L);
        rpcAndWait(c);

        assertEquals(LOCAL_MILLIS + SERVER_AHEAD_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void lateReplyToATimedOutCallDoesNotSetTheOffset() throws Exception {
        AtomicInteger silentId = new AtomicInteger();
        holdRpc(silentId);
        Client c = connectWithFixedClock(new Options());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));
        awaitCondition("the silent rpc to reach the server", () -> silentId.get() > 0);
        assertTimedOut(future);

        injectLateReply(c, silentId.get(), LOCAL_MILLIS + SERVER_AHEAD_MILLIS);

        assertEquals(LOCAL_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void lateReplyToATrackedTimedOutCallDoesNotSetTheOffset() throws Exception {
        enableRpcOptions();
        AtomicInteger silentId = new AtomicInteger();
        holdRpc(silentId);
        Client c = connectWithFixedClock(instanceOptions());
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));
        awaitCondition("the silent tracked rpc to reach the server", () -> silentId.get() > 0);
        assertTimedOut(future);

        injectLateReply(c, silentId.get(), LOCAL_MILLIS + SERVER_AHEAD_MILLIS);

        assertEquals(LOCAL_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void serverTimeIgnoresAnImplausibleReplyTime() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock(new Options());
        rpcAndWait(c);
        long[] implausible = {Long.MAX_VALUE, Long.MIN_VALUE, LOCAL_MILLIS + 400L * DAY_MILLIS};

        for (long time : implausible) {
            answerRpcs("", time);
            rpcAndWait(c);

            assertEquals(LOCAL_MILLIS + SERVER_AHEAD_MILLIS, c.serverTimeMillis());
        }
    }


    @Test
    public void serverTimeIsTakenOnlyFromReplies() throws Exception {
        Client c = connectWithFixedClock(new Options());
        CompletableFuture<Void> applied = new CompletableFuture<>();
        c.getExecutor().execute(() -> {
            try {
                // A reply without a command id is a server ping or a push: it never sets the offset.
                c.processReply(Protocol.Reply.newBuilder()
                        .setTime(LOCAL_MILLIS + SERVER_AHEAD_MILLIS)
                        .build());
                applied.complete(null);
            } catch (Exception e) {
                applied.completeExceptionally(e);
            }
        });
        applied.get(5, TimeUnit.SECONDS);

        assertEquals(LOCAL_MILLIS, c.serverTimeMillis());
    }


    // Holds every rpc on the fake server, and records the command id of the last held one.
    private void holdRpc(AtomicInteger silentId) {
        server.onCommand = cmd -> {
            if (cmd.hasRpc()) {
                silentId.set(cmd.getId());
            }
            return null;
        };
        // The fake server answers any command it does not hold, so the rpc is held to stay silent.
        server.deferCommand = cmd -> cmd.hasRpc();
    }


    // The client reads the fixed local clock from its first reply on.
    private Client connectWithFixedClock(Options options) throws InterruptedException {
        Client c = newClient(options);
        c.wallClock = () -> LOCAL_MILLIS;
        connect(c);
        return c;
    }


    private static void assertTimedOut(CompletableFuture<RPCResult> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("the silent call must time out");
        } catch (ExecutionException expected) {
            assertTrue("the call timed out", expected.getCause() instanceof TimeoutException);
        }
    }


    // Delivers a reply to the client on its executor, as the transport would, and waits until it is applied.
    private static void injectLateReply(Client c, int id, long time) throws Exception {
        CompletableFuture<Void> applied = new CompletableFuture<>();
        c.getExecutor().execute(() -> {
            try {
                c.processReply(Protocol.Reply.newBuilder()
                        .setId(id)
                        .setTime(time)
                        .build());
                applied.complete(null);
            } catch (Exception e) {
                applied.completeExceptionally(e);
            }
        });
        applied.get(5, TimeUnit.SECONDS);
    }


    private void rpcAndWait(Client c) throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));
        future.get(5, TimeUnit.SECONDS);
    }
}
