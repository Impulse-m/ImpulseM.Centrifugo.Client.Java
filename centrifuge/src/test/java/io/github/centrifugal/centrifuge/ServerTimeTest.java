package io.github.centrifugal.centrifuge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * serverTimeMillis() follows the server time carried by replies (Reply.time), and a reply
 * without a server time keeps the last offset. The local clock is injected, so the expected
 * server time is exact.
 */
public class ServerTimeTest extends FakeServerTestBase {
    private static final long LOCAL_MILLIS = 1_700_000_000_000L;
    private static final long SERVER_AHEAD_MILLIS = 60_000L;
    private static final long DAY_MILLIS = 24L * 60L * 60L * 1000L;


    @Test
    public void serverTimeIsLocalTimeBeforeAnyReplyCarriesIt() throws Exception {
        Client c = connectWithFixedClock();

        assertEquals(LOCAL_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void serverTimeFollowsReplyTime() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock();

        rpcAndWait(c);

        assertEquals(LOCAL_MILLIS + SERVER_AHEAD_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void replyWithoutServerTimeKeepsLastOffset() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock();
        rpcAndWait(c);

        answerRpcs("", 0L);
        rpcAndWait(c);

        assertEquals(LOCAL_MILLIS + SERVER_AHEAD_MILLIS, c.serverTimeMillis());
    }


    @Test
    public void lateReplyToATimedOutCallDoesNotSetTheOffset() throws Exception {
        AtomicLong silentId = new AtomicLong();
        server.onCommand = cmd -> {
            if (cmd.hasRpc()) {
                silentId.set(cmd.getId());
            }
            return null;
        };
        Client c = connectWithFixedClock();
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(100).build(), into(future));
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("the silent call must time out");
        } catch (ExecutionException expected) {
            // The call settled with its timeout before the late reply arrives.
        }

        long lateId = silentId.get();
        CompletableFuture<Void> applied = new CompletableFuture<>();
        c.getExecutor().execute(() -> {
            try {
                c.processReply(Protocol.Reply.newBuilder()
                        .setId(lateId)
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


    @Test
    public void serverTimeIgnoresAnImplausibleReplyTime() throws Exception {
        answerRpcs("", LOCAL_MILLIS + SERVER_AHEAD_MILLIS);
        Client c = connectWithFixedClock();
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
        Client c = connectWithFixedClock();
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


    // The client reads the fixed local clock from its first reply on.
    private Client connectWithFixedClock() throws InterruptedException {
        newClient();
        client.wallClock = () -> LOCAL_MILLIS;
        connect(client);
        return client;
    }


    private void rpcAndWait(Client c) throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));
        future.get(5, TimeUnit.SECONDS);
    }
}
