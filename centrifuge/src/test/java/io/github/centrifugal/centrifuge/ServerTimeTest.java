package io.github.centrifugal.centrifuge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * serverTimeMillis() follows the server time carried by replies (Reply.time), and a reply
 * without a server time keeps the last offset. The local clock is injected, so the expected
 * server time is exact.
 */
public class ServerTimeTest extends FakeServerTestBase {
    private static final long LOCAL_MILLIS = 1_700_000_000_000L;
    private static final long SERVER_AHEAD_MILLIS = 60_000L;


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
