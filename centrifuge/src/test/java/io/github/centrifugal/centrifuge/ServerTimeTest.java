package io.github.centrifugal.centrifuge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * serverTimeMillis() follows the server time carried by replies (Reply.time), and a reply
 * without a server time keeps the last offset.
 */
public class ServerTimeTest extends FakeServerTestBase {
    private static final long SERVER_AHEAD_MILLIS = 60_000L;


    private static final long TOLERANCE_MILLIS = 5_000L;


    @Test
    public void serverTimeIsLocalTimeBeforeAnyReplyCarriesIt() throws Exception {
        Client c = connectClient();

        assertServerClockAhead(c, 0L);
    }


    @Test
    public void serverTimeFollowsReplyTime() throws Exception {
        answerRpcs("", System.currentTimeMillis() + SERVER_AHEAD_MILLIS);
        Client c = connectClient();

        rpcAndWait(c);

        assertServerClockAhead(c, SERVER_AHEAD_MILLIS);
    }


    @Test
    public void replyWithoutServerTimeKeepsLastOffset() throws Exception {
        answerRpcs("", System.currentTimeMillis() + SERVER_AHEAD_MILLIS);
        Client c = connectClient();
        rpcAndWait(c);

        answerRpcs("", 0L);
        rpcAndWait(c);

        assertServerClockAhead(c, SERVER_AHEAD_MILLIS);
    }


    private void rpcAndWait(Client c) throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));
        future.get(5, TimeUnit.SECONDS);
    }


    private static void assertServerClockAhead(Client c, long expectedMillis) {
        long diff = c.serverTimeMillis() - System.currentTimeMillis();
        assertTrue(
                "server time offset " + diff + " not near " + expectedMillis,
                Math.abs(diff - expectedMillis) < TOLERANCE_MILLIS
        );
    }
}
