package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * serverTimeMillis() follows the server time carried by replies (Reply.time), and a reply
 * without a server time keeps the last offset.
 */
public class ServerTimeTest {

    private static final long SERVER_AHEAD_MILLIS = 60_000L;
    private static final long TOLERANCE_MILLIS = 5_000L;

    private static final byte[] EMPTY_PAYLOAD = "{}".getBytes(StandardCharsets.UTF_8);

    private FakeCentrifugoServer server;
    private Client client;

    @Before
    public void setUp() throws IOException {
        server = new FakeCentrifugoServer();
        server.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (client != null) {
            client.close(1000);
        }
        server.stop();
    }

    // Answers rpcs with an empty result carrying the given server time, 0 meaning unset.
    private void answerRpcsWithTime(long time) {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setTime(time)
                    .setRpc(Protocol.RPCResult.newBuilder())
                    .build();
        };
    }

    private void rpcAndWait() throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, (error, result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    private void assertServerClockAhead(long expectedMillis) {
        long diff = client.serverTimeMillis() - System.currentTimeMillis();
        assertTrue("server time offset " + diff + " not near " + expectedMillis,
                Math.abs(diff - expectedMillis) < TOLERANCE_MILLIS);
    }

    private void connect() throws InterruptedException {
        Semaphore connected = new Semaphore(0);
        client = new Client(server.url(), new Options(), new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connected.release();
            }
        });
        client.connect();
        assertTrue("connected", connected.tryAcquire(5, TimeUnit.SECONDS));
    }

    @Test
    public void serverTimeIsLocalTimeBeforeAnyReplyCarriesIt() throws Exception {
        connect();

        assertServerClockAhead(0L);
    }

    @Test
    public void serverTimeFollowsReplyTime() throws Exception {
        answerRpcsWithTime(System.currentTimeMillis() + SERVER_AHEAD_MILLIS);
        connect();

        rpcAndWait();

        assertServerClockAhead(SERVER_AHEAD_MILLIS);
    }

    @Test
    public void replyWithoutServerTimeKeepsLastOffset() throws Exception {
        answerRpcsWithTime(System.currentTimeMillis() + SERVER_AHEAD_MILLIS);
        connect();
        rpcAndWait();

        answerRpcsWithTime(0L);
        rpcAndWait();

        assertServerClockAhead(SERVER_AHEAD_MILLIS);
    }
}
