package io.github.centrifugal.centrifuge;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * The ping watchdog. A connection stays open while the server sends replies, and is closed by the second
 * check in a row which finds none. The checks run every half window on the clock, so each test moves the
 * clock from one check to the next.
 */
public class PingWatchdogTest extends RpcTestBase {
    // The kinds of reply which restart the watchdog.
    private enum ReplyKind {
        SERVER_PING,
        PUSH,
        COMMAND_REPLY
    }


    // The ping of the connect reply of the fake server, in seconds.
    private static final int PING_SECONDS = 25;
    // The allowed delay of the server ping which makes the window odd: 25 s + 10.001 s, so the half window is rounded up.
    private static final int ODD_MAX_SERVER_PING_DELAY_MILLIS = 10_001;
    // Checks a test moves through before it fails. A connection closes within three half windows of its last reply.
    private static final int MAX_CHECKS = 10;

    private ManualScheduler clock;
    private Options options;
    private final AtomicInteger messagesSeen = new AtomicInteger();
    private final AtomicInteger commandsAnswered = new AtomicInteger();


    // A reply just before each check keeps the connection open, however many checks follow. The replies cycle
    // through the three kinds.
    @Test
    public void replyJustBeforeEachCheckKeepsTheConnection() throws Exception {
        Client c = connectOnClock();
        for (long check = 1; check <= 6; check++) {
            advanceTo(check * halfWindowMillis() - 1);
            replyOf(ReplyKind.values()[(int) (check % ReplyKind.values().length)]);
            advanceTo(check * halfWindowMillis());
            assertEquals(ClientState.CONNECTED, c.getState());
        }
        assertEquals("no disconnect", 0, disconnectedSignals.availablePermits());
    }


    // With no reply after the last one at L, the connection closes at the second empty check, within the bounds of
    // assertClosedWithinBounds. The last reply is a server ping.
    @Test
    public void silenceClosesAtTheSecondEmptyCheck() throws Exception {
        assertClosedWithinBounds(connectOnClock(), 5_000L, ReplyKind.SERVER_PING);
    }


    // The last reply is a push, so a push alone keeps the watchdog from closing the connection.
    @Test
    public void silenceAfterAPushClosesWithinTheBounds() throws Exception {
        assertClosedWithinBounds(connectOnClock(), 5_000L, ReplyKind.PUSH);
    }


    // The last reply is a command reply, which keeps the watchdog from closing the connection in the same way.
    @Test
    public void silenceAfterACommandReplyClosesWithinTheBounds() throws Exception {
        assertClosedWithinBounds(connectOnClock(), 5_000L, ReplyKind.COMMAND_REPLY);
    }


    // The last reply falls just after a check, the case with the latest close.
    @Test
    public void silenceAfterAReplyJustAfterACheckClosesWithinTheBounds() throws Exception {
        Client c = connectOnClock();
        assertClosedWithinBounds(c, halfWindowMillis() + 1, ReplyKind.SERVER_PING);
    }


    // The same case with an odd window. The half window is rounded up, so the close can fall 1 ms after one and a half
    // windows, and the bound of three half windows still holds.
    @Test
    public void oddWindowAfterAReplyJustAfterACheckClosesWithinTheBounds() throws Exception {
        Client c = connectOnClock(ODD_MAX_SERVER_PING_DELAY_MILLIS);
        assertClosedWithinBounds(c, halfWindowMillis() + 1, ReplyKind.SERVER_PING);
    }


    // A server ping still gets exactly one pong.
    @Test
    public void serverPingStillGetsItsPong() throws Exception {
        connectOnClock();
        int before = server.received().size();
        server.sendPing();
        awaitCondition("the pong to the server ping", () -> server.received().size() > before);
        assertEquals("one pong", before + 1, server.received().size());
        assertEquals(Protocol.Command.getDefaultInstance(), server.received().get(before));
    }


    // A check queued before its connection ended exits when it runs. It closes nothing and adds no check, so the
    // reconnected connection has one check in flight.
    @Test
    public void staleCheckFromEndedConnectionExits() throws Exception {
        Client c = connectOnClock();
        CountDownLatch release = new CountDownLatch(1);
        c.getExecutor().submit(() -> {
            release.await();
            return null;
        });
        clock.advance(halfWindowMillis());
        c.processDisconnect(Client.CONNECTING_TRANSPORT_CLOSED, "transport closed", false);
        assertTrue("the disconnect is reported", disconnectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        c.connect();
        release.countDown();
        assertTrue("the connection is re-established", connectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        drainExecutor();
        assertEquals(ClientState.CONNECTED, c.getState());
        assertEquals("one check in flight", 1, clock.pendingWithDelay(halfWindowMillis()));
    }


    // A check which would close the connection is queued before the connection ends, and runs after the client has
    // reconnected. It finds the generation changed and closes nothing, so the new connection stays open.
    @Test
    public void staleCheckThatWouldCloseLeavesTheNewConnectionOpen() throws Exception {
        Client c = connectOnClock();
        // The first check finds no reply and arms the second one.
        advanceTo(halfWindowMillis());
        CountDownLatch release = new CountDownLatch(1);
        c.getExecutor().submit(() -> {
            release.await();
            return null;
        });
        // The second check would be the second empty one, so it would close the connection, and it queues behind the latch.
        clock.advance(halfWindowMillis());
        c.processDisconnect(Client.CONNECTING_TRANSPORT_CLOSED, "transport closed", false);
        assertTrue("the disconnect is reported", disconnectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        c.connect();
        release.countDown();
        assertTrue("the connection is re-established", connectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        drainExecutor();
        // The new connection runs its own checks and stays open through its first one.
        advanceTo(3 * halfWindowMillis());
        assertEquals(ClientState.CONNECTED, c.getState());
        assertEquals("no disconnect after the reconnect", 0, disconnectedSignals.availablePermits());
    }


    // Connects on the clock with the default allowed delay of the server ping.
    private Client connectOnClock() throws InterruptedException {
        return connectOnClock(instanceOptions().getMaxServerPingDelay());
    }


    // Connects on the clock, with the server confirming pong, so that a server ping gets its pong back. The listener
    // counts the messages, which is how a push is seen to be handled.
    private Client connectOnClock(final int maxServerPingDelayMillis) throws InterruptedException {
        server.connectResult = server.connectResult.toBuilder().setPong(true).build();
        options = instanceOptions();
        options.setMaxServerPingDelay(maxServerPingDelayMillis);
        clock = new ManualScheduler();
        Client c = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client source, ConnectedEvent e) {
                connectedSignals.release();
            }


            @Override
            public void onDisconnected(Client source, DisconnectedEvent e) {
                disconnectedSignals.release();
            }


            @Override
            public void onMessage(Client source, MessageEvent e) {
                messagesSeen.incrementAndGet();
            }
        }, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock, maximumDraws());
        client = c;
        connect(c);
        return c;
    }


    // The watchdog window: the server ping interval plus the allowed delay of the server ping.
    private long windowMillis() {
        return PING_SECONDS * 1000L + options.getMaxServerPingDelay();
    }


    // The half window of the watchdog, from the same two values as the client.
    private long halfWindowMillis() {
        return PingWatchdog.halfWindowMillis(PING_SECONDS * 1000L, options.getMaxServerPingDelay());
    }


    // Sends the last reply at L, then moves check by check until one closes the connection. The close must come no
    // earlier than one window after L, and no later than three half windows after L. A half window of an odd window is
    // rounded up by 1 ms, so three half windows are at most 1 ms past one and a half windows.
    private void assertClosedWithinBounds(final Client c, final long lastReplyMillis, final ReplyKind kind) throws Exception {
        advanceTo(lastReplyMillis);
        replyOf(kind);
        long closedAtMillis = movesUntilClosed(c, lastReplyMillis);
        assertTrue("closed no earlier than one window after the last reply", closedAtMillis >= lastReplyMillis + windowMillis());
        assertTrue("closed within three half windows of the last reply", closedAtMillis <= lastReplyMillis + 3 * halfWindowMillis());
    }


    // Moves check by check after the last reply until one check closes the connection, and returns the time of that
    // check. Fails when no check closes it within MAX_CHECKS checks.
    private long movesUntilClosed(final Client c, final long lastReplyMillis) throws Exception {
        for (long check = 1; check <= MAX_CHECKS; check++) {
            long checkMillis = check * halfWindowMillis();
            if (checkMillis <= lastReplyMillis) {
                continue;
            }
            advanceTo(checkMillis - 1);
            assertEquals("still open before the check", ClientState.CONNECTED, c.getState());
            advanceTo(checkMillis);
            if (c.getState() != ClientState.CONNECTED) {
                return checkMillis;
            }
        }
        throw new AssertionError("no disconnect within " + MAX_CHECKS + " checks");
    }


    // Moves the clock to the given time, and waits until the client has run the checks which fell due. The clock stops
    // at every half window on the way, so each check runs at its own time and schedules the next one from there.
    private void advanceTo(final long timeMillis) throws Exception {
        long halfWindow = halfWindowMillis();
        while (clock.elapsedMillis() < timeMillis) {
            long stop = Math.min(timeMillis, (clock.elapsedMillis() / halfWindow + 1) * halfWindow);
            clock.advance(stop - clock.elapsedMillis());
            drainExecutor();
        }
    }


    // Sends one reply of the kind, and waits until the client has handled it, so that the clock does not move on
    // before the next check could miss it.
    private void replyOf(final ReplyKind kind) throws InterruptedException {
        if (kind == ReplyKind.SERVER_PING) {
            serverPingAnswered();
        } else if (kind == ReplyKind.PUSH) {
            int before = messagesSeen.get();
            server.sendPush(Protocol.Push.newBuilder()
                    .setMessage(Protocol.Message.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("news")).build())
                    .build());
            awaitCondition("the push to be handled", () -> messagesSeen.get() > before);
        } else {
            int before = commandsAnswered.get();
            client.presence("news", (err, res) -> commandsAnswered.incrementAndGet());
            awaitCondition("the command reply to be handled", () -> commandsAnswered.get() > before);
        }
    }


    // Sends a server ping and waits for the pong, which the client sends after it has handled the ping.
    private void serverPingAnswered() throws InterruptedException {
        int before = server.received().size();
        server.sendPing();
        awaitCondition("the pong to the server ping", () -> server.received().size() > before);
    }
}
