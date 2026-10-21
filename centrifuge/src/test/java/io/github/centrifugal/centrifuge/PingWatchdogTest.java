package io.github.centrifugal.centrifuge;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    // The ping of the connect reply of the fake server, in seconds.
    private static final int PING_SECONDS = 25;

    private ManualScheduler clock;
    private Options options;
    private long halfWindowMillis;


    // A reply just before each check keeps the connection open, however many checks follow.
    @Test
    public void replyJustBeforeEachCheckKeepsTheConnection() throws Exception {
        Client c = connectOnClock();
        for (long check = 1; check <= 4; check++) {
            advanceTo(check * halfWindowMillis - 1);
            serverPingAnswered();
            advanceTo(check * halfWindowMillis);
            assertEquals(ClientState.CONNECTED, c.getState());
        }
        assertEquals("no disconnect", 0, disconnectedSignals.availablePermits());
    }


    // With no reply after the last one at L, the connection closes at the second check after L, within [L + window, L + 1.5 × window].
    @Test
    public void silenceClosesAtTheSecondEmptyCheck() throws Exception {
        long lastReplyMillis = 5_000L;
        long windowMillis = windowMillis();
        long closedAtMillis = closedAfterLastReplyAt(lastReplyMillis);
        assertTrue("closed no earlier than one window after the last reply", closedAtMillis >= lastReplyMillis + windowMillis);
        assertTrue("closed within one and a half windows of the last reply", closedAtMillis <= lastReplyMillis + windowMillis + windowMillis / 2);
    }


    // The same bounds hold when the last reply falls just after a check, the case with the latest close.
    @Test
    public void silenceAfterAReplyJustAfterACheckClosesWithinTheBounds() throws Exception {
        long lastReplyMillis = halfWindowMillis + 1;
        long windowMillis = windowMillis();
        long closedAtMillis = closedAfterLastReplyAt(lastReplyMillis);
        assertTrue("closed no earlier than one window after the last reply", closedAtMillis >= lastReplyMillis + windowMillis);
        assertTrue("closed within one and a half windows of the last reply", closedAtMillis <= lastReplyMillis + windowMillis + windowMillis / 2);
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
        clock.advance(halfWindowMillis);
        c.processDisconnect(Client.CONNECTING_TRANSPORT_CLOSED, "transport closed", false);
        assertTrue("the disconnect is reported", disconnectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        c.connect();
        release.countDown();
        assertTrue("the connection is re-established", connectedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        drainExecutor();
        assertEquals(ClientState.CONNECTED, c.getState());
        assertEquals("one check in flight", 1, clock.pendingWithDelay(halfWindowMillis));
    }


    // Connects on the clock, with the server confirming pong, so that a server ping gets its pong back.
    private Client connectOnClock() throws InterruptedException {
        server.connectResult = server.connectResult.toBuilder().setPong(true).build();
        options = instanceOptions();
        clock = new ManualScheduler();
        Client c = newClientOnClock(options, clock);
        connect(c);
        halfWindowMillis = (windowMillis() + 1) / 2;
        return c;
    }


    // The watchdog window: the server ping interval plus the allowed delay of the server ping.
    private static long windowMillis() {
        return PING_SECONDS * 1000L + instanceOptions().getMaxServerPingDelay();
    }


    // Connects, sends the last reply at the given time, then moves check by check until one closes the connection.
    // Returns the time of that check.
    private long closedAfterLastReplyAt(long lastReplyMillis) throws Exception {
        Client c = connectOnClock();
        advanceTo(lastReplyMillis);
        serverPingAnswered();
        for (long check = 1; ; check++) {
            long checkMillis = check * halfWindowMillis;
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
    }


    // Moves the clock to the given time, and waits until the client has run the checks which fell due.
    private void advanceTo(long timeMillis) throws Exception {
        clock.advance(timeMillis - clock.elapsedMillis());
        drainExecutor();
    }


    // Sends a server ping and waits for the pong, which the client sends after it has handled the ping.
    private void serverPingAnswered() throws InterruptedException {
        int before = server.received().size();
        server.sendPing();
        awaitCondition("the pong to the server ping", () -> server.received().size() > before);
    }
}
