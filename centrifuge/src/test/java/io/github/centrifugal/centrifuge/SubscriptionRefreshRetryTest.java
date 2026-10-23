package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * A failed subscription token refresh follows the connection refresh rules: a refusal with an expired token (109) or
 * a client error (below 100) is retried on the refresh backoff, a permission refusal (103) keeps the subscription and
 * is not retried. The retry delay is the connection's formula with the failures of this subscription in a row. The clock
 * and the random draws are the test's.
 */
public class SubscriptionRefreshRetryTest extends RpcTestBase {
    private static final int PERMISSION_DENIED = 103;
    private static final int TOKEN_EXPIRED = 109;
    private static final int CLIENT_ERROR = 42;
    // A refusal in the list below means the server refuses the refresh with that code; zero accepts it.
    private static final int ACCEPTED = 0;
    private static final long TTL_MILLIS = 1000L;
    // The lowest and the highest draw of the full jitter: 0 gives the minimum delay, the highest gives the ceiling.
    private static final double LOWEST_DRAW = 0.0;
    private static final double HIGHEST_DRAW = 1.0 - 1e-12;
    // The first retry ceiling is the maximum of the bounds, 20 s; the second one doubles it, 40 s, so with the
    // highest draw the second retry is 10 s plus 20 s.
    private static final long FIRST_RETRY_AT_HIGHEST_DRAW = 20000L;
    private static final long SECOND_RETRY_AT_HIGHEST_DRAW = 30000L;
    private static final long FIRST_RETRY_AT_LOWEST_DRAW = 10000L;

    private final AtomicInteger refreshRequests = new AtomicInteger();
    private final AtomicInteger refusalsReported = new AtomicInteger();
    private final AtomicInteger tokenFetches = new AtomicInteger();
    // Whether the token getter returns the same token on every fetch. A refused token is then not sent again.
    private boolean sameTokenOnEveryFetch = false;
    // The delays the clock was asked for once the subscription is subscribed: a refusal must add none for a permission refusal.
    private int scheduledAtSubscribe;


    @Test
    public void subscriptionRefreshRetryUsesTheFormula() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        subscribeWith(clock, drawsAt(HIGHEST_DRAW), TOKEN_EXPIRED, TOKEN_EXPIRED);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        awaitRetryArmed(clock, FIRST_RETRY_AT_HIGHEST_DRAW);

        assertEquals("the first retry is the ceiling of its step", FIRST_RETRY_AT_HIGHEST_DRAW, lastDelay(clock));
        clock.advance(FIRST_RETRY_AT_HIGHEST_DRAW);
        awaitRefreshRequests(2);
        awaitRefusals(2);
        awaitRetryArmed(clock, SECOND_RETRY_AT_HIGHEST_DRAW);

        assertEquals("the second consecutive failure doubles the ceiling", SECOND_RETRY_AT_HIGHEST_DRAW, lastDelay(clock));
    }


    @Test
    public void subscriptionRefreshAcceptedResetsTheRetryStep() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        subscribeWith(clock, drawsAt(HIGHEST_DRAW), TOKEN_EXPIRED, TOKEN_EXPIRED, ACCEPTED, TOKEN_EXPIRED);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        awaitRetryArmed(clock, FIRST_RETRY_AT_HIGHEST_DRAW);
        clock.advance(FIRST_RETRY_AT_HIGHEST_DRAW);
        awaitRefreshRequests(2);
        awaitRefusals(2);
        awaitRetryArmed(clock, SECOND_RETRY_AT_HIGHEST_DRAW);
        clock.advance(SECOND_RETRY_AT_HIGHEST_DRAW);
        awaitRefreshRequests(3);
        // The accepted refresh resets the step, so the next failure starts again at the first ceiling.
        clock.advance(TTL_MILLIS);
        awaitRefreshRequests(4);
        awaitRefusals(3);
        awaitRetryArmed(clock, FIRST_RETRY_AT_HIGHEST_DRAW);

        assertEquals(FIRST_RETRY_AT_HIGHEST_DRAW, lastDelay(clock));
    }


    @Test
    public void subscriptionRefreshRefusedWith109IsRetried() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Subscription sub = subscribeWith(clock, drawsAt(LOWEST_DRAW), TOKEN_EXPIRED);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        awaitRetryArmed(clock, FIRST_RETRY_AT_LOWEST_DRAW);

        assertEquals("the retry waits on the refresh backoff", FIRST_RETRY_AT_LOWEST_DRAW, lastDelay(clock));
        clock.advance(FIRST_RETRY_AT_LOWEST_DRAW);
        awaitRefreshRequests(2);
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    @Test
    public void subscriptionRefreshWith103KeepsTheConnection() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Subscription sub = subscribeWith(clock, drawsAt(HIGHEST_DRAW), PERMISSION_DENIED);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        drainExecutor();

        // The refresh command itself arms its reply timeout, and nothing else: no retry delay is scheduled.
        List<Long> armedSinceSubscribe = clock.requestedDelays().subList(scheduledAtSubscribe, clock.requestedDelays().size());
        assertEquals("no retry is armed, only the refresh command's own timeout", Collections.singletonList((long) client.getOpts().getTimeout()), armedSinceSubscribe);
        assertEquals("the refresh is not sent again", 1, refreshRequests.get());
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    @Test
    public void subscriptionRefreshCodeBelow100IsRetried() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Subscription sub = subscribeWith(clock, drawsAt(HIGHEST_DRAW), CLIENT_ERROR);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        awaitRetryArmed(clock, FIRST_RETRY_AT_HIGHEST_DRAW);

        assertEquals(FIRST_RETRY_AT_HIGHEST_DRAW, lastDelay(clock));
        clock.advance(FIRST_RETRY_AT_HIGHEST_DRAW);
        awaitRefreshRequests(2);
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    @Test
    public void subscriptionRefreshDoesNotResendARefusedToken() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        sameTokenOnEveryFetch = true;
        Subscription sub = subscribeWith(clock, drawsAt(HIGHEST_DRAW), TOKEN_EXPIRED);
        clock.advance(TTL_MILLIS);
        awaitRefusals(1);
        awaitRetryArmed(clock, FIRST_RETRY_AT_HIGHEST_DRAW);
        drainExecutor();

        // The getter returns the same token the server refused, so the retry does not send it again and arms no retry.
        clock.advance(FIRST_RETRY_AT_HIGHEST_DRAW);
        drainExecutor();

        assertEquals("the refused token is not sent again", 1, refreshRequests.get());
        // The ping watchdog re-arms its own check inside this advance, so no retry is counted by its delay instead.
        int retriesPending = clock.pendingWithDelay(FIRST_RETRY_AT_HIGHEST_DRAW) + clock.pendingWithDelay(SECOND_RETRY_AT_HIGHEST_DRAW);
        assertEquals("no retry is armed after the refused token", 0, retriesPending);
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    // The draws of the full jitter at a fixed value, for the retry delays of the test.
    private static Random drawsAt(final double draw) {
        return new Random() {
            @Override
            public double nextDouble() {
                return draw;
            }
        };
    }


    private static long lastDelay(ManualScheduler clock) {
        List<Long> delays = clock.requestedDelays();
        return delays.get(delays.size() - 1);
    }


    // A refusal is reported before its retry is scheduled, and both run on the thread of the reply. The test waits
    // until the retry of the given delay is armed, so it reads the delays only after that scheduling.
    private static void awaitRetryArmed(final ManualScheduler clock, final long delayMillis) throws InterruptedException {
        awaitCondition("the retry of " + delayMillis + " ms is armed", () -> clock.pendingWithDelay(delayMillis) == 1);
    }


    private void awaitRefusals(int count) throws Exception {
        awaitCondition("the refresh is refused and reported " + count + " times", () -> refusalsReported.get() == count);
        drainExecutor();
    }


    private void awaitRefreshRequests(int count) throws InterruptedException {
        awaitCondition("the refresh command " + count, () -> refreshRequests.get() == count);
    }


    // Connects a client on the clock and subscribes one channel whose subscribe result expires after one second.
    // The n-th refresh command gets the n-th refusal of the list, and every later one is accepted.
    private Subscription subscribeWith(ManualScheduler clock, Random draws, final int... refusals) throws Exception {
        client = new Client(server.url(), new Options(), new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        }, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock, draws);
        connect(client);
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasSubscribe()) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setSubscribe(Protocol.SubscribeResult.newBuilder().setExpires(true).setTtl(1))
                        .build();
            }
            if (cmd.hasSubRefresh()) {
                int index = refreshRequests.getAndIncrement();
                int code = index < refusals.length ? refusals[index] : ACCEPTED;
                if (code == ACCEPTED) {
                    return Protocol.Reply.newBuilder()
                            .setId(cmd.getId())
                            .setSubRefresh(Protocol.SubRefreshResult.newBuilder().setExpires(true).setTtl(1))
                            .build();
                }
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(code).setMessage("refused"))
                        .build();
            }
            return null;
        };
        SubscriptionOptions subOpts = new SubscriptionOptions();
        subOpts.setTokenGetter(new SubscriptionTokenGetter() {
            @Override
            public void getSubscriptionToken(SubscriptionTokenEvent event, TokenCallback cb) {
                cb.Done(null, sameTokenOnEveryFetch ? "token" : "token-" + tokenFetches.incrementAndGet());
            }
        });
        Subscription sub = client.newSubscription("ch", subOpts, new SubscriptionEventListener() {
            @Override
            public void onError(Subscription s, SubscriptionErrorEvent e) {
                refusalsReported.incrementAndGet();
            }
        });
        sub.subscribe();
        awaitCondition("subscribed", () -> sub.getState() == SubscriptionState.SUBSCRIBED);
        assertTrue("the refresh is due after the ttl", clock.requestedDelays().contains(TTL_MILLIS));
        scheduledAtSubscribe = clock.requestedDelays().size();
        return sub;
    }
}
