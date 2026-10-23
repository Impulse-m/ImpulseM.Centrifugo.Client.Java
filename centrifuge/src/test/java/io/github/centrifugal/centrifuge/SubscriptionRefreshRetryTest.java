package io.github.centrifugal.centrifuge;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * A failed subscription token refresh follows the connection refresh rules: an expired token (109) is retried on the
 * refresh backoff, a permission refusal (103) keeps the subscription and is not retried. The clock is the test's.
 */
public class SubscriptionRefreshRetryTest extends RpcTestBase {
    private static final int PERMISSION_DENIED = 103;
    private static final int TOKEN_EXPIRED = 109;
    private static final long TTL_MILLIS = 1000L;

    private final AtomicInteger refreshRequests = new AtomicInteger();
    private final AtomicInteger refusalsReported = new AtomicInteger();
    // The delays the clock was asked for once the subscription is subscribed: a refusal must add none for a permission refusal.
    private int scheduledAtSubscribe;


    @Test
    public void subscriptionRefreshWithAnExpiredTokenIsRetriedOnTheBackoff() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Subscription sub = subscribeWith(clock, TOKEN_EXPIRED);
        clock.advance(TTL_MILLIS);
        awaitCondition("the refresh is refused and reported", () -> refusalsReported.get() == 1);
        drainExecutor();

        long retryDelay = clock.requestedDelays().get(clock.requestedDelays().size() - 1);
        assertTrue("the retry waits on the refresh backoff: " + retryDelay,
                retryDelay >= AuthRefresh.RETRY_MIN_MILLIS && retryDelay <= AuthRefresh.RETRY_MAX_MILLIS);
        clock.advance(retryDelay);
        awaitCondition("the retry refreshes again", () -> refreshRequests.get() == 2);
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    @Test
    public void subscriptionRefreshRefusedWithPermissionDeniedKeepsTheSubscription() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Subscription sub = subscribeWith(clock, PERMISSION_DENIED);
        clock.advance(TTL_MILLIS);
        awaitCondition("the refresh is refused and reported", () -> refusalsReported.get() == 1);
        drainExecutor();

        assertEquals("no retry is armed", scheduledAtSubscribe, clock.requestedDelays().size());
        assertEquals("the refresh is not sent again", 1, refreshRequests.get());
        assertEquals(SubscriptionState.SUBSCRIBED, sub.getState());
    }


    // Connects a client on the clock and subscribes one channel whose subscribe result expires after one second.
    private Subscription subscribeWith(ManualScheduler clock, final int refusalCode) throws Exception {
        client = new Client(server.url(), new Options(), new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        }, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock, new Random());
        connect(client);
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasSubscribe()) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setSubscribe(Protocol.SubscribeResult.newBuilder().setExpires(true).setTtl(1))
                        .build();
            }
            if (cmd.hasSubRefresh()) {
                refreshRequests.incrementAndGet();
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(refusalCode).setMessage("refused"))
                        .build();
            }
            return null;
        };
        SubscriptionOptions subOpts = new SubscriptionOptions();
        subOpts.setTokenGetter(new SubscriptionTokenGetter() {
            @Override
            public void getSubscriptionToken(SubscriptionTokenEvent event, TokenCallback cb) {
                cb.Done(null, "token");
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
