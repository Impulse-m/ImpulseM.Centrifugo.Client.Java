package io.github.centrifugal.centrifuge;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Tests for the publish debounce of a subscription. The server returns the window in the subscribe
 * result; the client sends the first publish at once and coalesces the rest of the window into the
 * latest value. The windows run on a {@link ManualScheduler}, so a test moves the clock instead of
 * waiting for real time.
 */
public class PublishDebounceTest extends FakeServerTestBase {

    private static final String CHANNEL = "debounced";
    private static final String MARKER_CHANNEL = "marker";
    private static final String REFUSED_DATA = "refused";
    private static final int DEBOUNCE_MILLIS = 30;
    private static final int FAST_RETRY_MIN_MILLIS = 50;
    private static final int FAST_RETRY_MAX_MILLIS = 100;
    private static final long WAIT_MILLIS = 5000L;
    private static final int PERMISSION_DENIED_CODE = 103;


    private final List<String> published = new CopyOnWriteArrayList<String>();
    private final Semaphore subscribedSignals = new Semaphore(0);
    private ManualScheduler clock;
    private Subscription subscription;


    @Test
    public void firstPublishIsSentAtOnceAndWindowCoalescesTheRest() throws Exception {
        startDebounced(DEBOUNCE_MILLIS);

        CompletableFuture<PublishResult> first = publish("1");
        awaitPublishedCount(1);
        CompletableFuture<PublishResult> second = publish("2");
        CompletableFuture<PublishResult> third = publish("3");

        assertNoError(second);
        assertNoError(third);
        assertEquals(1, published.size());

        clock.advance(DEBOUNCE_MILLIS);
        awaitPublishedCount(2);
        syncWithServer();

        assertEquals(Arrays.asList("1", "3"), published);
        assertNoError(first);
    }


    @Test
    public void idleWindowEndsAndNextPublishIsSentAtOnce() throws Exception {
        startDebounced(DEBOUNCE_MILLIS);

        assertNoError(publish("1"));
        awaitPublishedCount(1);
        clock.advance(DEBOUNCE_MILLIS);
        clock.advance(DEBOUNCE_MILLIS);
        drainExecutor();

        assertEquals("no window is left armed", 0, clock.pendingWithDelay(DEBOUNCE_MILLIS));

        assertNoError(publish("2"));
        awaitPublishedCount(2);
        assertEquals(Arrays.asList("1", "2"), published);
    }


    @Test
    public void unsubscribeDropsPendingValue() throws Exception {
        startDebounced(DEBOUNCE_MILLIS);

        assertNoError(publish("1"));
        awaitPublishedCount(1);
        assertNoError(publish("2"));
        subscription.unsubscribe();
        clock.advance(2 * DEBOUNCE_MILLIS);
        syncWithServer();

        assertEquals(Arrays.asList("1"), published);
    }


    @Test
    public void reconnectDropsPendingValue() throws Exception {
        startDebounced(DEBOUNCE_MILLIS);

        assertNoError(publish("1"));
        awaitPublishedCount(1);
        assertNoError(publish("2"));

        server.closeConnection();
        awaitCondition("the subscription resubscribes after the transport closed",
                () -> subscription.getState() == SubscriptionState.SUBSCRIBING);
        reconnectOnClock();
        assertTrue("resubscribed", subscribedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));

        assertNoError(publish("3"));
        awaitPublishedCount(2);
        syncWithServer();

        assertEquals(Arrays.asList("1", "3"), published);
    }


    @Test
    public void zeroWindowSendsEveryPublish() throws Exception {
        startDebounced(0);

        assertNoError(publish("1"));
        assertNoError(publish("2"));
        assertNoError(publish("3"));
        awaitPublishedCount(3);

        assertEquals(Arrays.asList("1", "2", "3"), published);
        assertFalse("no window is armed", clock.requestedDelays().contains((long) DEBOUNCE_MILLIS));
    }


    @Test
    public void timerDrivenPublishRefusedByServerDoesNotStopTheNextWindow() throws Exception {
        startDebounced(DEBOUNCE_MILLIS);

        assertNoError(publish("1"));
        awaitPublishedCount(1);
        CompletableFuture<PublishResult> refused = publish(REFUSED_DATA);
        clock.advance(DEBOUNCE_MILLIS);
        awaitPublishedCount(2);
        syncWithServer();
        assertNoError(refused);

        assertNoError(publish("3"));
        clock.advance(DEBOUNCE_MILLIS);
        awaitPublishedCount(3);
        syncWithServer();

        assertEquals(Arrays.asList("1", REFUSED_DATA, "3"), published);
        assertEquals(SubscriptionState.SUBSCRIBED, subscription.getState());
    }


    /** Connects a client on the manual clock and subscribes to the debounced channel. */
    private void startDebounced(final int debounceMillis) throws Exception {
        server.onSubscribe = (channel, req) -> Protocol.SubscribeResult.newBuilder()
                .setPublishDebounce(debounceMillis)
                .build();
        server.onCommand = cmd -> {
            if (!cmd.hasPublish()) {
                return null;
            }
            String data = cmd.getPublish().getData().toStringUtf8();
            if (CHANNEL.equals(cmd.getPublish().getChannel())) {
                published.add(data);
            }
            if (REFUSED_DATA.equals(data)) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(PERMISSION_DENIED_CODE).setMessage("refused"))
                        .build();
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setPublish(Protocol.PublishResult.getDefaultInstance())
                    .build();
        };
        clock = new ManualScheduler();
        client = new Client(server.url(), new Options(), new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        }, FAST_RETRY_MIN_MILLIS, FAST_RETRY_MAX_MILLIS, clock);
        connect(client);
        subscription = client.newSubscription(CHANNEL, new SubscriptionEventListener() {
            @Override
            public void onSubscribed(Subscription s, SubscribedEvent e) {
                subscribedSignals.release();
            }
        });
        subscription.subscribe();
        assertTrue("subscribed", subscribedSignals.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS));
    }


    private CompletableFuture<PublishResult> publish(final String data) {
        CompletableFuture<PublishResult> future = new CompletableFuture<PublishResult>();
        subscription.publish(data.getBytes(StandardCharsets.UTF_8), publishInto(future));
        return future;
    }


    private static ResultCallback<PublishResult> publishInto(final CompletableFuture<PublishResult> future) {
        return (error, result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        };
    }


    private static void assertNoError(final CompletableFuture<PublishResult> future) throws Exception {
        future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }


    /**
     * Returns once the server has received the given number of publishes to the debounced channel.
     */
    private void awaitPublishedCount(final int count) throws InterruptedException {
        awaitCondition("published " + count, () -> published.size() >= count);
    }


    /**
     * Returns once the server has handled every command the client wrote before this call. The
     * server handles the commands of a connection in order, so the reply to the marker comes after them.
     */
    private void syncWithServer() throws Exception {
        CompletableFuture<PublishResult> marker = new CompletableFuture<PublishResult>();
        client.publish(MARKER_CHANNEL, EMPTY_PAYLOAD, publishInto(marker));
        marker.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }


    /** Moves the clock until the client reconnects after the server closed the connection. */
    private void reconnectOnClock() throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            clock.advance(FAST_RETRY_MAX_MILLIS);
            if (connectedSignals.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
        fail("timed out waiting for the reconnect");
    }


    private static void awaitCondition(final String description, final BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_MILLIS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + description);
            }
            Thread.sleep(10L);
        }
    }


    private void drainExecutor() throws Exception {
        client.getExecutor().submit(() -> {
        }).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }
}
