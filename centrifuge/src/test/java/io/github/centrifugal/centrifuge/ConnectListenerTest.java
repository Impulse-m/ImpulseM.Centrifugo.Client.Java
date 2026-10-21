package io.github.centrifugal.centrifuge;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * The connect handshake with the connect listener callbacks. A callback which throws, or throws an Error, is reported
 * after the handshake has written its state, and the callbacks which follow it still run.
 */
public class ConnectListenerTest extends RpcTestBase {
    // The channel which the connect reply subscribes, with one publication recovered on it.
    private static final String CHANNEL = "news";


    // A listener which throws from onConnected leaves the handshake whole: the call queued while disconnected is written,
    // the watchdog and expiry timers are armed when the failure is reported, and the failure is reported after them.
    @Test
    public void listenerFailureInOnConnectedLeavesTheHandshakeWhole() throws Exception {
        enableRpcOptions();
        answerRpcs("{}", System::currentTimeMillis);
        final int ttlSeconds = 60;
        final long expiryMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
        // The watchdog checks every half window, so its first check is armed at half the window.
        final long pingMillis = PingWatchdog.halfWindowMillis(server.connectResult.getPing() * 1000L, instanceOptions().getMaxServerPingDelay());
        server.connectResult = server.connectResult.toBuilder().setExpires(true).setTtl(ttlSeconds).build();
        AtomicBoolean failOnConnect = new AtomicBoolean();
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        BlockingQueue<Integer> expiryArmedAtReport = new LinkedBlockingQueue<>();
        BlockingQueue<Integer> pingArmedAtReport = new LinkedBlockingQueue<>();
        ManualScheduler clock = new ManualScheduler();
        EventListener signalling = signallingListener();
        Client c = new Client(server.url(), instanceOptions(), new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                signalling.onConnected(client, event);
                if (failOnConnect.get()) {
                    throw new IllegalStateException("listener failed on connect");
                }
            }


            @Override
            public void onDisconnected(Client client, DisconnectedEvent event) {
                signalling.onDisconnected(client, event);
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
                expiryArmedAtReport.add(clock.pendingWithDelay(expiryMillis));
                pingArmedAtReport.add(clock.pendingWithDelay(pingMillis));
            }
        }, AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock, maximumDraws());
        client = c;
        connect(c);
        c.disconnect();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(new CompletableFuture<RPCResult>()));
        drainExecutor();

        failOnConnect.set(true);
        c.connect();

        Throwable failure = reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the listener failure is reported", failure instanceof IllegalStateException);
        awaitCondition("the queued call is written", () -> sentRpcRequests().size() == 1);
        assertTrue("the expiry timer is armed when the failure is reported", expiryArmedAtReport.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS) > 0);
        assertTrue("the watchdog timer is armed when the failure is reported", pingArmedAtReport.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS) > 0);
    }


    // An Error from onConnected is reported like a failure: the publication which the connect reply recovers is still delivered.
    @Test
    public void errorFromOnConnectedStillDeliversTheRecoveredPublication() throws Exception {
        enableRpcOptions();
        connectWithRecoveredPublication();
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        BlockingQueue<ServerPublicationEvent> publications = new LinkedBlockingQueue<>();
        Client c = newClient(instanceOptions(), new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                throw new NoClassDefFoundError("listener linkage failed on connect");
            }


            @Override
            public void onPublication(Client client, ServerPublicationEvent event) {
                publications.add(event);
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
            }
        });
        c.connect();

        ServerPublicationEvent publication = publications.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the recovered publication is delivered", publication != null);
        assertEquals(CHANNEL, publication.getChannel());
        assertTrue("the Error is reported", reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS) instanceof NoClassDefFoundError);
    }


    // The call queued for the connect is written before the subscribed and publication callbacks of the same reply run.
    @Test
    public void queuedCallIsWrittenBeforeTheSubscribedAndPublicationCallbacks() throws Exception {
        enableRpcOptions();
        answerRpcs("{}", System::currentTimeMillis);
        connectWithRecoveredPublication();
        BlockingQueue<String> order = new LinkedBlockingQueue<>();
        Client c = newClient(instanceOptions(), new EventListener() {
            @Override
            public void onSubscribed(Client client, ServerSubscribedEvent event) {
                awaitQueuedCallReceived();
                order.add("subscribed");
            }


            @Override
            public void onPublication(Client client, ServerPublicationEvent event) {
                order.add("publication");
            }
        });
        CompletableFuture<RPCResult> queued = new CompletableFuture<>();
        c.rpcCall("queued", EMPTY_PAYLOAD, into(queued));

        c.connect();

        assertEquals("subscribed", order.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        assertEquals("publication", order.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS));
        assertEquals("{}", new String(queued.get(WAIT_MILLIS, TimeUnit.MILLISECONDS).getData(), StandardCharsets.UTF_8));
    }


    // Waits until the server has received the call queued for the connect. A callback which ran before the flush never sees it.
    private void awaitQueuedCallReceived() {
        try {
            awaitCondition("the queued call is written", () -> sentRpcRequests().size() == 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }


    // Makes the connect reply subscribe CHANNEL, with one publication recovered on it. Call after enableRpcOptions.
    private void connectWithRecoveredPublication() {
        Protocol.SubscribeResult subscribed = Protocol.SubscribeResult.newBuilder()
                .setRecoverable(true)
                .setEpoch("epoch")
                .setOffset(1)
                .addPublications(Protocol.Publication.newBuilder()
                        .setOffset(1)
                        .setData(com.google.protobuf.ByteString.copyFromUtf8("{}")))
                .build();
        server.connectResult = server.connectResult.toBuilder().putSubs(CHANNEL, subscribed).build();
    }
}
