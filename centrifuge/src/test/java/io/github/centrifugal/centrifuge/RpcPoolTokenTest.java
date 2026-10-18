package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Test;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * The rpc pool which takes its token from the main client (tokenFrom), on the fake servers. The main client takes its token
 * from a token exchange, which the fake server answers with numbered access tokens, or from a static token. The fake fleet
 * confirms the identity of each token (the users map, or the exchange's identity), so that a pool connection is verified
 * against the identity the main client recorded. The pool connections live on their own fake servers, so that the connects
 * of the main client and of the pool are told apart.
 */
public class RpcPoolTokenTest extends RpcTestBase {
    private static final String ACCESS_TOKEN = "access-1";
    private static final String METHOD = "exchange";
    private static final int CODE_TOKEN_EXPIRED = 109;
    private static final int CODE_PERMISSION_DENIED = 103;
    private static final int CODE_TEMPORARY = 100;
    private static final int LONG_TTL = 3600;
    private static final int RETRY_MILLIS = 1000;
    // The deadline of the calls of the tests: long enough for the waits of a test, and shorter than the wait for an outcome.
    private static final long CALL_TIMEOUT_MILLIS = 2000L;

    // The ttl of the first token of the main client, in seconds: its later tokens last LONG_TTL.
    private volatile int firstTtl = LONG_TTL;
    // The user the exchange gives to the main client after its first token.
    private volatile String renewedUser = "u1";
    // The access tokens the exchange answers, in order; the last one repeats. Null answers "access-n" for the n-th exchange.
    private volatile List<String> issued = null;
    // The code the main client's refresh answers with after the first exchange; 0 for none.
    private volatile int refreshErrorCode = 0;
    // The users the fleet confirms for the tokens it is given, besides the access tokens of the exchange.
    private final Map<String, String> users = new ConcurrentHashMap<>();
    // Whether the main client takes its tokens from the exchange, whose access tokens carry the exchanged identity.
    private volatile boolean exchangeServed = false;
    // The exchanges the fake server answered, which the tests count.
    private final AtomicInteger exchanges = new AtomicInteger();
    private final List<Throwable> poolErrors = new CopyOnWriteArrayList<>();
    private final List<RpcPool> pools = new ArrayList<>();
    private final List<FakeCentrifugoServer> poolServers = new ArrayList<>();


    @After
    public void closePools() {
        for (RpcPool pool : pools) {
            pool.close();
        }
        for (FakeCentrifugoServer poolServer : poolServers) {
            poolServer.stop();
        }
    }


    // T1
    @Test
    public void refusesCallWhileMainDisconnected() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("client disconnected", errorOf(submit(pool)).getMessage());
        assertEquals("the fleet sees no connect", 0, connectTokensOf(endpoint).size());
        assertAllZero(pool);
        connect(main);
        main.disconnect();
        awaitCondition("the main client disconnected", () -> main.getState() == ClientState.DISCONNECTED);
        assertEquals("client disconnected", errorOf(submit(pool)).getMessage());
        assertEquals("the fleet sees no connect", 0, connectTokensOf(endpoint).size());
        assertAllZero(pool);
    }


    // T2
    @Test
    public void refusesCallAfterLogoutUntilConnectAttempt() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        firstTtl = 1;
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        // The next renewal finds no refresh token: the session ends as unauthorized.
        storage.refreshToken = null;
        awaitCondition("the session ended", () -> main.getState() == ClientState.DISCONNECTED);
        assertEquals("unauthorized", errorOf(submit(pool)).getMessage());
        assertEquals("no connection was opened after the logout", 0, connectTokensOf(endpoint).size());
        // A connect attempt holds its reply: the call waits, queued, with no socket.
        storage.refreshToken = "refresh-3";
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        CompletableFuture<RPCResult> future = submit(pool);
        assertEquals("queued", 1, pool.stats().getQueued());
        assertEquals("no socket", 0, connectTokensOf(endpoint).size());
        // The anonymous connect is answered; the exchange then connects with its token, which is held as well.
        server.releaseDeferredCommands();
        awaitCondition("the connect with the exchanged token is held", () -> serverConnects() == 2);
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(future));
    }


    // T3
    @Test
    public void holdsUnboundCallUntilMainConnects() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        Client main = clientWithToken(ACCESS_TOKEN);
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> future = submit(pool);
        assertEquals("no pool socket before the reply", 0, connectTokensOf(endpoint).size());
        assertEquals("the call is queued", 1, pool.stats().getQueued());
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(future));
        assertEquals(Collections.singletonList(ACCESS_TOKEN), connectTokensOf(endpoint));
    }


    // T4
    @Test
    public void bindsUnboundCallToIdentityConfirmedAtConnect() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).idleTimeout(200L));
        assertEquals("payload", payloadOf(submit(pool)));
        main.disconnect();
        main.setToken("bob-token");
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        CompletableFuture<RPCResult> future = submit(pool);
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(future));
        // The alice connection carried the first call only; the bob connection carries the call made after the change.
        assertEquals(Arrays.asList("alice-token", "bob-token"), connectTokensOf(endpoint));
        assertEquals(2, rpcCount(endpoint));
        awaitCondition("the alice connection is closed", () -> pool.stats().getConnections() == 1);
        assertTrue("no error event for the stale connection", poolErrors.isEmpty());
    }


    // T5
    @Test
    public void placesBoundCallOnLiveConnectionWhileMainReconnects() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && rpcCount(endpoint) <= 2;
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxInflightPerConnection(2));
        CompletableFuture<RPCResult> a = submit(pool);
        CompletableFuture<RPCResult> b = submit(pool);
        awaitCondition("two calls on the wire", () -> rpcCount(endpoint) == 2);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        server.closeConnection();
        awaitCondition("the main client is reconnecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        CompletableFuture<RPCResult> c = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(c));
        endpoint.releaseDeferredCommands();
        a.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        b.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals("c is not written before the main connect", 2, rpcCount(endpoint));
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(c));
        assertEquals("no new socket", 1, connectTokensOf(endpoint).size());
    }


    // T6
    @Test
    public void failsQueuedCallWhenIdentityChanges() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> b = submit(pool);
        assertEquals("B is queued", 1, pool.stats().getQueued());
        awaitRefresh(main, "bob-token");
        assertEquals("B fails with the identity change at once", "identity changed", errorOf(b).getMessage());
        assertEquals("B is never written", 1, rpcCount(endpoint));
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        awaitCondition("P1 closed after A", () -> pool.stats().getConnections() == 0);
        CompletableFuture<RPCResult> c = submit(pool);
        endpoint.deferCommand = null;
        assertEquals("payload", payloadOf(c));
        assertEquals(Arrays.asList("alice-token", "bob-token"), connectTokensOf(endpoint));
    }


    // T7
    @Test
    public void doesNotResendCallLostOnStaleConnection() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        awaitRefresh(main, "bob-token");
        endpoint.closeConnection();
        assertEquals("A settles with the loss", "rpc connection retired", errorOf(a).getMessage());
        assertEquals("A is written once", 1, rpcCount(endpoint));
        assertEquals("no connect with alice's token after the change", Collections.singletonList("alice-token"), connectTokensOf(endpoint));
    }


    // T8
    @Test
    public void resendsCallLostWhileMainReconnectsUnderSameIdentity() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && rpcCount(endpoint) <= 1;
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        // The main transport and the pool transport drop together; the main client's reconnect is held.
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        server.closeConnection();
        endpoint.closeConnection();
        awaitCondition("the pool connection reconnects first and A is written again", () -> rpcCount(endpoint) == 2);
        assertEquals("payload", payloadOf(a));
        assertEquals("written twice", 2, rpcCount(endpoint));
        server.releaseDeferredCommands();
    }


    // T9
    @Test
    public void failsResendWhenReconnectConfirmsAnotherIdentity() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        // A's deadline covers the reconnect of the main client, which the test waits for before the loss of A.
        CompletableFuture<RPCResult> a = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(a));
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        // The reconnect carries the token the main client holds when it sends the connect: bob's, set before the transport drops.
        main.setToken("bob-token");
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        server.closeConnection();
        awaitCondition("the main client reconnects", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        endpoint.disconnect(3501, "terminal");
        awaitCondition("A waits for the connect", () -> pool.stats().getQueued() == 1);
        assertEquals("no open while the main client connects", 1, connectTokensOf(endpoint).size());
        server.releaseDeferredCommands();
        assertEquals("identity changed", errorOf(a).getMessage());
        assertEquals("never written on a bob connection", 1, rpcCount(endpoint));
    }


    // T10
    @Test
    public void failsQueuedCallsWhenMainDisconnects() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxInflightPerConnection(1).maxConnectionsPerEndpoint(1));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> b = submit(pool);
        main.disconnect();
        assertEquals("client disconnected", errorOf(b).getMessage());
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        assertEquals("client disconnected", errorOf(submit(pool)).getMessage());
    }


    // T11
    @Test
    public void failsQueuedCallsOnLogout() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        firstTtl = 1;
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> b = submit(pool);
        storage.refreshToken = null;
        assertEquals("unauthorized", errorOf(b).getMessage());
        int written = rpcCount(endpoint);
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        assertEquals("nothing is written after the logout", written, rpcCount(endpoint));
    }


    // T12
    @Test
    public void maxQueuedZeroAdmitsOnlyTheOpener() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        AtomicBoolean holdConnect = new AtomicBoolean(true);
        AtomicBoolean holdCalls = new AtomicBoolean(true);
        endpoint.deferCommand = (Protocol.Command cmd) -> (cmd.hasConnect() && holdConnect.get()) || (cmd.hasRpc() && holdCalls.get());
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxQueued(0));
        CompletableFuture<RPCResult> a = submit(pool);
        CompletableFuture<RPCResult> b = submit(pool);
        assertEquals("the call beyond the opener is refused", "too many rpc calls are waiting", errorOf(b).getMessage());
        assertEquals(1, pool.stats().getConnections());
        assertEquals(0, pool.stats().getInflight());
        assertEquals(1, pool.stats().getQueued());
        holdConnect.set(false);
        endpoint.releaseDeferredCommandsOnTheirSockets();
        awaitCondition("A is on the wire", () -> pool.stats().getInflight() == 1);
        CompletableFuture<RPCResult> b2 = submit(pool);
        assertEquals("too many rpc calls are waiting", errorOf(b2).getMessage());
        holdCalls.set(false);
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        assertEquals("payload", payloadOf(submit(pool)));
    }


    // T13
    @Test
    public void maxQueuedBoundsCallsWaitingForASlot() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && rpcCount(endpoint) <= 1;
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxQueued(2));
        // A's deadline covers the reconnect of its connection, so that its resend is still allowed.
        CompletableFuture<RPCResult> a = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(a));
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> b = submit(pool);
        CompletableFuture<RPCResult> c = submit(pool);
        CompletableFuture<RPCResult> d = submit(pool);
        assertEquals("two calls are queued", 2, pool.stats().getQueued());
        assertEquals("too many rpc calls are waiting", errorOf(d).getMessage());
        endpoint.disconnect(1000, "bye");
        // A is sent again, after its connection reconnects, rather than refused: the queued calls then go through in order.
        awaitCondition("A is written again", () -> rpcCount(endpoint) >= 2);
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        assertEquals("payload", payloadOf(b));
        assertEquals("payload", payloadOf(c));
    }


    // T17
    @Test
    public void lendsOnlyTheConfirmedToken() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken("tA");
        connect(main);
        main.setToken("tX");
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        assertEquals(Collections.singletonList("tA"), connectTokensOf(endpoint));
    }


    // T18
    @Test
    public void retiresConnectionWhoseIdentityDiffersFromTheMain() throws Exception {
        users.put("tA", "other");
        users.put("tB", "");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken("tA");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).errorListener((String url, Throwable error) -> poolErrors.add(error)));
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(CALL_TIMEOUT_MILLIS).build(), into(first));
        awaitCondition("the mismatch is reported", () -> !poolErrors.isEmpty());
        assertEquals("identity mismatch", poolErrors.get(0).getMessage());
        assertEquals("no write on the retired connection", 0, rpcCount(endpoint));
        assertEquals("no second connection", 1, connectTokensOf(endpoint).size());
        assertEquals("the call times out", "rpc call timed out", errorOf(first).getMessage());
        assertEquals("no second connection, no token request", 1, connectTokensOf(endpoint).size());
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(second));
        awaitRefresh(main, "tB");
        assertEquals("payload", payloadOf(second));
        assertEquals(Arrays.asList("tA", "tB"), connectTokensOf(endpoint));
    }


    // T19
    @Test
    public void oneRenewalPerConfirmedTokenPerMainConnection() throws Exception {
        issued = Arrays.asList(ACCESS_TOKEN, "access-2", "access-2");
        serveMain();
        FakeCentrifugoServer first = newPoolServer();
        FakeCentrifugoServer second = newPoolServer();
        serveFleet(first, ACCESS_TOKEN);
        serveFleet(second, ACCESS_TOKEN);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList("a".equals(endpointKey) ? first.url() : second.url()))
                .tokenFrom(main));
        // Both connections are opened, and refused the token, before the one renewal answers them.
        CompletableFuture<RPCResult> a = submitTo(pool, "a");
        CompletableFuture<RPCResult> b = submitTo(pool, "b");
        assertEquals("payload", payloadOf(a));
        assertEquals("payload", payloadOf(b));
        assertEquals("one renewal for both refusals", 2, exchanges.get());
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(first));
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(second));
        // The fleets refuse access-2 from now on, and both connections reconnect with it: the issuer gives access-2 back.
        // Both waiters are refused the token they hold, so they fail as unauthorized, and the one renewal is the third exchange.
        serveFleet(first, "access-2");
        serveFleet(second, "access-2");
        first.closeConnection();
        second.closeConnection();
        awaitCondition("the renewal was answered with the refused token", () -> exchanges.get() == 3);
        awaitCondition("both waiters refused the token", () -> connectTokensOf(first).size() == 3 && connectTokensOf(second).size() == 3);
        // A connection opened afterwards is refused access-2 too, and starts no second renewal.
        FakeCentrifugoServer later = newPoolServer();
        serveFleet(later, "access-2");
        RpcPool laterPool = build(builderOn(main, later));
        CompletableFuture<RPCResult> lateCall = submit(laterPool);
        awaitCondition("the later connection was refused", () -> connectTokensOf(later).size() == 1);
        assertEquals("no second renewal", 3, exchanges.get());
        assertTrue(errorOf(lateCall) != null);
    }


    // T20
    @Test
    public void noRenewalWhileTheMainRefreshIsInProgress() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, "t1");
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger getterCalls = new AtomicInteger();
        server.connectResult = Protocol.ConnectResult.newBuilder().setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setExpires(true).setTtl(1).build();
        serveFleet(server);
        Options options = new Options();
        setScriptedGetter(options, (ConnectionTokenEvent event, TokenCallback cb) -> {
            int call = getterCalls.incrementAndGet();
            if (call == 1) {
                cb.Done(null, "t1");
                return;
            }
            new Thread(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                cb.Done(null, "t2");
            }).start();
        });
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        connect(client);
        awaitCondition("the main refresh is blocked in the getter", () -> getterCalls.get() == 2);
        RpcPool pool = build(builderOn(client, endpoint));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(future));
        awaitCondition("the pool connection is refused", () -> connectTokensOf(endpoint).size() == 1);
        assertEquals("no second getter call for the pool", 2, getterCalls.get());
        gate.countDown();
        assertEquals("payload", payloadOf(future));
        assertEquals("the pool connection took the renewed token", "t2", connectTokensOf(endpoint).get(1));
        assertEquals("no second getter call", 2, getterCalls.get());
    }


    // T21
    @Test
    public void renewalRefusedWith103FailsTheWaiters() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, ACCESS_TOKEN);
        refreshErrorCode = CODE_PERMISSION_DENIED;
        renewedUser = "other";
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).errorListener((String url, Throwable error) -> poolErrors.add(error)));
        CompletableFuture<RPCResult> call = submit(pool);
        awaitCondition("the connection ends as unauthorized", () -> !poolErrors.isEmpty());
        assertEquals("one renewal", 2, exchanges.get());
        assertEquals("the main client keeps its connection", ClientState.CONNECTED, main.getState());
        assertEquals("the call is not sent again", 0, rpcCount(endpoint));
        assertEquals("the connection was not connected again with a token", 1, connectTokensOf(endpoint).size());
        // Open question for the lead, see the summary: the table does not say that a queued call settles when its connection ends.
        assertEquals("the queued call settles at its deadline", "rpc call timed out", errorOf(call).getMessage());
    }


    // T22
    @Test
    public void failedRefreshCallReevaluatesTheWaiters() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, ACCESS_TOKEN);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh() && "tY".equals(cmd.getRefresh().getToken());
        CompletableFuture<AuthResult> applicationRefresh = new CompletableFuture<>();
        main.refresh("tY", (Throwable error, AuthResult result) -> {
            if (error != null) {
                applicationRefresh.completeExceptionally(error);
            } else {
                applicationRefresh.complete(result);
            }
        });
        awaitCondition("the application refresh is held", () -> refreshCountOf("tY") == 1);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> call = submit(pool);
        awaitCondition("the pool connection is refused", () -> connectTokensOf(endpoint).size() == 1);
        assertEquals("no renewal while the call is in flight", 1, exchanges.get());
        int refreshId = commandIdOf("tY");
        server.sendReply(Protocol.Reply.newBuilder().setId(refreshId)
                .setError(Protocol.Error.newBuilder().setCode(CODE_TEMPORARY).setMessage("temporary").setTemporary(true)).build());
        assertTrue(errorOf(applicationRefresh) != null);
        assertEquals("payload", payloadOf(call));
        assertEquals("exactly one renewal", 2, exchanges.get());
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(endpoint));
    }


    // T23
    @Test
    public void failedTimerRenewalLeavesWaitersToTheMainRetry() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, ACCESS_TOKEN);
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, clock);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        storage.failNext.set(true);
        // The call's deadline is real time, the retry is on the manual clock: the deadline covers the clock's steps.
        CompletableFuture<RPCResult> call = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(call));
        advanceUntil(clock, "the renewal was attempted and refused", () -> storage.exchanged.size() == 2);
        clock.advance(RETRY_MILLIS / 2);
        assertEquals(2, storage.exchanged.size());
        advanceUntil(clock, "the retry of the main client delivered the token", () -> connectTokensOf(endpoint).contains("access-2"));
        assertEquals("the bootstrap, the refused renewal and its retry", 3, storage.exchanged.size());
        assertEquals("payload", payloadOf(call));
        assertEquals("one token, from the retry", 2, exchanges.get());
    }


    // T24
    @Test
    public void connectAnswersTheWaiterWithoutAPoolCall() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, "tA");
        AtomicInteger getterCalls = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);
        server.connectResult = Protocol.ConnectResult.newBuilder().setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setExpires(true).setTtl(1).build();
        serveFleet(server);
        Options options = new Options();
        setScriptedGetter(options, (ConnectionTokenEvent event, TokenCallback cb) -> {
            int call = getterCalls.incrementAndGet();
            if (call == 1) {
                cb.Done(null, "tA");
                return;
            }
            if (call >= 3) {
                cb.Done(null, "tB");
                return;
            }
            if (call == 2) {
                new Thread(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    cb.Done(null, "tZ");
                }).start();
            }
        });
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        connect(client);
        awaitCondition("the renewal is in progress", () -> getterCalls.get() == 2);
        RpcPool pool = build(builderOn(client, endpoint));
        CompletableFuture<RPCResult> call = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(WAIT_MILLIS).build(), into(call));
        awaitCondition("the pool connection is refused tA", () -> connectTokensOf(endpoint).size() == 1);
        // The main transport drops: the main client connects again with a new token, which confirms the waiting request.
        client.setToken("tB");
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        server.closeConnection();
        awaitCondition("the main client reconnects", () -> client.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(call));
        assertEquals(Arrays.asList("tA", "tB"), connectTokensOf(endpoint));
        gate.countDown();
    }


    // T25
    @Test
    public void sessionEndDoesNotKeepRefusingAfterAConnectAttempt() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        firstTtl = 1;
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        storage.refreshToken = null;
        awaitCondition("the session ended", () -> main.getState() == ClientState.DISCONNECTED);
        assertEquals("unauthorized", errorOf(submit(pool)).getMessage());
        assertEquals(0, connectTokensOf(endpoint).size());
        storage.refreshToken = "refresh-3";
        main.connect();
        awaitCondition("the main client connected again", () -> main.getState() == ClientState.CONNECTED);
        assertEquals("payload", payloadOf(submit(pool)));
    }


    // T26
    @Test
    public void forgottenWaiterIsAnsweredUnauthorized() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, "tA");
        AtomicInteger getterCalls = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);
        server.connectResult = Protocol.ConnectResult.newBuilder().setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setExpires(true).setTtl(1).build();
        serveFleet(server);
        Options options = new Options();
        setScriptedGetter(options, (ConnectionTokenEvent event, TokenCallback cb) -> {
            int call = getterCalls.incrementAndGet();
            if (call == 1) {
                cb.Done(null, "tA");
                return;
            }
            new Thread(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                cb.Done(null, "tZ");
            }).start();
        });
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        connect(client);
        awaitCondition("the renewal is in progress", () -> getterCalls.get() == 2);
        RpcPool pool = build(builderOn(client, endpoint));
        submit(pool);
        awaitCondition("the pool connection waits for its token", () -> pool.stats().getConnections() == 1 && waitingCountOf(client) == 1);
        pool.close();
        awaitCondition("the waiting request was answered and left", () -> waitingCountOf(client) == 0);
        gate.countDown();
    }


    // T27
    @Test
    public void sourceReferencesOnlyPoolsWithWaitingCalls() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        Client main = clientWithToken(ACCESS_TOKEN);
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        RpcPool idle = build(builderOn(main, endpoint));
        RpcPool waiting = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> call = submit(waiting);
        awaitCondition("the listening set holds the pool with a waiting call", () -> listeningOf(main).equals(Collections.singleton(waiting)));
        long before = idle.keyLookups();
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(call));
        assertTrue("the set is empty after the connect", listeningOf(main).isEmpty());
        assertEquals("the idle pool is never drained", before, idle.keyLookups());
        // A pool closed with a waiting call leaves the set.
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        main.disconnect();
        awaitCondition("the main client disconnected", () -> main.getState() == ClientState.DISCONNECTED);
        main.connect();
        submit(waiting);
        awaitCondition("the call waits again", () -> listeningOf(main).contains(waiting));
        waiting.close();
        awaitCondition("the closed pool left the set", () -> listeningOf(main).isEmpty());
        server.releaseDeferredCommands();
    }


    // T28
    @Test
    public void mainEventDrainsOnlyKeysWithWaitingCalls() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList(endpoint.url())).tokenFrom(main));
        for (int i = 0; i < 1000; i++) {
            RpcPool.CallOptions options = RpcPool.CallOptions.builder().endpointKey("k" + i).build();
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            pool.rpc("svc.read", EMPTY_PAYLOAD, options, into(future));
            payloadOf(future);
        }
        // The pool connection stays live through the main client's disconnect and reconnect, so the call placed at the main connect
        // takes the connection without a connection event of its own: the main event is the only thing that drains the line.
        main.disconnect();
        awaitCondition("the main client disconnected", () -> main.getState() == ClientState.DISCONNECTED);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        awaitHeld(server);
        CompletableFuture<RPCResult> call = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("k999").build(), into(call));
        awaitCondition("the call waits for the main client", () -> pool.stats().getQueued() == 1);
        long before = pool.keyLookups();
        server.releaseDeferredCommands();
        assertEquals("payload", payloadOf(call));
        assertEquals("exactly one key line is drained", before + 1, pool.keyLookups());
    }


    // T29
    @Test
    public void connectionEventDrainsOnlyKeysWithWaitingCalls() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList(endpoint.url())).tokenFrom(main));
        assertEquals("payload", payloadOf(submit(pool)));
        long before = pool.keyLookups();
        for (int i = 0; i < 1000; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey("k" + i).build(), into(future));
            payloadOf(future);
        }
        assertEquals("no key line is looked up", before, pool.keyLookups());
    }


    // T30
    @Test
    public void staleConnectionIsRetiredWhenItReconnects() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        awaitRefresh(main, "bob-token");
        endpoint.closeConnection();
        awaitCondition("the stale connection is retired", () -> pool.stats().getConnections() == 0);
        assertEquals("no connect with alice's token after the change", Collections.singletonList("alice-token"), connectTokensOf(endpoint));
    }


    // T38
    @Test
    public void closeSettlesAndUnlistens() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxInflightPerConnection(1));
        CompletableFuture<RPCResult> inFlight = submit(pool);
        awaitCondition("the call is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> queued = submit(pool);
        awaitCondition("the pool listens with a waiting call", () -> listeningOf(main).equals(Collections.singleton(pool)));
        pool.close();
        assertEquals("rpc pool closed", errorOf(inFlight).getMessage());
        assertEquals("queued call is cancelled", ReplyError.CANCELLED, ((ReplyError) errorOf(queued)).getCode());
        awaitCondition("the pool left the set", () -> listeningOf(main).isEmpty());
        endpoint.releaseDeferredCommandsOnTheirSockets();
    }


    // T39
    @Test
    public void identityChangeRetiresAndCompletesInFlight() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && rpcCount(endpoint) <= 1;
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> a = submit(pool);
        awaitCondition("A is on the wire", () -> rpcCount(endpoint) == 1);
        awaitRefresh(main, "bob-token");
        int written = rpcCount(endpoint);
        assertEquals("payload", payloadOf(submit(pool)));
        assertEquals(Arrays.asList("alice-token", "bob-token"), connectTokensOf(endpoint));
        assertEquals("the new call went to the bob connection", written + 1, rpcCount(endpoint));
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(a));
        awaitCondition("P1 closed when A settled", () -> pool.stats().getConnections() == 1);
    }


    // T40
    @Test
    public void replacedTokenRequestEndsNothing() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, ACCESS_TOKEN);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).errorListener((String url, Throwable error) -> poolErrors.add(error)));
        CompletableFuture<RPCResult> call = submit(pool);
        awaitCondition("the connection asked for its renewal", () -> exchanges.get() == 2 || connectTokensOf(endpoint).size() == 1);
        assertEquals("payload", payloadOf(call));
        assertTrue("no error event for a replaced answer", poolErrors.isEmpty());
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(endpoint));
    }


    // T44
    @Test
    public void resendOfAnOldBindingFailsBeforeItJoinsALine() throws Exception {
        users.put("alice-token", "alice");
        users.put("bob-token", "bob");
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        serveFleet(server);
        endpoint.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc() && rpcCount(endpoint) <= 1;
        Client main = clientWithToken("alice-token");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxInflightPerConnection(1).retries(3));
        CompletableFuture<RPCResult> c = submit(pool);
        awaitCondition("C is on the wire", () -> rpcCount(endpoint) == 1);
        CompletableFuture<RPCResult> b = submit(pool);
        assertEquals("B waits in the line", 1, pool.stats().getQueued());
        awaitRefresh(main, "bob-token");
        assertEquals("identity changed", errorOf(b).getMessage());
        endpoint.releaseDeferredCommandsOnTheirSockets();
        assertEquals("payload", payloadOf(c));
    }


    // T33
    @Test
    public void staticTokenRefusalFailsWithoutAGetter() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, "static-1");
        Client main = clientWithToken("static-1");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> call = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(CALL_TIMEOUT_MILLIS).build(), into(call));
        awaitCondition("the connection ends as unauthorized", () -> !poolErrors.isEmpty());
        assertEquals("the connection is not connected again with a token", 1, connectTokensOf(endpoint).size());
        assertEquals("the call is not sent", 0, rpcCount(endpoint));
        // Open question for the lead, see the summary: the table does not say that a queued call settles when its connection ends.
        assertEquals("the queued call settles at its deadline", "rpc call timed out", errorOf(call).getMessage());
    }


    // Kept item 1: the connections of the pool take the main client's token, from one exchange.
    @Test
    public void connectsWithTheMainTokenAtOneExchange() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        assertEquals("the pool connection makes no exchange of its own", 1, exchanges.get());
        assertEquals(Collections.singletonList(ACCESS_TOKEN), connectTokensOf(endpoint));
    }


    // Kept item 2: a pool connection refreshed at its expiry takes the renewed token of the main client.
    @Test
    public void refreshAtExpiryTakesTheRenewedToken() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        expiringConnects(endpoint, 1);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        awaitCondition("the pool connection refreshed with the renewed token", () -> refreshTokensOf(endpoint).contains("access-2"));
        assertEquals(2, exchanges.get());
        assertEquals(Collections.singletonList(ACCESS_TOKEN), connectTokensOf(endpoint));
        assertEquals(Collections.singletonList("access-2"), refreshTokensOf(endpoint));
    }


    // Kept item 3: two connections refused the held token cause one renewal.
    @Test
    public void twoRefusalsCauseOneRenewal() throws Exception {
        serveMain();
        FakeCentrifugoServer first = newPoolServer();
        FakeCentrifugoServer second = newPoolServer();
        serveFleet(first, ACCESS_TOKEN);
        serveFleet(second, ACCESS_TOKEN);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(new RpcPool.Builder(endpointKey -> Collections.singletonList("a".equals(endpointKey) ? first.url() : second.url()))
                .tokenFrom(main));
        CompletableFuture<RPCResult> a = submitTo(pool, "a");
        CompletableFuture<RPCResult> b = submitTo(pool, "b");
        assertEquals("payload", payloadOf(a));
        assertEquals("payload", payloadOf(b));
        assertEquals("one renewal for both refusals", 2, exchanges.get());
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(first));
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(second));
    }


    // Kept item 4: a refused token the main client no longer holds is answered with its current token.
    @Test
    public void refusedTokenNoLongerHeldIsAnsweredWithTheCurrent() throws Exception {
        firstTtl = 1;
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint, ACCESS_TOKEN);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        awaitCondition("the main client renewed its token", () -> exchanges.get() == 2);
        assertEquals("the pool connection needed no renewal of its own", 2, exchanges.get());
    }


    // Kept item 8, the anonymous main client: its pool connections connect anonymously.
    @Test
    public void anonymousMainGivesAnonymousConnections() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        Client main = clientWithToken("");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        assertEquals(Collections.singletonList(""), connectTokensOf(endpoint));
    }


    // Kept item 5: the configuration of tokenFrom is exclusive with the pool's own token, and needs a client.
    @Test
    public void configurationOfTokenFrom() {
        Client main = clientWithToken(ACCESS_TOKEN);
        assertThrowsIllegalArgument(builderOn(main, server).token("other"));
        assertThrowsIllegalArgument(builderOn(main, server).tokenGetter(new ConnectionTokenGetter() {
        }));
        try {
            new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url())).tokenFrom(null);
            fail("a null client is refused");
        } catch (NullPointerException expected) {
            assertTrue(expected.getMessage() != null);
        }
    }


    // Kept item 6: the pools of one client share one source.
    @Test
    public void poolsOfOneClientShareOneSource() {
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool first = build(builder(main));
        RpcPool second = build(builder(main));
        assertNotNull(first);
        assertNotNull(second);
        assertSame(main.auth.tokenSource(), main.auth.tokenSource());
    }


    // Kept item 7: the token getter of the main client runs once for the connect, and each connection takes the token it is lent.
    @Test
    public void getTokenIsCalledForEachConnection() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        serveFleet(endpoint);
        AtomicInteger getterCalls = new AtomicInteger();
        Options options = new Options();
        setScriptedGetter(options, (ConnectionTokenEvent event, TokenCallback cb) -> {
            getterCalls.incrementAndGet();
            cb.Done(null, ACCESS_TOKEN);
        });
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        connect(client);
        assertEquals(1, getterCalls.get());
        RpcPool pool = build(builderOn(client, endpoint).maxConnectionsPerEndpoint(2));
        CompletableFuture<RPCResult> first = submit(pool);
        CompletableFuture<RPCResult> second = submit(pool);
        assertEquals("payload", payloadOf(first));
        assertEquals("payload", payloadOf(second));
        assertEquals("the pool connections are lent the token, not fetched", 1, getterCalls.get());
    }


    // Sets a token getter which answers with the script: the getter class is abstract, so the script is a lambda of its own.
    private static void setScriptedGetter(Options options, Script script) {
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                script.answer(event, cb);
            }
        });
    }


    private interface Script {
        void answer(ConnectionTokenEvent event, TokenCallback cb);
    }


    private static void assertAllZero(RpcPool pool) {
        RpcPool.Stats stats = pool.stats();
        assertEquals(0, stats.getConnections());
        assertEquals(0, stats.getInflight());
        assertEquals(0, stats.getQueued());
        assertEquals(0, stats.getRedirects());
    }


    private Client clientWithToken(String token) {
        Options options = new Options();
        options.setToken(token);
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        return client;
    }


    // A main client with the token exchange of the storage, on the fake server. A clock, when given, runs its timers.
    private Client exchangeClient(Storage storage, ManualScheduler clock) {
        Options options = new Options();
        options.setTokenExchange(storage);
        EventListener listener = new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        };
        if (clock == null) {
            client = new Client(server.url(), options, listener);
        } else {
            client = new Client(server.url(), options, listener, RETRY_MILLIS, RETRY_MILLIS, clock, new Random());
        }
        return client;
    }


    private RpcPool.Builder builder(Client main) {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .tokenFrom(main);
    }


    // The pool of the tests: one connection per endpoint, a window of one call, and the errors kept for the tests to read.
    private RpcPool.Builder builderOn(Client main, FakeCentrifugoServer endpoint) {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(endpoint.url()))
                .tokenFrom(main)
                .maxConnectionsPerEndpoint(1)
                .maxInflightPerConnection(1)
                .errorListener((String url, Throwable error) -> poolErrors.add(error));
    }


    private RpcPool build(RpcPool.Builder builder) {
        RpcPool pool = builder.build();
        pools.add(pool);
        return pool;
    }


    private FakeCentrifugoServer newPoolServer() throws IOException {
        FakeCentrifugoServer endpoint = new FakeCentrifugoServer();
        // A refused connect reconnects at once only when the server completes the close handshake.
        endpoint.completeClose = true;
        endpoint.start();
        poolServers.add(endpoint);
        return endpoint;
    }


    private static void assertThrowsIllegalArgument(RpcPool.Builder builder) {
        try {
            builder.build();
            fail("the builder refuses an own token with tokenFrom");
        } catch (IllegalArgumentException expected) {
            assertEquals("tokenFrom is exclusive with token and tokenGetter", expected.getMessage());
        }
    }


    private static CompletableFuture<RPCResult> submit(RpcPool pool) {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().timeout(CALL_TIMEOUT_MILLIS).build(), into(future));
        return future;
    }


    private static CompletableFuture<RPCResult> submitTo(RpcPool pool, String endpointKey) {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey(endpointKey).timeout(CALL_TIMEOUT_MILLIS).build(), into(future));
        return future;
    }


    private static String payloadOf(CompletableFuture<RPCResult> future) throws Exception {
        return new String(future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS).getData(), StandardCharsets.UTF_8);
    }


    // The tokens of the connect commands the server received, in order.
    private static List<String> connectTokensOf(FakeCentrifugoServer endpoint) {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasConnect()) {
                tokens.add(cmd.getConnect().getToken());
            }
        }
        return tokens;
    }


    private static List<String> refreshTokensOf(FakeCentrifugoServer endpoint) {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasRefresh()) {
                tokens.add(cmd.getRefresh().getToken());
            }
        }
        return tokens;
    }


    private int refreshCountOf(String token) {
        int count = 0;
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRefresh() && token.equals(cmd.getRefresh().getToken())) {
                count++;
            }
        }
        return count;
    }


    private int commandIdOf(String token) {
        int id = 0;
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRefresh() && token.equals(cmd.getRefresh().getToken())) {
                id = cmd.getId();
            }
        }
        return id;
    }


    // The connect commands the main client's server received.
    private int serverConnects() {
        int count = 0;
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                count++;
            }
        }
        return count;
    }


    private static int rpcCount(FakeCentrifugoServer endpoint) {
        int count = 0;
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasRpc()) {
                count++;
            }
        }
        return count;
    }


    // Refreshes the main client with the token, and waits until the server accepted it.
    private void awaitRefresh(Client main, String token) throws Exception {
        CompletableFuture<AuthResult> refreshed = new CompletableFuture<>();
        main.refresh(token, (Throwable error, AuthResult result) -> {
            if (error != null) {
                refreshed.completeExceptionally(error);
            } else {
                refreshed.complete(result);
            }
        });
        refreshed.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }


    // The number of the waiting requests of the main client's token source, read on the main client's executor.
    private int waitingCountOf(Client main) {
        Callable<Integer> count = () -> waitingCount(main.auth.tokenSource());
        try {
            return main.getExecutor().submit(count).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    private static int waitingCount(TokenSource source) throws Exception {
        Field field = TokenSource.class.getDeclaredField("waiting");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(source)).size();
    }


    // The pools which are listening to the main client, as the source holds them.
    private Set<RpcPool> listeningOf(Client main) {
        try {
            Field field = TokenSource.class.getDeclaredField("listening");
            field.setAccessible(true);
            return Collections.unmodifiableSet(new java.util.HashSet<>((Set<RpcPool>) field.get(main.auth.tokenSource())));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }


    // Moves the manual clock in small steps until the condition holds, as the fake servers answer in real time.
    private static void advanceUntil(ManualScheduler clock, String what, BooleanSupplier condition) throws InterruptedException {
        for (int step = 0; step < 400 && !condition.getAsBoolean(); step++) {
            clock.advance(10L);
            Thread.sleep(10L);
        }
        awaitCondition(what, condition);
    }


    // Makes the pool's endpoint hold its connect ttl: the connection is refreshed at the expiry of its token.
    private static void expiringConnects(FakeCentrifugoServer endpoint, int ttl) {
        endpoint.connectResult = endpoint.connectResult.toBuilder()
                .setExpires(true)
                .setTtl(ttl)
                .build();
    }


    // The connect reply of an anonymous client: no user, and no auth class.
    private static Protocol.ConnectResult anonymousConnect() {
        return Protocol.ConnectResult.newBuilder()
                .setClient("fake-client")
                .setVersion("0.0.0")
                .setPing(25)
                .build();
    }


    // The user the fleet confirms for a token: the one the test gave it, else the identity of the exchange.
    private String userOf(String token) {
        String known = users.get(token);
        if (known != null) {
            return known;
        }
        if (exchangeServed && token.startsWith("access-")) {
            return ACCESS_TOKEN.equals(token) ? "u1" : renewedUser;
        }
        return "";
    }


    // A fleet server, which confirms the identity of each token it is given, refuses the tokens listed with 109, answers the
    // calls with a payload, and accepts the refreshes with the identity of their token.
    private void serveFleet(FakeCentrifugoServer endpoint, String... refused) {
        List<String> refusedTokens = Arrays.asList(refused);
        // A command the test holds is answered when it is released, by the unhandled hook, which gives the same answers.
        endpoint.onUnhandled = (Protocol.Command cmd) -> fleetAnswer(endpoint, refusedTokens, cmd);
        endpoint.onCommand = (Protocol.Command cmd) -> isHeld(endpoint, cmd) ? null : fleetAnswer(endpoint, refusedTokens, cmd);
    }


    private static boolean isHeld(FakeCentrifugoServer endpoint, Protocol.Command cmd) {
        return endpoint.deferCommand != null && endpoint.deferCommand.apply(cmd);
    }


    private Protocol.Reply fleetAnswer(FakeCentrifugoServer endpoint, List<String> refusedTokens, Protocol.Command cmd) {
        {
            if (cmd.hasConnect()) {
                String token = cmd.getConnect().getToken();
                if (refusedTokens.contains(token)) {
                    return refusal(cmd, CODE_TOKEN_EXPIRED);
                }
                String user = userOf(token);
                Protocol.ConnectResult.Builder reply = endpoint.connectResult.toBuilder().setUser(user);
                if (!user.isEmpty()) {
                    reply.setAuthClass("authenticated");
                }
                return Protocol.Reply.newBuilder().setId(cmd.getId()).setConnect(reply.build()).build();
            }
            if (cmd.hasRpc()) {
                return rpcReply(cmd, "payload");
            }
            if (cmd.hasRefresh()) {
                String user = userOf(cmd.getRefresh().getToken());
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setRefresh(Protocol.RefreshResult.newBuilder()
                                .setUser(user)
                                .setAuthClass(user.isEmpty() ? "" : "authenticated")
                                .setExpires(false))
                        .build();
            }
            return null;
        }
    }


    // The main client's server for the token exchange: each exchange is numbered, and the access token of the n-th is "access-n",
    // unless the issued tokens say otherwise. The first token lasts firstTtl seconds, the later ones LONG_TTL, and the identity of
    // the later ones is renewedUser. The refresh with the access token of the exchange is answered with its identity.
    private void serveMain() {
        server.completeClose = true;
        exchangeServed = true;
        server.onUnhandled = (Protocol.Command cmd) -> mainAnswer(cmd);
        server.onCommand = (Protocol.Command cmd) -> isHeld(server, cmd) ? null : mainAnswer(cmd);
    }


    private Protocol.Reply mainAnswer(Protocol.Command cmd) {
        {
            if (cmd.hasConnect()) {
                // An anonymous connect starts the exchange, so its reply has no user; a connect with a token has the exchanged identity.
                Protocol.ConnectResult.Builder reply = cmd.getConnect().getToken().isEmpty()
                        ? anonymousConnect().toBuilder()
                        : anonymousConnect().toBuilder().setUser("u1").setAuthClass("authenticated");
                return Protocol.Reply.newBuilder().setId(cmd.getId()).setConnect(reply.build()).build();
            }
            if (cmd.hasRpc() && METHOD.equals(cmd.getRpc().getMethod())) {
                int number = exchanges.incrementAndGet();
                List<String> answers = issued;
                String access = answers == null ? "access-" + number : answers.get(Math.min(number, answers.size()) - 1);
                return rpcReply(cmd, access + "|refresh-" + (number + 1));
            }
            if (cmd.hasRefresh()) {
                // The application refresh of T22 is held by the test, and answered by it.
                if ("tY".equals(cmd.getRefresh().getToken())) {
                    return null;
                }
                boolean first = exchanges.get() <= 1;
                if (!first && refreshErrorCode != 0) {
                    return Protocol.Reply.newBuilder()
                            .setId(cmd.getId())
                            .setError(Protocol.Error.newBuilder().setCode(refreshErrorCode).setMessage("refused"))
                            .build();
                }
                String user = first ? "u1" : renewedUser;
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setRefresh(Protocol.RefreshResult.newBuilder()
                                .setUser(user)
                                .setAuthClass("authenticated")
                                .setExpires(true)
                                .setTtl(first ? firstTtl : LONG_TTL))
                        .build();
            }
            return null;
        }
    }


    private static Protocol.Reply rpcReply(Protocol.Command cmd, String data) {
        return Protocol.Reply.newBuilder()
                .setId(cmd.getId())
                .setRpc(Protocol.RPCResult.newBuilder().setData(ByteString.copyFromUtf8(data)))
                .build();
    }


    private static Protocol.Reply refusal(Protocol.Command cmd, int code) {
        return Protocol.Reply.newBuilder()
                .setId(cmd.getId())
                .setError(Protocol.Error.newBuilder().setCode(code).setMessage("refused"))
                .build();
    }


    private static TokenExchangeResult parseExchange(RPCResult result) {
        String[] parts = new String(result.getData(), StandardCharsets.UTF_8).split("\\|");
        return new TokenExchangeResult(parts[0], parts.length > 1 ? parts[1] : null);
    }


    // The application's refresh token, and the exchanges it was asked for. The next exchange fails when failNext is set.
    private static final class Storage extends TokenExchange {
        final List<String> exchanged = new CopyOnWriteArrayList<>();
        final AtomicBoolean failNext = new AtomicBoolean();
        volatile String refreshToken;


        Storage(String refreshToken) {
            this.refreshToken = refreshToken;
        }


        @Override
        public String load() throws UnauthorizedException {
            return refreshToken;
        }


        @Override
        public void store(String token) {
            refreshToken = token;
        }


        @Override
        public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
            exchanged.add(event.getRefreshToken());
            if (failNext.getAndSet(false)) {
                cb.onDone(new IOException("exchange failed"), null);
                return;
            }
            event.rpc(METHOD, new byte[0], (Throwable err, RPCResult result) -> {
                if (err != null) {
                    cb.onDone(err, null);
                    return;
                }
                cb.onDone(null, parseExchange(result));
            });
        }
    }
}
