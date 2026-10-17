package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.lang.reflect.Field;

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
 * The rpc pool which takes its token from the main client (tokenFrom), on the fake server. The main client takes its token
 * from a token exchange, which the fake server answers with numbered access tokens, or holds a static token. The pool
 * connections live on their own fake servers, so that the connects of the main client and of the pool are told apart.
 */
public class RpcPoolTokenTest extends RpcTestBase {
    private static final String ACCESS_TOKEN = "access-1";
    private static final String METHOD = "exchange";
    private static final int CODE_TOKEN_EXPIRED = 109;
    private static final int CODE_DISCONNECT_UNAUTHORIZED = 1;
    private static final int LONG_TTL = 3600;
    private static final int RETRY_MILLIS = 1000;

    // The ttl of the first token of the main client, in seconds: its later tokens last LONG_TTL.
    private volatile int firstTtl = LONG_TTL;
    // The user the main client holds after its first token.
    private volatile String renewedUser = "u1";
    // Whether a renewal gives the first access token back, as a server does when it accepts the token it refused.
    private volatile boolean renewalRepeatsFirstToken = false;
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


    @Test
    public void poolConnectionsConnectWithTheClientsToken() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builder(main).maxConnectionsPerEndpoint(2));
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        futures.add(submit(pool));
        futures.add(submit(pool));
        awaitCondition("two connections of the pool", () -> pool.stats().getConnections() == 2);
        // The main connection carries the token too, and the two pool connections follow it.
        awaitCondition("three connects", () -> connectTokens().size() == 3);
        List<String> tokens = connectTokens();
        for (String token : tokens) {
            assertEquals(ACCESS_TOKEN, token);
        }
        awaitCondition("every call answered", () -> {
            server.releaseDeferredCommands();
            for (CompletableFuture<RPCResult> future : futures) {
                if (!future.isDone()) {
                    return false;
                }
            }
            return true;
        });
    }


    @Test
    public void aPoolConnectionWaitsForTheMainClientToConnect() throws Exception {
        answerRpcs("payload");
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool pool = build(builder(main));
        CompletableFuture<RPCResult> future = submit(pool);
        // The main client has not connected yet: the pool connection asks for a token and waits for it.
        awaitCondition("the pool connection is opened", () -> pool.stats().getConnections() == 1);
        connect(main);
        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("payload", new String(result.getData(), StandardCharsets.UTF_8));
        List<String> tokens = connectTokens();
        assertEquals(ACCESS_TOKEN, tokens.get(tokens.size() - 1));
    }


    @Test
    public void anAnonymousMainClientGivesAnonymousPoolConnections() throws Exception {
        answerRpcs("payload");
        Client main = clientWithToken("");
        connect(main);
        RpcPool pool = build(builder(main));
        RPCResult result = submit(pool).get(5, TimeUnit.SECONDS);
        assertEquals("payload", new String(result.getData(), StandardCharsets.UTF_8));
        List<String> tokens = connectTokens();
        assertEquals("", tokens.get(tokens.size() - 1));
    }


    @Test
    public void theSourceIsSharedByTheClientsPools() {
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool first = build(builder(main));
        RpcPool second = build(builder(main));
        assertNotNull(first);
        assertNotNull(second);
        assertSame(main.auth.tokenSource(), main.auth.tokenSource());
    }


    @Test
    public void tokenFromIsExclusiveWithAnOwnToken() {
        Client main = clientWithToken(ACCESS_TOKEN);
        assertThrowsIllegalArgument(builder(main).token("other"));
        assertThrowsIllegalArgument(builder(main).tokenGetter(new ConnectionTokenGetter() {
        }));
        assertThrowsIllegalArgument(new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .tokenFrom(main)
                .tokenGetter(new ConnectionTokenGetter() {
                }));
    }


    @Test
    public void tokenFromNeedsAClient() {
        try {
            new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url())).tokenFrom(null);
            fail("a null client is refused");
        } catch (NullPointerException expected) {
            assertTrue(expected.getMessage() != null);
        }
    }


    @Test
    public void aPoolConnectionRefreshedAtItsExpiryTakesTheRenewedTokenOfTheMainClient() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        expiringConnects(endpoint, 1);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        awaitCondition("the pool connection refreshed with the renewed token", () -> refreshTokensOf(endpoint).contains("access-2"));
        // The bootstrap and the renewal the pool asked for: the pool connection makes no exchange of its own.
        assertEquals(2, exchanges.get());
        assertEquals(Collections.singletonList(ACCESS_TOKEN), connectTokensOf(endpoint));
        assertEquals(Collections.singletonList("access-2"), refreshTokensOf(endpoint));
    }


    @Test
    public void twoPoolConnectionsRefusedForTheHeldTokenCauseOneRenewal() throws Exception {
        serveMain();
        FakeCentrifugoServer first = newPoolServer();
        FakeCentrifugoServer second = newPoolServer();
        servePool(first, ACCESS_TOKEN, false);
        servePool(second, ACCESS_TOKEN, false);
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


    @Test
    public void aRefusedTokenTheMainClientNoLongerHoldsIsAnsweredWithItsCurrentToken() throws Exception {
        firstTtl = 1;
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        expiringConnects(endpoint, 3);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        // The main client renews its token on its own timer, before the pool connection's refresh at its expiry.
        awaitCondition("the main client renewed its token", () -> exchanges.get() == 2);
        awaitCondition("the pool connection refreshed", () -> refreshTokensOf(endpoint).contains("access-2"));
        assertEquals("the pool connection needed no renewal of its own", 2, exchanges.get());
    }


    @Test
    public void aFailedTriggeredRenewalKeepsTheWaitersAndTheMainRetryDeliversIt() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        expiringConnects(endpoint, 1);
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, clock);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        storage.failNext.set(true);
        // The refresh of the pool connection at its expiry asks for a renewal, which the storage refuses.
        advanceUntil(clock, "the renewal was attempted and refused", () -> storage.exchanged.size() == 2);
        // The main client's retry is on the clock, and the pool starts no renewal of its own in the meantime.
        clock.advance(RETRY_MILLIS / 2);
        assertEquals(2, storage.exchanged.size());
        assertTrue(refreshTokensOf(endpoint).isEmpty());
        advanceUntil(clock, "the retry of the main client delivered the token", () -> refreshTokensOf(endpoint).contains("access-2"));
        assertEquals("the bootstrap, the refused renewal and its retry", 3, storage.exchanged.size());
        assertEquals("one token, from the retry", 2, exchanges.get());
    }


    @Test
    public void afterDisconnectOfTheMainClientNoConnectionIsOpenedWithItsToken() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        main.disconnect();
        awaitCondition("the main client disconnected", () -> main.getState() == ClientState.DISCONNECTED);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        // The disconnected client lends no token: the connection, which the call waits on, ends as unauthorized and the call
        // settles with that loss, without a resend.
        Throwable error = errorOf(future);
        assertTrue(error instanceof IOException);
        assertEquals("unauthorized", error.getMessage());
        assertTrue("no connect reached the endpoint", connectTokensOf(endpoint).isEmpty());
        assertEquals("no renewal was started", 1, exchanges.get());
    }


    @Test
    public void aLogoutFailsTheCallsAtOnceAndOpensNoConnectionUntilTheMainClientConnectsAgain() throws Exception {
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        expiringConnects(endpoint, 1);
        Storage storage = new Storage("refresh-1");
        Client main = exchangeClient(storage, null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).errorListener((String url, Throwable error) -> poolErrors.add(error)));
        assertEquals("payload", payloadOf(submit(pool)));
        // The next renewal finds no refresh token: the session ends as unauthorized.
        storage.refreshToken = null;
        awaitCondition("the session ended", () -> main.getState() == ClientState.DISCONNECTED);
        Throwable refused = errorOf(submit(pool));
        assertTrue(refused instanceof IOException);
        assertEquals("unauthorized", refused.getMessage());
        assertEquals("no connection was opened after the logout", 1, connectTokensOf(endpoint).size());
        awaitCondition("the waiting connection ended as unauthorized", () -> !poolErrors.isEmpty());
        // A connect of the main client lets the calls through again.
        storage.refreshToken = "refresh-3";
        main.connect();
        awaitCondition("the main client connected again", () -> main.getState() == ClientState.CONNECTED);
        assertEquals("payload", payloadOf(submit(pool)));
    }


    @Test
    public void aRenewalThatReturnsTheRefusedTokenFailsTheWaitingRequestAtOnce() throws Exception {
        renewalRepeatsFirstToken = true;
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, ACCESS_TOKEN, false);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertTrue(errorOf(submit(pool)) instanceof IOException);
        // The renewal gave the refused token back: the request failed on that refresh, and no second renewal followed.
        assertEquals(2, exchanges.get());
    }


    @Test
    public void aStaleConnectionIsRetiredAtItsTokenRequestAndAsksTheMainClientForNothing() throws Exception {
        firstTtl = 1;
        renewedUser = "u2";
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        expiringConnects(endpoint, 2);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertEquals("payload", payloadOf(submit(pool)));
        // The main client moves to the other identity on its own renewal, before the connection's refresh at its expiry.
        awaitCondition("the main client renewed to the other identity", () -> exchanges.get() == 2);
        awaitCondition("the stale connection is retired and closed", () -> pool.stats().getConnections() == 0);
        assertTrue("the stale connection asked the main client for nothing", refreshTokensOf(endpoint).isEmpty());
        assertEquals(2, exchanges.get());
    }


    @Test
    public void anInFlightCallCompletesOnItsIdentityAndTheNextCallOpensANewConnection() throws Exception {
        firstTtl = 1;
        renewedUser = "u2";
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, true);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint).maxConnectionsPerEndpoint(2));
        CompletableFuture<RPCResult> first = submit(pool);
        awaitCondition("the first call is on the wire", () -> rpcCount(endpoint) == 1);
        awaitCondition("the main client moved to the other identity", () -> exchanges.get() == 2);
        CompletableFuture<RPCResult> second = submit(pool);
        awaitCondition("the second call is on the wire too", () -> rpcCount(endpoint) == 2);
        assertEquals(Arrays.asList(ACCESS_TOKEN, "access-2"), connectTokensOf(endpoint));
        // The old connection is held for its call, and the new one for the next.
        assertEquals(2, pool.stats().getConnections());
        endpoint.releaseDeferredCommandsOnTheirSockets();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        awaitCondition("the old connection closed when its call settled", () -> pool.stats().getConnections() == 1);
    }


    @Test
    public void aCallLostWithARetiredConnectionIsNotSentAgain() throws Exception {
        firstTtl = 1;
        renewedUser = "u2";
        serveMain();
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, true);
        expiringConnects(endpoint, 1);
        Client main = exchangeClient(new Storage("refresh-1"), null);
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        CompletableFuture<RPCResult> call = submit(pool);
        awaitCondition("the call is on the wire", () -> rpcCount(endpoint) == 1);
        // The main client moves to the other identity. The connection's refresh at its expiry finds its token stale and retires
        // the connection, which the server then ends with the call still on the wire: the call settles with that loss.
        assertTrue(errorOf(call) instanceof IOException);
        assertEquals("the call was not sent again", 1, rpcCount(endpoint));
        assertEquals("no connection was opened for it", Collections.singletonList(ACCESS_TOKEN), connectTokensOf(endpoint));
        awaitCondition("the retired connection is closed", () -> pool.stats().getConnections() == 0);
    }


    @Test
    public void aStaticTokenRefusalFailsTheConnectionAsUnauthorizedWithoutAGetter() throws Exception {
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, "static-1", false);
        Client main = clientWithToken("static-1");
        connect(main);
        RpcPool pool = build(builderOn(main, endpoint));
        assertTrue(errorOf(submit(pool)) instanceof IOException);
        // Refused at once with no renewal to wait for: the connection is not connected again with a token.
        assertEquals(1, connectTokensOf(endpoint).size());
    }


    @Test
    public void aPoolConnectionThatLeavesWhileWaitingLeavesNoWaitingRequest() throws Exception {
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasConnect();
        Client main = clientWithToken(ACCESS_TOKEN);
        main.connect();
        awaitCondition("the main client is connecting", () -> main.getState() == ClientState.CONNECTING);
        FakeCentrifugoServer endpoint = newPoolServer();
        servePool(endpoint, null, false);
        RpcPool pool = build(builderOn(main, endpoint));
        submit(pool);
        awaitCondition("the pool connection is opened and waits for its token", () -> pool.stats().getConnections() == 1 && waitingCountOf(main) == 1);
        // The pool closes its connection while it waits: the connection leaves, and so does its waiting request.
        pool.close();
        awaitCondition("the waiting request left the token source", () -> waitingCountOf(main) == 0);
        server.releaseDeferredCommands();
        awaitCondition("the main client connected", () -> main.getState() == ClientState.CONNECTED);
        assertEquals("no waiting request is answered after the connect", 0, waitingCountOf(main));
        assertTrue("no connection of the pool was given a token", connectTokensOf(endpoint).isEmpty());
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


    private RpcPool.Builder builderOn(Client main, FakeCentrifugoServer endpoint) {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(endpoint.url()))
                .tokenFrom(main);
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
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        return future;
    }


    private static CompletableFuture<RPCResult> submitTo(RpcPool pool, String endpointKey) {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, RpcPool.CallOptions.builder().endpointKey(endpointKey).build(), into(future));
        return future;
    }


    private static String payloadOf(CompletableFuture<RPCResult> future) throws Exception {
        return new String(future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS).getData(), StandardCharsets.UTF_8);
    }


    // The tokens of the connect commands the server received, in order.
    private List<String> connectTokens() {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                tokens.add(cmd.getConnect().getToken());
            }
        }
        return tokens;
    }


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


    private static int rpcCount(FakeCentrifugoServer endpoint) {
        int count = 0;
        for (Protocol.Command cmd : endpoint.received()) {
            if (cmd.hasRpc()) {
                count++;
            }
        }
        return count;
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
        endpoint.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client")
                .setVersion("0.0.0")
                .setPing(25)
                .setExpires(true)
                .setTtl(ttl)
                .build();
    }


    // The pool's endpoint: refuses a connect with the given token, answers the calls with a payload (or holds them, for the
    // test to release), and answers a refresh with no expiry.
    private static void servePool(FakeCentrifugoServer endpoint, String refusedToken, boolean holdCalls) {
        endpoint.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasConnect() && cmd.getConnect().getToken().equals(refusedToken)) {
                return refusal(cmd, CODE_TOKEN_EXPIRED);
            }
            if (cmd.hasRpc()) {
                return holdCalls ? null : rpcReply(cmd, "payload");
            }
            if (cmd.hasRefresh()) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setRefresh(Protocol.RefreshResult.newBuilder().setExpires(false))
                        .build();
            }
            return null;
        };
        endpoint.deferCommand = holdCalls ? (Protocol.Command cmd) -> cmd.hasRpc() : null;
    }


    // The main client's server: each exchange is numbered, and the access token of the n-th is "access-n". The first
    // token lasts firstTtl seconds, the later ones LONG_TTL, and the identity of the later ones is renewedUser.
    private void serveMain() {
        server.completeClose = true;
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRpc() && METHOD.equals(cmd.getRpc().getMethod())) {
                int number = exchanges.incrementAndGet();
                String access = renewalRepeatsFirstToken && number > 1 ? ACCESS_TOKEN : "access-" + number;
                return rpcReply(cmd, access + "|refresh-" + (number + 1));
            }
            if (cmd.hasRefresh()) {
                boolean first = exchanges.get() <= 1;
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setRefresh(Protocol.RefreshResult.newBuilder()
                                .setUser(first ? "u1" : renewedUser)
                                .setAuthClass("authenticated")
                                .setExpires(true)
                                .setTtl(first ? firstTtl : LONG_TTL))
                        .build();
            }
            return null;
        };
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
