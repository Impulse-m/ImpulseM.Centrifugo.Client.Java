package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletionException;

import okhttp3.WebSocket;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * The token exchange: the anonymous connect is completed over the same socket, the access token is kept
 * and renewed by the client, and the refresh token is read and stored by the application. The app's
 * exchange is a plain rpc to the fake server, which answers "access|rotated" (or "access" when the token did
 * not rotate); the refresh command of the bootstrap or the renewal is answered by the fake server's refresh
 * result.
 */
public class TokenExchangeTest extends RpcTestBase {
    private static final String METHOD = "exchange";
    private static final String ECHO = "echo";
    private static final int TTL_SECONDS = 10;
    private static final int RETRY_MIN_MILLIS = 50;
    private static final int RETRY_MAX_MILLIS = 100;
    // The smallest delay of a bootstrap retry: the first backoff step is [min, 2 * min].
    private static final long BOOTSTRAP_RETRY_MIN_MILLIS = 5000L;
    private static final int CODE_DISCONNECT_UNAUTHORIZED = 1;
    private static final int CODE_PERMISSION_DENIED = 103;
    private static final int CODE_TOKEN_EXPIRED = 109;
    private static final long WINDOW_MIN_MILLIS = 7000L;
    private static final long WINDOW_MAX_MILLIS = 9000L;
    // The clock moves at most this far per test, so that the ping timer (25 s plus the ping delay) stays idle.
    private static final int PUMP_STEPS = 300;
    private static final int PUMP_STEP_MILLIS = 100;


    private final List<ConnectedEvent> connected = new CopyOnWriteArrayList<>();
    private final List<AuthEvent> auths = new CopyOnWriteArrayList<>();
    private final List<Throwable> errors = new CopyOnWriteArrayList<>();
    private final List<DisconnectedEvent> disconnects = new CopyOnWriteArrayList<>();
    private final List<String> order = new CopyOnWriteArrayList<>();
    // The replies of the fake server to the exchange and to the refresh command, set by answerExchanges and answerIdentity.
    private final Map<String, byte[]> rpcReplies = new ConcurrentHashMap<>();
    private volatile Protocol.RefreshResult refreshResult = Protocol.RefreshResult.getDefaultInstance();


    @Before
    public void answerExchangeCommands() {
        server.completeClose = true;
        server.onUnhandled = this::answerExchangeCommand;
    }


    // The application's storage and exchange: the refresh token it holds, what it stored, and each refresh token
    // the exchange was asked for. The next exchange fails with an IOException when failNext is set.
    private static class Storage extends TokenExchange {
        final List<String> exchanged = new CopyOnWriteArrayList<>();
        final List<String> stored = new CopyOnWriteArrayList<>();
        final AtomicBoolean failNext = new AtomicBoolean();
        volatile String refreshToken;


        Storage(String refreshToken) {
            this.refreshToken = refreshToken;
        }


        @Override
        public String load() {
            return refreshToken;
        }


        @Override
        public void store(String token) {
            stored.add(token);
            refreshToken = token;
        }


        @Override
        public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
            exchanged.add(event.getRefreshToken());
            if (failNext.getAndSet(false)) {
                cb.onDone(new IOException("exchange failed"), null);
                return;
            }
            event.rpc(METHOD, event.getRefreshToken().getBytes(StandardCharsets.UTF_8), (Throwable err, RPCResult result) -> {
                if (err != null) {
                    cb.onDone(err, null);
                    return;
                }
                cb.onDone(null, parseExchange(result));
            });
        }
    }


    // An exchange whose callback the test completes itself: the one numbered stallOn (1 is the first exchange, the
    // bootstrap's), after the connection or the timer changed.
    private static final class StallingStorage extends Storage {
        volatile ResultCallback<TokenExchangeResult> held;
        private final int stallOn;


        StallingStorage(String refreshToken) {
            this(refreshToken, 1);
        }


        StallingStorage(String refreshToken, int stallOn) {
            super(refreshToken);
            this.stallOn = stallOn;
        }


        @Override
        public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
            if (held == null && exchanged.size() == stallOn - 1) {
                exchanged.add(event.getRefreshToken());
                held = cb;
                return;
            }
            super.exchange(event, cb);
        }
    }


    @Test
    public void bootstrapCompletesTheAnonymousConnectOverTheSameSocket() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("connect", "rpc:" + METHOD, "refresh"), kinds());
        assertEquals("", connectTokens().get(0));
        assertEquals("access-1", lastRefreshToken());
        assertEquals("u1", connected.get(0).getUser());
        assertEquals("authenticated", connected.get(0).getAuthClass());
        assertEquals(1, auths.size());
        assertEquals(Arrays.asList("refresh-1"), storage.exchanged);
        assertTrue(errors.isEmpty());
    }


    @Test
    public void rotatedRefreshTokenIsStoredBeforeTheRefreshCommand() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRefresh()) {
                log.add("refresh");
            }
            return null;
        };
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void store(String token) {
                log.add("store:" + token);
                super.store(token);
            }
        };
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("store:refresh-2", "refresh"), log);
    }


    @Test
    public void storeIsNotCalledWithoutARotation() throws Exception {
        answerExchanges("access-1");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals("access-1", lastRefreshToken());
        assertTrue(storage.stored.isEmpty());
    }


    @Test
    public void storeIsNotCalledForTheSameRefreshToken() throws Exception {
        answerExchanges("access-1|refresh-1");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertTrue(storage.stored.isEmpty());
    }


    @Test
    public void keptAccessTokenReconnectsWithoutAnExchange() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        // The server identifies a connect which carries a valid token, so its reply carries the user.
        server.connectResult = server.connectResult.toBuilder()
                .setUser("u1")
                .setAuthClass("authenticated")
                .build();

        server.closeConnection();

        awaitCondition("reconnected", () -> connected.size() == 2);
        assertEquals("access-1", connectTokens().get(connectTokens().size() - 1));
        assertEquals(Arrays.asList("connect", "rpc:" + METHOD, "refresh", "connect"), kinds());
        assertEquals(1, storage.exchanged.size());
    }


    @Test
    public void anonymousReplyToAConnectWithATokenBootstraps() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Options options = exchangeOptions(storage);
        options.setToken("cached-token");
        newClient(options);

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("connect", "rpc:" + METHOD, "refresh"), kinds());
        assertEquals("cached-token", connectTokens().get(0));
        assertEquals("access-1", lastRefreshToken());
        assertEquals("u1", connected.get(0).getUser());
        assertEquals(Arrays.asList("refresh-1"), storage.exchanged);
        assertTrue(errors.isEmpty());
    }


    @Test
    public void connectReplyWithAUserIsNotBootstrapped() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        server.connectResult = server.connectResult.toBuilder()
                .setUser("proxy-user")
                .setAuthClass("authenticated")
                .build();
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("connect"), kinds());
        assertEquals("proxy-user", connected.get(0).getUser());
        assertEquals(1, auths.size());
        assertTrue(storage.exchanged.isEmpty());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void expiredAccessTokenBootstrapsAgainOnReconnect() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, 1);
        Options options = exchangeOptions(storage);
        // The reconnect comes after the token's one second expired, and the renewal timer is cancelled by the close.
        options.setMinReconnectDelay(1500);
        options.setMaxReconnectDelay(2000);
        newClient(options);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);

        server.closeConnection();

        awaitCondition("reconnected", () -> connected.size() == 2);
        assertEquals("", connectTokens().get(connectTokens().size() - 1));
        assertEquals(2, storage.exchanged.size());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void connectRefusedWithTokenExpiredBootstrapsAgain() throws Exception {
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasConnect() && !cmd.getConnect().getToken().isEmpty()) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(CODE_TOKEN_EXPIRED).setMessage("expired"))
                        .build();
            }
            return null;
        };
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);

        server.closeConnection();

        awaitCondition("connected again", () -> connected.size() == 2);
        assertEquals(2, storage.exchanged.size());
        assertEquals(1, errors.size());
        assertEquals(CODE_TOKEN_EXPIRED, ((ReplyError) errors.get(0)).getCode());
        assertTrue(disconnects.isEmpty());
    }


    @Test
    public void renewalFiresInsideTheWindowAndRotatesTheTokens() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1");
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        long delay = renewalDelay(clock);
        assertTrue("renewal delay " + delay, delay >= WINDOW_MIN_MILLIS && delay <= WINDOW_MAX_MILLIS);

        answerExchanges("access-2|refresh-3");
        clock.advance(delay - 200);
        drainExecutor();
        assertEquals("no renewal before the window", 1, storage.exchanged.size());
        clock.advance(300);
        awaitCondition("renewed", () -> refreshTokens().size() == 2);

        assertEquals(Arrays.asList("access-1", "access-2"), refreshTokens());
        assertEquals(Arrays.asList("refresh-2", "refresh-3"), storage.stored);
        assertEquals(1, auths.size());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void renewalDelayStaysInItsWindowOverTwentyRuns() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        for (int run = 0; run < 20; run++) {
            ManualScheduler clock = new ManualScheduler();
            newExchangeClient(exchangeOptions(new Storage("refresh-1")), clock);
            client.connect();
            final int expected = run + 1;
            awaitCondition("connected", () -> connected.size() == expected);
            long delay = renewalDelay(clock);
            assertTrue("renewal delay " + delay, delay >= WINDOW_MIN_MILLIS && delay <= WINDOW_MAX_MILLIS);
            client.close(1000);
        }
    }


    @Test
    public void unchangedRefreshTokenIsRetriedUntilTheGraceHorizon() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        // A refusal inside the grace is retried, and is not the verdict.
        awaitCondition("first refusal reported", () -> !errors.isEmpty());
        assertTrue(disconnects.isEmpty());
        pumpUntil(clock, "disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
        assertEquals(ClientState.DISCONNECTED, client.getState());
        assertTrue(storage.stored.isEmpty());
        // Each attempt presented the stored token, and the attempts stayed within the grace.
        assertTrue("attempts " + storage.exchanged.size(), storage.exchanged.size() >= 2);
        assertTrue("attempts " + storage.exchanged.size(), storage.exchanged.size() <= 30);
        for (String token : storage.exchanged) {
            assertEquals("refresh-1", token);
        }
        // The refusal reports name no token.
        assertFalse(errors.isEmpty());
        for (Throwable error : errors) {
            assertFalse(String.valueOf(error.getMessage()).contains("refresh-1"));
            assertFalse(String.valueOf(error.getCause()).contains("refresh-1"));
        }
        clock.advance(60000);
        assertEquals("no reconnect", 1, kinds().stream().filter("connect"::equals).count());
    }


    @Test
    public void wrappedUnauthorizedAtBootstrapIsRetriedWithinTheGrace() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new CompletionException(new UnauthorizedException()), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        // The wrapped refusal is recognized: it opens the run and is retried, instead of being an ordinary error.
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        assertTrue(disconnects.isEmpty());
        assertEquals("refresh-1", storage.exchanged.get(0));
        pumpUntil(clock, "disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
        assertEquals(ClientState.DISCONNECTED, client.getState());
        assertTrue(storage.stored.isEmpty());
        clock.advance(60000);
        assertEquals("no reconnect", 1, kinds().stream().filter("connect"::equals).count());
    }


    @Test
    public void emptyRefreshTokenAfterARefusalIsTerminalAtOnce() throws Exception {
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                // The application removed its token, as on logout, while the exchange was refused.
                refreshToken = null;
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), new ManualScheduler());

        client.connect();

        awaitCondition("disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
        assertEquals(1, storage.exchanged.size());
        assertTrue(storage.stored.isEmpty());
    }


    @Test
    public void graceOfTheResultSetsTheHorizonAndIsClamped() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerIdentity(false, 0);
        AtomicInteger calls = new AtomicInteger();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                if (calls.incrementAndGet() == 1) {
                    cb.onDone(null, new TokenExchangeResult("access-1", "refresh-2", 1000));
                    return;
                }
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);

        // The transport is lost: the reconnect is refused, and its run takes the grace the accepted result published.
        server.closeConnection();
        pumpUntil(clock, "horizon of the published grace", () -> clock.requestedDelays().contains(300000L));
        assertEquals(Arrays.asList("refresh-1", "refresh-2"), storage.exchanged.subList(0, 2));
    }


    @Test
    public void graceOfTheOptionIsUsedUntilTheIssuerPublishesOne() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public int grace() {
                return 7;
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        awaitCondition("horizon of the option", () -> clock.requestedDelays().contains(7000L));
    }


    @Test
    public void horizonIsTimedOnTheSchedulerClock() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public int grace() {
                return 5;
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        assertTrue(clock.requestedDelays().contains(5000L));

        // The verdict waits for the scheduler's clock to pass the grace; no wall clock step moves it.
        for (int step = 0; step < 4; step++) {
            clock.advance(1000);
            drainExecutor();
        }
        drainExecutor();
        assertTrue(disconnects.isEmpty());
        pumpUntil(clock, "disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
    }


    @Test
    public void renewalRefusalWithinTheGraceRidesTheFetchRetry() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        AtomicBoolean refusing = new AtomicBoolean();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                if (refusing.get()) {
                    exchanged.add(event.getRefreshToken());
                    cb.onDone(new UnauthorizedException(), null);
                    return;
                }
                super.exchange(event, cb);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        long delay = renewalDelay(clock);

        refusing.set(true);
        clock.advance(delay);
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
        assertTrue(disconnects.isEmpty());

        // The fetch retry presents the stored token again, inside the grace.
        pumpUntil(clock, "second attempt", () -> storage.exchanged.size() >= 3);
        assertEquals("refresh-2", storage.exchanged.get(1));
        assertEquals("refresh-2", storage.exchanged.get(2));
        assertTrue(disconnects.isEmpty());
        assertEquals(1, connected.size());
    }


    @Test
    public void refusalRunSurvivesAReconnectAndDisconnectEndsIt() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        Object horizon = horizonOf();
        assertNotNull(horizon);

        // The reconnect goes on with the same run: its horizon is the one of the first refusal.
        server.closeConnection();
        pumpUntil(clock, "reconnect refused", () -> storage.exchanged.size() >= 3);
        assertSame(horizon, horizonOf());
        assertTrue(disconnects.isEmpty());

        // The application's disconnect ends the run: a new connect starts one, even past the old horizon.
        client.disconnect();
        awaitCondition("run closed", () -> horizonOf() == null);
        clock.advance(29000);
        int before = bootstrapDelays(clock).size();
        client.connect();
        awaitCondition("retry of the new run scheduled", () -> bootstrapDelays(clock).size() > before);
        assertNotNull(horizonOf());
        assertEquals(0, disconnects.stream().filter((DisconnectedEvent event) -> event.getCode() == CODE_DISCONNECT_UNAUTHORIZED).count());
    }


    @Test
    public void changingStoredTokenIsPresentedOnlyWhileTheHorizonIsOpen() throws Exception {
        // The storage alternates between two values the issuer refuses. Each attempt moves the clock on by a second.
        ManualScheduler clock = new ManualScheduler();
        AtomicInteger loads = new AtomicInteger();
        Storage storage = new Storage("refresh-a") {
            @Override
            public int grace() {
                return 3;
            }

            @Override
            public String load() {
                return loads.incrementAndGet() % 2 == 0 ? "refresh-a" : "refresh-b";
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                clock.advance(1000);
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();
        pumpUntil(clock, "disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
        // The first changed token is presented at once; the later ones wait for the backoff, which the clock moves past.
        assertTrue(storage.exchanged.size() <= 4);
    }


    @Test
    public void changedValuesAfterTheFirstWaitForTheBackoffWhileTheClockStands() throws Exception {
        // The storage returns a new value on every read and the issuer refuses each attempt at once. The clock does not
        // move: only the first changed value is presented at once, and the next one waits for the backoff of the run.
        ManualScheduler clock = new ManualScheduler();
        AtomicInteger loads = new AtomicInteger();
        Storage storage = new Storage("refresh-0") {
            @Override
            public String load() {
                return "refresh-" + loads.incrementAndGet();
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        drainExecutor();
        assertEquals(Arrays.asList("refresh-1", "refresh-2"), storage.exchanged);
        assertTrue(disconnects.isEmpty());

        // The retry comes after the backoff, and presents the value read then, once more and no sooner.
        clock.advance(2000);
        drainExecutor();
        assertEquals(3, storage.exchanged.size());
        assertTrue(disconnects.isEmpty());
    }


    @Test
    public void refusalAfterDisconnectOpensNoRun() throws Exception {
        // The application disconnects, then logs out: the held exchange is refused after both. No run may survive them,
        // or the anonymous connect that follows would end the session on an empty storage.
        ManualScheduler clock = new ManualScheduler();
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("first exchange started", () -> storage.held != null);

        client.disconnect();
        awaitCondition("disconnected", () -> client.getState() == ClientState.DISCONNECTED);
        storage.refreshToken = "";
        storage.held.onDone(new UnauthorizedException(), null);
        drainExecutor();
        assertNull(horizonOf());

        client.connect();
        awaitCondition("connected anonymously", () -> connected.size() == 1);
        assertEquals(0, disconnects.stream().filter((DisconnectedEvent event) -> event.getCode() == CODE_DISCONNECT_UNAUTHORIZED).count());
        // The empty storage is read, and nothing is presented to the issuer: the connect went on anonymously.
        assertEquals(Collections.singletonList("refresh-1"), storage.exchanged);
    }


    @Test
    public void graceOfTheOptionReturnsWhenAResultPublishesNone() throws Exception {
        // The first result publishes a grace of 300 s, the second publishes none: the refusal run then takes the option's 7 s.
        ManualScheduler clock = new ManualScheduler();
        answerIdentity(false, 0);
        AtomicInteger calls = new AtomicInteger();
        Storage storage = new Storage("refresh-1") {
            @Override
            public int grace() {
                return 7;
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                int call = calls.incrementAndGet();
                if (call == 1) {
                    cb.onDone(null, new TokenExchangeResult("access-1", "refresh-2", 300));
                    return;
                }
                if (call == 2) {
                    cb.onDone(null, new TokenExchangeResult("access-2", "refresh-3"));
                    return;
                }
                cb.onDone(new UnauthorizedException(), null);
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);

        server.closeConnection();
        pumpUntil(clock, "second connected", () -> connected.size() == 2);
        server.closeConnection();
        pumpUntil(clock, "horizon of the option", () -> clock.requestedDelays().contains(7000L));
        assertFalse(clock.requestedDelays().contains(300000L));
    }


    @Test
    public void twoClientsSharingOneStorageStoreTheSameRotationWithTheirOwnAccessTokens() throws Exception {
        // Both clients read the token before either stores its rotation. The issuer answers each exchange with the
        // same rotated refresh token and a fresh access token, as it does inside the grace.
        answerIdentity(false, 0);
        AtomicInteger issued = new AtomicInteger();
        Storage storage = new Storage("refresh-1") {
            @Override
            public String load() {
                return "refresh-1";
            }

            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                cb.onDone(null, new TokenExchangeResult("access-" + issued.incrementAndGet(), "refresh-2"));
            }
        };
        newExchangeClient(exchangeOptions(storage), new ManualScheduler());
        Client second = new Client(
                server.url(),
                exchangeOptions(storage),
                listener(),
                RETRY_MIN_MILLIS,
                RETRY_MAX_MILLIS,
                new ManualScheduler(),
                new Random()
        );
        try {
            client.connect();
            second.connect();
            awaitCondition("both connected", () -> connected.size() == 2);

            assertEquals(Arrays.asList("refresh-2", "refresh-2"), storage.stored);
            assertEquals(new HashSet<>(Arrays.asList("access-1", "access-2")), new HashSet<>(refreshTokens()));
        } finally {
            second.close(1000);
        }
    }


    @Test
    public void exchangeThatPersistedItsRotationStoresNothing() throws Exception {
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                // The rotation was written under the application's own lock, inside the exchange.
                refreshToken = "refresh-2";
                cb.onDone(null, new TokenExchangeResult("access-1", null));
            }
        };
        newExchangeClient(exchangeOptions(storage), new ManualScheduler());

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertTrue(storage.stored.isEmpty());
        assertEquals("refresh-2", storage.refreshToken);
        assertEquals("access-1", lastRefreshToken());
    }


    @Test
    public void constructorRejectsAnEmptyRefusalType() throws Exception {
        Storage storage = new Storage("refresh-1") {
            @Override
            public Set<String> refusalTypes() {
                return new HashSet<>(Arrays.asList("REFRESH_TOKEN_REFUSED", ""));
            }
        };

        try {
            new Client(server.url(), exchangeOptions(storage), listener(), RETRY_MIN_MILLIS, RETRY_MAX_MILLIS, new ManualScheduler(), new Random());
            fail("the client must not be created with an empty refusal type");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }


    @Test
    public void refusalIsClassifiedByTheListedErrorName() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        rejectExchangeRpc("REFRESH_TOKEN_REFUSED");
        Storage storage = new Storage("refresh-1") {
            @Override
            public Set<String> refusalTypes() {
                return Collections.singleton("REFRESH_TOKEN_REFUSED");
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        // The listed name starts the refusal run: the attempt is retried within the grace, and is not the verdict.
        awaitCondition("refusal reported", () -> !errors.isEmpty());
        assertNotNull(horizonOf());
        assertTrue(disconnects.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
    }


    @Test
    public void anotherErrorNameIsAnOrdinaryErrorWithTheBootstrapBackoff() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        rejectExchangeRpc("SOMETHING_ELSE");
        Storage storage = new Storage("refresh-1") {
            @Override
            public Set<String> refusalTypes() {
                return Collections.singleton("REFRESH_TOKEN_REFUSED");
            }
        };
        Options options = exchangeOptions(storage);
        // Short exchange timeouts keep them out of the bootstrap delays, which are the retries only.
        options.setTimeout(1000);
        newExchangeClient(options, clock);

        client.connect();

        // Not a refusal: no run is opened, and the error is retried with the ordinary bootstrap backoff.
        awaitCondition("retry scheduled", () -> bootstrapDelays(clock).size() == 1);
        assertNull(fieldOf(fieldOf(client.auth, "flow"), "horizon"));
        assertTrue(errors.get(0) instanceof TokenError);
        assertTrue(disconnects.isEmpty());
    }


    // The fake server rejects every exchange rpc with a 400 whose app type is the given name.
    private void rejectExchangeRpc(final String appType) {
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRpc() && METHOD.equals(cmd.getRpc().getMethod())) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(400).setMessage("refused").setAppType(appType))
                        .build();
            }
            return null;
        };
    }


    @Test
    public void nullRpcPayloadFailsTheBootstrapExchangeAtOnce() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                event.rpc(METHOD, null, (Throwable err, RPCResult result) -> cb.onDone(err, null));
            }
        };
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        awaitCondition("error reported", () -> !errors.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
        assertTrue(((TokenError) errors.get(0)).getError() instanceof NullPointerException);
        assertTrue(connected.isEmpty());
        assertFalse(kinds().contains("rpc:" + METHOD));
    }


    @Test
    public void stalledBootstrapExchangeFailsWithinTheTimeoutAndReconnects() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        ManualScheduler clock = new ManualScheduler();
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("first exchange started", () -> storage.held != null);
        int delaysBefore = clock.requestedDelays().size();

        clock.advance(options.getTimeout());

        awaitCondition("timeout reported", () -> !errors.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
        assertTrue(connected.isEmpty());
        awaitCondition("reconnect scheduled", () -> clock.requestedDelays().size() == delaysBefore + 1);
        clock.advance(60000);
        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("refresh-1", "refresh-1"), storage.exchanged);
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertEquals(2, kinds().stream().filter("connect"::equals).count());
    }


    @Test
    public void unansweredBootstrapRefreshClosesTheConnectionBeforeConnected() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        Storage storage = new Storage("refresh-1");
        Options options = exchangeOptions(storage);
        options.setTimeout(200);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("refresh sent", () -> refreshTokens().size() == 1);
        int delaysBefore = clock.requestedDelays().size();

        awaitCondition("timeout reported", () -> !errors.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
        awaitCondition("reconnect scheduled", () -> clock.requestedDelays().size() > delaysBefore);
        assertTrue(connected.isEmpty());
        // The app logs out while the client waits: the next bootstrap finds no refresh token.
        storage.refreshToken = null;
        clock.advance(60000);

        awaitCondition("connected", () -> connected.size() == 1);
        assertTrue(connected.get(0).getUser().isEmpty());
        assertEquals(Arrays.asList("connect", "rpc:" + METHOD, "refresh", "connect"), kinds());
        assertEquals(Arrays.asList("refresh-1"), storage.exchanged);
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
    }


    @Test
    public void pingDuringBootstrapIsAnsweredWithAPong() throws Exception {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25).setPong(true).build();
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("exchange started", () -> storage.held != null);

        server.sendPing();

        awaitCondition("pong sent", () -> server.received().contains(Protocol.Command.getDefaultInstance()));
        storage.held.onDone(null, new TokenExchangeResult("access-1", null));
        awaitCondition("connected", () -> connected.size() == 1);
    }


    @Test
    public void pingDuringBootstrapDoesNotDropTheConnectedConnection() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25).setPong(true).build();
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        Options options = exchangeOptions(storage);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("exchange started", () -> storage.held != null);
        server.sendPing();
        awaitCondition("pong sent during the bootstrap", () -> countPongs() == 1);
        storage.held.onDone(null, new TokenExchangeResult("access-1", null));
        awaitCondition("connected", () -> connected.size() == 1);
        // The ping timer of the bootstrap is replaced by the one of the connected state: exactly one is pending.
        assertEquals(1, clock.pendingWithDelay(25000L + options.getMaxServerPingDelay()));

        // The server pings on schedule after the connect. Each ping is answered, and the connection stays up past the
        // deadline of the ping timer which the bootstrap ping started.
        for (int round = 2; round <= 4; round++) {
            clock.advance(25000);
            drainExecutor();
            server.sendPing();
            final int expected = round;
            awaitCondition("pong " + round, () -> countPongs() == expected);
        }
        drainExecutor();
        assertEquals(ClientState.CONNECTED, client.getState());
        assertTrue("no disconnect", disconnects.isEmpty());
    }


    @Test
    public void abandonedBootstrapKeepsItsBackoffOnTheNextConnect() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        AtomicBoolean failing = new AtomicBoolean(true);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                if (failing.get()) {
                    exchanged.add(event.getRefreshToken());
                    cb.onDone(new IOException("exchange failed"), null);
                    return;
                }
                super.exchange(event, cb);
            }
        };
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        options.setMinReconnectDelay(RETRY_MIN_MILLIS);
        options.setMaxReconnectDelay(RETRY_MAX_MILLIS);
        newExchangeClient(options, clock);
        client.connect();
        // Three failed exchanges: the retry after the third one is pending when the connection drops.
        for (int retry = 1; retry <= 2; retry++) {
            final int expected = retry;
            awaitCondition("retry " + retry + " scheduled", () -> bootstrapDelays(clock).size() == expected);
            clock.advance(60000);
        }
        awaitCondition("third retry scheduled", () -> bootstrapDelays(clock).size() == 3);
        int delaysBefore = clock.requestedDelays().size();
        server.closeConnection();
        awaitCondition("reconnect scheduled", () -> clock.requestedDelays().size() > delaysBefore);
        clock.advance(RETRY_MAX_MILLIS);

        // The attempts count on across the abandoned bootstrap: the new one's first failure takes the fourth step.
        awaitCondition("fourth retry scheduled", () -> bootstrapDelays(clock).size() == 4);
        assertEquals(4, ((Integer) fieldOf(fieldOf(client.auth, "flow"), "bootstrapAttempt")).intValue());
    }


    @Test
    public void unauthorizedWithAChangedRefreshTokenIsPresentedAtOnce() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                if (exchanged.isEmpty()) {
                    // Another tab rotated the token meanwhile: the store now holds a newer one.
                    refreshToken = "refresh-1b";
                    exchanged.add(event.getRefreshToken());
                    cb.onDone(new UnauthorizedException(), null);
                    return;
                }
                super.exchange(event, cb);
            }
        };
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("refresh-1", "refresh-1b"), storage.exchanged);
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertTrue(errors.isEmpty());
        assertTrue(disconnects.isEmpty());
    }


    @Test
    public void failedBootstrapExchangeIsRetriedOnTheSameConnection() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        storage.failNext.set(true);
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        assertFalse(connectedNow());
        pumpUntil(clock, "connected", () -> connected.size() == 1);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof TokenError);
        assertEquals(1, kinds().stream().filter("connect"::equals).count());
        assertEquals(2, storage.exchanged.size());
    }


    @Test
    public void failedRenewalIsReportedAndRetried() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        Storage storage = new Storage("refresh-1");
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        long delay = renewalDelay(clock);

        storage.failNext.set(true);
        clock.advance(delay);
        awaitCondition("renewal reported", () -> !errors.isEmpty());
        pumpUntil(clock, "renewed", () -> refreshTokens().size() == 2);

        assertTrue(errors.get(0) instanceof TokenError);
        assertEquals(1, errors.size());
        assertEquals(1, connected.size());
        assertTrue(disconnects.isEmpty());
    }


    @Test
    public void renewalKeepsTheExchangeWhenTheOptionIsClearedAfterConstruction() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        Options options = exchangeOptions(new Storage("refresh-1"));
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        long delay = renewalDelay(clock);

        options.setTokenExchange(null);
        clock.advance(delay);

        pumpUntil(clock, "renewed", () -> refreshTokens().size() == 2);
        assertTrue(errors.isEmpty());
        assertEquals("access-1", lastRefreshToken());
    }


    @Test
    public void bootstrapEndedByARefusalStartsTheBackoffAfreshOnTheNextConnect() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRefresh()) {
                return refusal(cmd, CODE_PERMISSION_DENIED);
            }
            return null;
        };
        AtomicBoolean failing = new AtomicBoolean(true);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                if (failing.getAndSet(false)) {
                    exchanged.add(event.getRefreshToken());
                    cb.onDone(new IOException("exchange failed"), null);
                    return;
                }
                super.exchange(event, cb);
            }
        };
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        // One failed exchange, then the retry is refused: the session ends, and the backoff of the bootstrap ends with it.
        awaitCondition("retry scheduled", () -> bootstrapDelays(clock).size() == 1);
        clock.advance(60000);
        awaitCondition("disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_PERMISSION_DENIED, disconnects.get(0).getCode());
        assertEquals(0, ((Integer) fieldOf(fieldOf(fieldOf(client, "auth"), "flow"), "bootstrapAttempt")).intValue());

        // A new connect starts at the first step of the backoff again: its first retry is at most the first step.
        failing.set(true);
        client.connect();
        awaitCondition("retry of the new connect scheduled", () -> bootstrapDelays(clock).size() == 2);
        assertTrue(bootstrapDelays(clock).get(1) <= 2 * BOOTSTRAP_RETRY_MIN_MILLIS);
    }


    @Test
    public void bootstrapRefreshRefusedWithADisconnectCodeDisconnects() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRefresh()) {
                return refusal(cmd, CODE_PERMISSION_DENIED);
            }
            return null;
        };
        newExchangeClient(exchangeOptions(new Storage("refresh-1")), clock);

        client.connect();

        awaitCondition("disconnected", () -> disconnects.size() == 1);
        assertEquals(CODE_PERMISSION_DENIED, disconnects.get(0).getCode());
        assertTrue(connected.isEmpty());
        clock.advance(60000);
        assertEquals(1, kinds().stream().filter("connect"::equals).count());
    }


    @Test
    public void bootstrapRefreshRefusedWithTokenExpiredExchangesAgain() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        AtomicInteger refreshes = new AtomicInteger();
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRefresh() && refreshes.getAndIncrement() == 0) {
                return refusal(cmd, CODE_TOKEN_EXPIRED);
            }
            return null;
        };
        Storage storage = new Storage("refresh-1");
        newExchangeClient(exchangeOptions(storage), clock);

        client.connect();

        pumpUntil(clock, "connected", () -> connected.size() == 1);
        assertEquals(2, storage.exchanged.size());
        assertEquals(Arrays.asList("refresh-1", "refresh-2"), storage.exchanged);
        assertEquals(1, kinds().stream().filter("connect"::equals).count());
        assertTrue(disconnects.isEmpty());
        assertEquals(CODE_TOKEN_EXPIRED, ((ReplyError) errors.get(0)).getCode());
    }


    @Test
    public void subscribePushDuringBootstrapIsMergedIntoConnect() throws Exception {
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRpc()) {
                server.sendPush(subscribePush("news"));
            }
            return null;
        };
        answerExchanges("access-1|refresh-2");
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("subscribed", () -> order.size() == 2);
        assertEquals(Arrays.asList("connected", "subscribed:news"), order);
    }


    @Test
    public void unsubscribePushDuringBootstrapLeavesNoEvent() throws Exception {
        answerIdentity(false, 0);
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasRpc()) {
                server.sendPush(subscribePush("news"));
                server.sendPush(Protocol.Push.newBuilder()
                        .setChannel("news")
                        .setUnsubscribe(Protocol.Unsubscribe.newBuilder().setCode(0).setReason("gone"))
                        .build());
            }
            return null;
        };
        answerExchanges("access-1|refresh-2");
        newClient(exchangeOptions(new Storage("refresh-1")));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        drainExecutor();
        assertEquals(Arrays.asList("connected"), order);
    }


    @Test
    public void transportLostDuringExchangeIgnoresTheLateResult() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("first exchange started", () -> storage.held != null);

        server.closeConnection();

        awaitCondition("second bootstrap", () -> storage.exchanged.size() == 2);
        awaitCondition("connected", () -> connected.size() == 1);
        storage.held.onDone(null, new TokenExchangeResult("access-stale", "refresh-stale"));
        drainExecutor();

        assertEquals(1, connected.size());
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertEquals("access-1", lastRefreshToken());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void refreshTokenDuringBootstrapIsAppliedAfterTheBootstrap() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("bootstrap exchange started", () -> storage.held != null);

        client.refresh("app-token", (Throwable err, AuthResult result) -> {
        });
        storage.held.onDone(null, new TokenExchangeResult("access-1", "refresh-2"));

        awaitCondition("app refresh applied", () -> refreshTokens().size() == 2);
        awaitCondition("connected", () -> connected.size() == 1);
        // The held exchange answered the test itself, so no rpc reached the server.
        assertEquals(Arrays.asList("connect", "refresh", "refresh"), kinds());
        assertEquals(Arrays.asList("access-1", "app-token"), refreshTokens());
        assertEquals("app-token", lastRefreshToken());
        assertEquals("u1", connected.get(0).getUser());
        // The app's refresh moved the generation on, so the rotation of the bootstrap is stored only while the
        // app still holds the token which it used.
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertTrue(errors.isEmpty());
    }


    @Test
    public void rotationOfAnExchangeLostWithItsConnectionIsStoredWhileTheAppHoldsTheUsedToken() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        ManualScheduler clock = new ManualScheduler();
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("first exchange started", () -> storage.held != null);
        int delaysBefore = clock.requestedDelays().size();

        server.closeConnection();
        awaitCondition("connection lost", () -> clock.requestedDelays().size() > delaysBefore);
        // The reconnect is not run: the late reply is the only one, and the application still holds the used token.
        assertEquals("refresh-1", storage.load());
        storage.held.onDone(null, new TokenExchangeResult("access-stale", "refresh-2"));
        drainExecutor();

        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertEquals(Arrays.asList("refresh-1"), storage.exchanged);
        assertEquals(1, kinds().stream().filter("connect"::equals).count());
        assertTrue(connected.isEmpty());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void rotationOfAnExchangeLateAfterItsTimeoutIsStored() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        ManualScheduler clock = new ManualScheduler();
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("first exchange started", () -> storage.held != null);

        clock.advance(options.getTimeout());
        awaitCondition("timeout reported", () -> !errors.isEmpty());
        storage.held.onDone(null, new TokenExchangeResult("access-stale", "refresh-2"));
        drainExecutor();

        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertTrue(connected.isEmpty());
    }


    @Test
    public void renewalLateAfterItsTimeoutIsStoredWhileTheAppHoldsTheUsedToken() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        StallingStorage storage = new StallingStorage("refresh-1", 2);
        ManualScheduler clock = new ManualScheduler();
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        clock.advance(renewalDelay(clock));
        awaitCondition("renewal exchange started", () -> storage.held != null);

        clock.advance(options.getTimeout());
        awaitCondition("timeout reported", () -> !errors.isEmpty());
        storage.held.onDone(null, new TokenExchangeResult("access-2", "refresh-3"));
        drainExecutor();

        assertEquals(Arrays.asList("refresh-2", "refresh-3"), storage.stored);
        assertEquals("access-1", lastRefreshToken());
        assertEquals(1, connected.size());
        assertTrue(errors.get(0) instanceof TokenError);
        assertEquals(1, errors.size());
    }


    @Test
    public void renewalLateAfterItsTimeoutIsNotStoredAfterLogout() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        StallingStorage storage = new StallingStorage("refresh-1", 2);
        ManualScheduler clock = new ManualScheduler();
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        clock.advance(renewalDelay(clock));
        awaitCondition("renewal exchange started", () -> storage.held != null);

        clock.advance(options.getTimeout());
        awaitCondition("timeout reported", () -> !errors.isEmpty());
        // The app logs out while the connection stays up: the storage is cleared, and the late rotation must not return to it.
        storage.refreshToken = null;
        storage.held.onDone(null, new TokenExchangeResult("access-2", "refresh-3"));
        drainExecutor();

        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertNull(storage.load());
        assertEquals(1, connected.size());
        assertEquals(1, errors.size());
    }


    @Test
    public void renewalRefusedAfterItsTimeoutOpensTheRefusalRun() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        StallingStorage storage = new StallingStorage("refresh-1", 2);
        ManualScheduler clock = new ManualScheduler();
        Options options = exchangeOptions(storage);
        options.setTimeout(1000);
        newExchangeClient(options, clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        clock.advance(renewalDelay(clock));
        awaitCondition("renewal exchange started", () -> storage.held != null);

        clock.advance(options.getTimeout());
        awaitCondition("timeout reported", () -> !errors.isEmpty());
        assertNull(horizonOf());
        // The refusal arrives late: the horizon counts from it, as it does for a refusal within the deadline.
        storage.held.onDone(new UnauthorizedException(), null);
        drainExecutor();

        assertNotNull(horizonOf());
        assertTrue(disconnects.isEmpty());
        assertEquals(1, connected.size());
    }


    @Test
    public void renewalRotationDoesNotOverwriteATokenSetDuringTheExchange() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        StallingStorage storage = new StallingStorage("refresh-1", 2);
        ManualScheduler clock = new ManualScheduler();
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        clock.advance(renewalDelay(clock));
        awaitCondition("renewal exchange started", () -> storage.held != null);

        // The app logs in as another user while the renewal runs: it stores that user's refresh token, and refreshes with its access token.
        storage.refreshToken = "login-refresh";
        client.refresh("login-token", (Throwable err, AuthResult result) -> {
        });
        drainExecutor();
        storage.held.onDone(null, new TokenExchangeResult("access-2", "refresh-3"));

        awaitCondition("login refresh sent", () -> refreshTokens().contains("login-token"));
        drainExecutor();
        assertEquals("login-refresh", storage.load());
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertTrue(errors.isEmpty());
    }


    @Test
    public void rotationOfABootstrapDoesNotOverwriteATokenSetDuringTheExchange() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("exchange started", () -> storage.held != null);

        // The app logs in while the exchange runs: the refresh token of the login is the newest one, and the app stores it.
        client.refresh("login-token", (Throwable err, AuthResult result) -> {
        });
        drainExecutor();
        storage.refreshToken = "login-refresh";
        storage.held.onDone(null, new TokenExchangeResult("access-1", "refresh-2"));

        awaitCondition("connected", () -> connected.size() == 1);
        assertTrue(storage.stored.isEmpty());
        assertEquals("login-refresh", storage.load());
        assertTrue(errors.isEmpty());
    }


    @Test
    public void bootstrapRotationIsStoredWhileTheAppHoldsTheUsedTokenAfterARefreshCall() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("exchange started", () -> storage.held != null);

        // The app refreshes with a login token but stores no refresh token: the storage still holds the used one.
        client.refresh("login-token", (Throwable err, AuthResult result) -> {
        });
        drainExecutor();
        storage.held.onDone(null, new TokenExchangeResult("access-1", "refresh-2"));

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertEquals("refresh-2", storage.load());
    }


    @Test
    public void bootstrapRefreshWhoseWriteFailsClosesTheConnectionAndBootstrapsAgain() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        StallingStorage storage = new StallingStorage("refresh-1");
        ManualScheduler clock = new ManualScheduler();
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("exchange started", () -> storage.held != null);
        int delaysBefore = clock.requestedDelays().size();

        // The socket is closed before the refresh command is written: the write is refused, so its outcome is unknown.
        // The peer does not answer the close frame, so the disconnect cannot overtake the exchange's result on the executor.
        server.completeClose = false;
        ((WebSocket) fieldOf(client, "ws")).close(1000, "");
        storage.held.onDone(null, new TokenExchangeResult("access-1", "refresh-2"));

        awaitCondition("write failure reported", () -> !errors.isEmpty());
        assertTrue(errors.get(0) instanceof TokenError);
        assertTrue(refreshTokens().isEmpty());
        assertTrue(connected.isEmpty());
        awaitCondition("reconnect scheduled", () -> clock.requestedDelays().size() > delaysBefore);
        clock.advance(60000);

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("refresh-1", "refresh-2"), storage.exchanged);
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
        assertEquals(2, kinds().stream().filter("connect"::equals).count());
    }


    @Test
    public void emptyRefreshTokenAtBootstrapConnectsAnonymously() throws Exception {
        answerExchanges("access-1|refresh-2");
        Storage storage = new Storage("");
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(Arrays.asList("connect"), kinds());
        assertEquals("", connected.get(0).getUser());
        assertTrue(storage.exchanged.isEmpty());
    }


    @Test
    public void emptyRefreshTokenAtRenewalDisconnects() throws Exception {
        ManualScheduler clock = new ManualScheduler();
        answerExchanges("access-1|refresh-2");
        answerIdentity(true, TTL_SECONDS);
        Storage storage = new Storage("refresh-1");
        newExchangeClient(exchangeOptions(storage), clock);
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);

        storage.refreshToken = null;
        pumpUntil(clock, "disconnected", () -> disconnects.size() == 1);

        assertEquals(CODE_DISCONNECT_UNAUTHORIZED, disconnects.get(0).getCode());
        assertEquals(ClientState.DISCONNECTED, client.getState());
    }


    @Test
    public void storeFailureIsReportedAndBootstrapCompletes() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void store(String token) {
                throw new IllegalStateException("storage is full");
            }
        };
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof TokenError);
        assertEquals("access-1", lastRefreshToken());
    }


    @Test
    public void constructorRejectsTokenExchangeWithTokenGetter() throws Exception {
        Options options = exchangeOptions(new Storage("refresh-1"));
        options.setTokenGetter(new ConnectionTokenGetter() {
        });

        try {
            new Client(server.url(), options, listener());
            fail("the client must not be created with both");
        } catch (IllegalArgumentException e) {
            assertEquals("tokenExchange and tokenGetter are exclusive", e.getMessage());
        }
    }


    @Test
    public void manualRefreshDoesNotUseTheExchange() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        CompletableFuture<AuthResult> refreshed = new CompletableFuture<>();

        client.refresh("manual-token", (Throwable err, AuthResult result) -> {
            if (err != null) {
                refreshed.completeExceptionally(err);
            } else {
                refreshed.complete(result);
            }
        });

        assertEquals("u1", refreshed.get(5, TimeUnit.SECONDS).getUser());
        assertEquals(Arrays.asList("refresh-1"), storage.exchanged);
        assertEquals(Arrays.asList("access-1", "manual-token"), refreshTokens());
    }


    @Test
    public void rpcBeforeConnectWaitsForTheBootstrap() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        rpcReplies.put(ECHO, "pong".getBytes(StandardCharsets.UTF_8));
        newClient(exchangeOptions(new Storage("refresh-1")));
        client.connect();
        CompletableFuture<RPCResult> echo = new CompletableFuture<>();

        client.rpc(ECHO, EMPTY_PAYLOAD, into(echo));

        assertEquals("pong", new String(echo.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(Arrays.asList("connect", "rpc:" + METHOD, "refresh", "rpc:" + ECHO), kinds());
    }


    @Test
    public void exchangeCallbackCanCompleteOnAnotherThread() throws Exception {
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                exchanged.add(event.getRefreshToken());
                new Thread(() -> cb.onDone(null, new TokenExchangeResult("access-1", "refresh-2"))).start();
            }
        };
        newClient(exchangeOptions(storage));

        client.connect();

        awaitCondition("connected", () -> connected.size() == 1);
        assertEquals("access-1", lastRefreshToken());
        assertEquals(Arrays.asList("refresh-2"), storage.stored);
    }


    @Test
    public void closeDuringExchangeCallsEveryCallbackOnce() throws Exception {
        List<Throwable> rpcErrors = new CopyOnWriteArrayList<>();
        AtomicInteger rpcCalls = new AtomicInteger();
        List<ResultCallback<TokenExchangeResult>> held = new CopyOnWriteArrayList<>();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRpc();
        Storage storage = new Storage("refresh-1") {
            @Override
            public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
                held.add(cb);
                event.rpc(METHOD, EMPTY_PAYLOAD, (Throwable err, RPCResult result) -> {
                    rpcCalls.incrementAndGet();
                    rpcErrors.add(err);
                });
            }
        };
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("exchange started", () -> held.size() == 1);

        client.close(5000);
        held.get(0).onDone(null, new TokenExchangeResult("access-late", "refresh-late"));
        awaitCondition("late result settled", () -> rpcCalls.get() == 1);

        assertEquals(1, rpcCalls.get());
        assertTrue(rpcErrors.get(0) instanceof IOException);
        assertTrue(connected.isEmpty());
        // The close ends the exchange like a lost connection: its rotation is stored, as the app still holds the used token.
        assertEquals(Arrays.asList("refresh-late"), storage.stored);
    }


    @Test
    public void noFieldOfTheFlowRetainsTheRefreshToken() throws Exception {
        answerExchanges("access-1|refresh-2");
        answerIdentity(false, 0);
        Storage storage = new Storage("refresh-1");
        newClient(exchangeOptions(storage));
        client.connect();
        awaitCondition("connected", () -> connected.size() == 1);
        Object flow = fieldOf(client.auth, "flow");
        assertNotNull(flow);
        // Every refresh token the application handed out or took in: the flow may hold none of them.
        Set<String> refreshTokens = new HashSet<>(storage.exchanged);
        refreshTokens.addAll(storage.stored);
        assertEquals(new HashSet<>(Arrays.asList("refresh-1", "refresh-2")), refreshTokens);

        for (Field field : flow.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.getName().equals("exchange") || field.getName().equals("host")) {
                continue;
            }
            field.setAccessible(true);
            Object value = field.get(flow);
            assertFalse("field " + field.getName() + " holds a refresh token", holdsRefreshToken(value, 2, refreshTokens));
        }
    }


    @Override
    protected Client newClient(Options options) {
        return newClient(options, listener());
    }


    // The client on the manual clock, with this test's listener. Named apart from the base helper, which is protected.
    private Client newExchangeClient(Options options, ManualScheduler clock) {
        client = new Client(server.url(), options, listener(), RETRY_MIN_MILLIS, RETRY_MAX_MILLIS, clock, new Random());
        return client;
    }


    private EventListener listener() {
        return new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent event) {
                order.add("connected");
                connected.add(event);
            }


            @Override
            public void onAuth(Client c, AuthEvent event) {
                auths.add(event);
            }


            @Override
            public void onError(Client c, ErrorEvent event) {
                errors.add(event.getError());
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent event) {
                disconnects.add(event);
            }


            @Override
            public void onSubscribed(Client c, ServerSubscribedEvent event) {
                order.add("subscribed:" + event.getChannel());
            }


            @Override
            public void onUnsubscribed(Client c, ServerUnsubscribedEvent event) {
                order.add("unsubscribed:" + event.getChannel());
            }
        };
    }


    private static Options exchangeOptions(TokenExchange exchange) {
        Options options = new Options();
        options.setTokenExchange(exchange);
        return options;
    }


    // The rpc reply of the exchange method: the access token, then the rotated refresh token when there is one.
    private void answerExchanges(String reply) {
        rpcReplies.put(METHOD, reply.getBytes(StandardCharsets.UTF_8));
    }


    // The fake server's replies to the rpc and the refresh commands, unless a test answered them already.
    private Protocol.Reply answerExchangeCommand(Protocol.Command cmd) {
        if (cmd.hasRpc()) {
            byte[] data = rpcReplies.getOrDefault(cmd.getRpc().getMethod(), new byte[0]);
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRpc(Protocol.RPCResult.newBuilder().setData(com.google.protobuf.ByteString.copyFrom(data)))
                    .build();
        }
        if (cmd.hasRefresh()) {
            return Protocol.Reply.newBuilder().setId(cmd.getId()).setRefresh(refreshResult).build();
        }
        return null;
    }


    // The identity of the refresh reply: user u1, with the expiry of the access token when expires is set.
    private void answerIdentity(boolean expires, int ttl) {
        refreshResult = Protocol.RefreshResult.newBuilder()
                .setUser("u1")
                .setAuthClass("authenticated")
                .setExpires(expires)
                .setTtl(ttl)
                .build();
    }


    private static TokenExchangeResult parseExchange(RPCResult result) {
        String[] parts = new String(result.getData(), StandardCharsets.UTF_8).split("\\|");
        return new TokenExchangeResult(parts[0], parts.length > 1 ? parts[1] : null);
    }


    private static Protocol.Reply refusal(Protocol.Command cmd, int code) {
        return Protocol.Reply.newBuilder()
                .setId(cmd.getId())
                .setError(Protocol.Error.newBuilder().setCode(code).setMessage("refused"))
                .build();
    }


    private static Protocol.Push subscribePush(String channel) {
        return Protocol.Push.newBuilder()
                .setChannel(channel)
                .setSubscribe(Protocol.Subscribe.newBuilder().setOffset(5).setEpoch("e1").setRecoverable(true))
                .build();
    }


    private boolean connectedNow() {
        return !connected.isEmpty();
    }


    // The kinds of the commands received, in order: "connect", "rpc:<method>" and "refresh".
    private List<String> kinds() {
        List<String> kinds = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                kinds.add("connect");
            } else if (cmd.hasRpc()) {
                kinds.add("rpc:" + cmd.getRpc().getMethod());
            } else if (cmd.hasRefresh()) {
                kinds.add("refresh");
            }
        }
        return kinds;
    }


    private List<String> connectTokens() {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                tokens.add(cmd.getConnect().getToken());
            }
        }
        return tokens;
    }


    private List<String> refreshTokens() {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRefresh()) {
                tokens.add(cmd.getRefresh().getToken());
            }
        }
        return tokens;
    }


    private String lastRefreshToken() {
        List<String> tokens = refreshTokens();
        return tokens.isEmpty() ? null : tokens.get(tokens.size() - 1);
    }


    // The delays of the bootstrap retries scheduled so far. The exchange timeouts and the reconnects are shorter.
    private static List<Long> bootstrapDelays(ManualScheduler clock) {
        List<Long> delays = new ArrayList<>();
        for (long delay : clock.requestedDelays()) {
            if (delay >= BOOTSTRAP_RETRY_MIN_MILLIS) {
                delays.add(delay);
            }
        }
        return delays;
    }


    // The delay of the renewal timer: the one of the scheduled delays inside the window. The connected event comes
    // before the timer is set, so this waits for it.
    private long renewalDelay(ManualScheduler clock) throws InterruptedException {
        awaitCondition("renewal timer", () -> renewalDelayIn(clock) != null);
        return renewalDelayIn(clock);
    }


    private static Long renewalDelayIn(ManualScheduler clock) {
        for (long delay : clock.requestedDelays()) {
            if (delay >= WINDOW_MIN_MILLIS && delay <= WINDOW_MAX_MILLIS) {
                return delay;
            }
        }
        return null;
    }


    // Moves the manual clock forward until the condition holds. The clock stays inside the ping delay of a connection.
    private void pumpUntil(ManualScheduler clock, String what, BooleanSupplier condition) throws InterruptedException {
        for (int step = 0; step < PUMP_STEPS; step++) {
            if (condition.getAsBoolean()) {
                return;
            }
            clock.advance(PUMP_STEP_MILLIS);
            Thread.sleep(5);
        }
        awaitCondition(what, condition);
    }


    // The deadline of the open refusal run of the flow, null when no run is open.
    private Object horizonOf() {
        try {
            return fieldOf(fieldOf(client.auth, "flow"), "horizon");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    // The empty commands the client sent: its answers to the pings of the server.
    private long countPongs() {
        return server.received().stream().filter(Protocol.Command.getDefaultInstance()::equals).count();
    }


    private static Object fieldOf(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }


    // Whether the value holds one of the given refresh tokens, in its fields of this package, to the given depth.
    private static boolean holdsRefreshToken(Object value, int depth, Set<String> refreshTokens) throws IllegalAccessException {
        if (value == null) {
            return false;
        }
        if (value instanceof String) {
            return refreshTokens.contains(value);
        }
        if (depth == 0 || !value.getClass().getName().startsWith("io.github.centrifugal.centrifuge.")) {
            return false;
        }
        for (Field field : value.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
                continue;
            }
            field.setAccessible(true);
            if (holdsRefreshToken(field.get(value), depth - 1, refreshTokens)) {
                return true;
            }
        }
        return false;
    }
}
