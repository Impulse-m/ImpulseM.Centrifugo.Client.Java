package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


/**
 * refresh() changes the identity of a connection in place: the listener hears it through
 * onAuth, rpc calls wait for an in-flight refresh, and a refused refresh keeps the identity and
 * the token. The refresh timer keeps one chain and keeps the connection on a refused refresh.
 * A server without the identity fields reports empty values.
 */
public class AuthTest extends RpcTestBase {
    private static final int PERMISSION_DENIED = 103;
    private static final int TOKEN_EXPIRED = 109;
    private static final int UNAUTHORIZED = 101;
    // A client error, below 100: not temporary, and the timer retries it with a fresh token.
    private static final int CLIENT_ERROR = 3;
    private static final int LIMIT_EXCEEDED = ReplyError.LIMIT_EXCEEDED;

    private static final long ONE_SECOND_NANOS = 1_000_000_000L;

    // Retry bounds of the tests which wait for a retry: far shorter than the client defaults.
    private static final int FAST_RETRY_MIN_MILLIS = 50;
    private static final int FAST_RETRY_MAX_MILLIS = 100;


    private final List<String> authLog = new CopyOnWriteArrayList<>();


    private final List<Thread> authThreads = new CopyOnWriteArrayList<>();


    private final List<ConnectedEvent> connectedEvents = new CopyOnWriteArrayList<>();


    private final List<ErrorEvent> errorEvents = new CopyOnWriteArrayList<>();


    private final List<DisconnectedEvent> disconnectedEvents = new CopyOnWriteArrayList<>();


    @Test
    public void refreshWhileConnectedReturnsTheNewIdentity() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        answerRefreshes("u2", "authenticated");

        AuthResult result = refresh(client, "token-2").get(5, TimeUnit.SECONDS);

        assertEquals("u2", result.getUser());
        assertEquals("authenticated", result.getAuthClass());
        assertEquals("token-2", lastRefreshToken());
    }


    @Test
    public void refreshWhileDisconnectedIsSentAfterConnect() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRefreshes("u2", "authenticated");
        newAuthClient(new Options());

        CompletableFuture<AuthResult> future = refresh(client, "token-2");
        connect(client);

        assertEquals("u2", future.get(5, TimeUnit.SECONDS).getUser());
        assertEquals("token-2", lastRefreshToken());
    }


    @Test
    public void rpcWaitsForInFlightRefresh() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRpcs("after-refresh");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();

        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, into(rpcFuture));

        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        syncWithServer();
        assertEquals("rpc must wait for the refresh reply", 0, sentRpcRequests().size());
        assertFalse(rpcFuture.isDone());

        server.releaseDeferredCommands();

        refreshFuture.get(5, TimeUnit.SECONDS);
        assertEquals("after-refresh", new String(rpcFuture.get(5, TimeUnit.SECONDS).getData()));
        assertEquals(1, sentRpcRequests().size());
    }


    @Test
    public void rpcBeyondTheQueueBoundFailsWhileARefreshIsInFlight() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRpcs("ok");
        Options options = new Options();
        options.setInstance("abcdefghijklmnopqrstuv");
        options.setMaxPendingRpc(2);
        newAuthClient(options);
        connect(client);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        awaitCondition("refresh sent", () -> lastRefreshToken() != null);

        List<CompletableFuture<RPCResult>> rpcs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            rpcs.add(future);
            client.rpc("method", EMPTY_PAYLOAD, into(future));
        }

        assertEquals(ReplyError.LIMIT_EXCEEDED, replyErrorOf(rpcs.get(2)).getCode());
        server.releaseDeferredCommands();
        refreshFuture.get(5, TimeUnit.SECONDS);
        assertEquals("ok", new String(rpcs.get(0).get(5, TimeUnit.SECONDS).getData()));
        assertEquals("ok", new String(rpcs.get(1).get(5, TimeUnit.SECONDS).getData()));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void refreshBeyondThe32InProgressIsRefusedWithLimitExceeded() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> first = refresh(client, "token-0");
        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        for (int i = 1; i < 32; i++) {
            refresh(client, "token-" + i);
        }
        CompletableFuture<AuthResult> refused = refresh(client, "token-32");

        assertEquals(ReplyError.LIMIT_EXCEEDED, ((ReplyError) errorOf(refused)).getCode());
        server.deferCommand = null;
        server.releaseDeferredCommands();
        first.get(5, TimeUnit.SECONDS);
    }


    @Test
    public void deadlineOfAnRpcIssuedDuringARefreshStartsWhenIssued() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRpcs("ok");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();

        client.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(200).build(), into(rpcFuture));

        assertTrue("the deadline ends the call while the refresh is in flight", errorOf(rpcFuture) instanceof TimeoutException);
        assertFalse(refreshFuture.isDone());
        server.releaseDeferredCommands();
        refreshFuture.get(5, TimeUnit.SECONDS);
    }


    @Test
    public void authEventFiresOnConnectAndWhenTheIdentityChanges() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        awaitCondition("auth on connect", () -> authLog.size() == 1);
        assertEquals(Arrays.asList("u1/authenticated"), authLog);

        answerRefreshes("u1", "authenticated");
        refresh(client, "token-1b").get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("u1/authenticated"), authLog);

        answerRefreshes("u2", "authenticated");
        refresh(client, "token-2").get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("u1/authenticated", "u2/authenticated"), authLog);

        answerRefreshes("u2", "bot");
        refresh(client, "token-3").get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("u1/authenticated", "u2/authenticated", "u2/bot"), authLog);

        server.connectResult = connectResultFor("u2", "bot");
        server.closeConnection();
        awaitCondition("reconnected", () -> connectedEvents.size() == 2);
        awaitCondition("auth on reconnect", () -> authLog.size() == 4);
        assertEquals("u2/bot", authLog.get(3));
    }


    @Test
    public void refusedRefreshKeepsTheIdentityAndTheToken() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        refuseRefreshes();

        ReplyError error = (ReplyError) errorOf(refresh(client, "token-2"));

        assertEquals(PERMISSION_DENIED, error.getCode());
        assertEquals(Arrays.asList("u1/authenticated"), authLog);
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());

        server.onCommand = null;
        server.closeConnection();
        awaitCondition("reconnected", () -> connectedEvents.size() == 2);
        assertEquals("token-1", lastConnectRequest().getToken());
    }


    @Test
    public void refusedTimerRefreshKeepsTheConnection() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        newAuthClient(optionsWithTokenGetter(new CopyOnWriteArrayList<Long>()));
        connect(client);
        refuseRefreshes();

        awaitCondition("refresh error reported", () -> !errorEvents.isEmpty());
        drainExecutor();

        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
        assertEquals(Arrays.asList("u1/authenticated"), authLog);
    }


    @Test
    public void refusedTimerRefreshIsNotRetried() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        List<Long> fetchTimes = new CopyOnWriteArrayList<>();
        ManualScheduler clock = new ManualScheduler();
        newAuthClientOnClock(optionsWithTokenGetter(fetchTimes), clock);
        connect(client);
        awaitCondition("the timer is scheduled one ttl after the connect", () -> clock.requestedDelays().contains(1000L));
        refuseRefreshes();
        int scheduled = clock.requestedDelays().size();

        clock.advance(1000);
        awaitCondition("refused refresh sent", () -> refreshTokens().size() == 1);
        awaitCondition("refresh error reported", () -> !errorEvents.isEmpty());
        // The error is reported on the executor before a retry would be scheduled, so once the executor
        // has run everything queued before this call, a retry is either scheduled or it is not.
        drainExecutor();

        assertEquals("no retry is scheduled", scheduled, clock.requestedDelays().size());
        assertEquals("the refused refresh is not sent again", 1, refreshTokens().size());
        assertEquals("the token is not fetched again", 1, fetchTimes.size());
        assertEquals(ClientState.CONNECTED, client.getState());
    }


    @Test
    public void timerRefreshRefusedWithAnotherCodeDisconnectsWithIt() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        newAuthClient(optionsWithTokenGetter(new CopyOnWriteArrayList<Long>()));
        connect(client);
        answerRefreshWith(Protocol.Reply.newBuilder()
                .setError(Protocol.Error.newBuilder().setCode(UNAUTHORIZED).setMessage("unauthorized")));

        awaitCondition("disconnected", () -> !disconnectedEvents.isEmpty());

        assertEquals(UNAUTHORIZED, disconnectedEvents.get(0).getCode());
        assertEquals(ClientState.DISCONNECTED, client.getState());
    }


    @Test
    public void timerRefreshWithAnExpiredTokenRetriesWithAFreshToken() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        AtomicInteger fetches = new AtomicInteger();
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                cb.Done(null, "token-" + fetches.incrementAndGet());
            }
        });
        newAuthClient(options, FAST_RETRY_MIN_MILLIS, FAST_RETRY_MAX_MILLIS);
        connect(client);
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            if ("token-1".equals(cmd.getRefresh().getToken())) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(TOKEN_EXPIRED).setMessage("token expired"))
                        .build();
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRefresh(Protocol.RefreshResult.newBuilder().setUser("u1").setAuthClass("authenticated").setExpires(true).setTtl(1))
                    .build();
        };

        awaitCondition("retry sent with a fresh token", () -> refreshTokens().size() >= 2);

        assertEquals(Arrays.asList("token-1", "token-2"), refreshTokens().subList(0, 2));
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
    }


    @Test
    public void timerRefreshWithAClientErrorCodeIsRetriedWithAFreshToken() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        AtomicInteger fetches = new AtomicInteger();
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                cb.Done(null, "token-" + fetches.incrementAndGet());
            }
        });
        newAuthClient(options, FAST_RETRY_MIN_MILLIS, FAST_RETRY_MAX_MILLIS);
        connect(client);
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            if ("token-1".equals(cmd.getRefresh().getToken())) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(CLIENT_ERROR).setMessage("bad request"))
                        .build();
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRefresh(Protocol.RefreshResult.newBuilder().setUser("u1").setAuthClass("authenticated").setExpires(true).setTtl(1))
                    .build();
        };

        awaitCondition("retry sent with a fresh token", () -> refreshTokens().size() >= 2);

        assertEquals(Arrays.asList("token-1", "token-2"), refreshTokens().subList(0, 2));
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
    }


    @Test
    public void rpcWaitsForARefreshQueuedBehindATimerFetch() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        List<TokenCallback> fetches = new CopyOnWriteArrayList<>();
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                fetches.add(cb);
            }
        });
        newAuthClient(options);
        connect(client);
        answerRefreshes("u2", "authenticated");
        awaitCondition("timer fetch started", () -> fetches.size() == 1);

        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, into(rpcFuture));
        syncWithServer();
        assertTrue("the refresh waits behind the fetch", refreshTokens().isEmpty());
        assertEquals("the rpc waits for the refresh", 0, sentRpcRequests().size());

        fetches.get(0).Done(null, "token-timer");

        refreshFuture.get(5, TimeUnit.SECONDS);
        rpcFuture.get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("token-timer", "token-2"), refreshTokens());
        int refreshAt = -1;
        int rpcAt = -1;
        List<Protocol.Command> received = server.received();
        for (int i = 0; i < received.size(); i++) {
            if (received.get(i).hasRefresh() && "token-2".equals(received.get(i).getRefresh().getToken())) {
                refreshAt = i;
            }
            if (received.get(i).hasRpc()) {
                rpcAt = i;
            }
        }
        assertTrue("the rpc is sent after the refresh", refreshAt >= 0 && rpcAt > refreshAt);
    }


    @Test
    public void refreshWithTheSameTokenSharesOneCommand() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();

        CompletableFuture<AuthResult> first = refresh(client, "token-a");
        CompletableFuture<AuthResult> second = refresh(client, "token-a");
        awaitCondition("refresh sent", () -> refreshTokens().size() == 1);
        syncWithServer();
        assertEquals(Arrays.asList("token-a"), refreshTokens());

        server.releaseDeferredCommands();

        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals("one command for both calls", Arrays.asList("token-a"), refreshTokens());
    }


    @Test
    public void aThrowingCallbackDoesNotStopTheOthersSharingItsRefresh() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        AtomicInteger thrower = new AtomicInteger();
        AtomicInteger sibling = new AtomicInteger();
        client.refresh("token-a", (Throwable error, AuthResult result) -> {
            thrower.incrementAndGet();
            throw new IllegalStateException("callback failed");
        });
        CompletableFuture<AuthResult> after = new CompletableFuture<>();
        client.refresh("token-a", (Throwable error, AuthResult result) -> {
            sibling.incrementAndGet();
            completeInto(after).onDone(error, result);
        });
        awaitCondition("refresh sent", () -> refreshTokens().size() == 1);

        server.releaseDeferredCommands();

        assertNotNull("the sibling gets the result", after.get(5, TimeUnit.SECONDS));
        assertEquals(1, thrower.get());
        assertEquals(1, sibling.get());
        assertEquals(Arrays.asList("token-a"), refreshTokens());
        assertEquals(ClientState.CONNECTED, client.getState());
    }


    @Test
    public void closingFailsEveryWaitingRefreshEvenWhenACallbackThrows() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> inFlight = refresh(client, "token-a");
        awaitCondition("refresh sent", () -> refreshTokens().size() == 1);
        AtomicInteger thrower = new AtomicInteger();
        client.refresh("token-b", (Throwable error, AuthResult result) -> {
            thrower.incrementAndGet();
            throw new IllegalStateException("callback failed");
        });
        CompletableFuture<AuthResult> shared = refresh(client, "token-b");

        client.close(0L);

        awaitCondition("the close completes its teardown", () -> client.getState() == ClientState.CLOSED);
        assertTrue("the in-flight refresh fails with the close", errorOf(inFlight) instanceof IOException);
        assertTrue("the waiting refresh which shares the throwing callback fails too", errorOf(shared) instanceof IOException);
        assertEquals(1, thrower.get());
        awaitCondition("the callback failure is reported", () -> errorEvents.stream().anyMatch((ErrorEvent e) -> e.getError() instanceof IllegalStateException));
    }


    @Test
    public void timerEntriesDoNotCountAgainstTheRefreshBound() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        ManualScheduler clock = new ManualScheduler();
        newAuthClientOnClock(optionsWithTokenGetter(new CopyOnWriteArrayList<Long>()), clock);
        connect(client);
        awaitCondition("the timer is scheduled one ttl after the connect", () -> clock.requestedDelays().contains(1000L));
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> first = refresh(client, "token-0");
        awaitCondition("refresh sent", () -> refreshTokens().size() == 1);
        // The timer fires one ttl after the connect and queues behind the refresh in flight.
        clock.advance(1000);
        drainExecutor();

        List<CompletableFuture<AuthResult>> accepted = new ArrayList<>();
        for (int i = 1; i < 32; i++) {
            accepted.add(refresh(client, "token-" + i));
        }
        CompletableFuture<AuthResult> refused = refresh(client, "token-32");

        assertEquals(LIMIT_EXCEEDED, ((ReplyError) errorOf(refused)).getCode());
        for (CompletableFuture<AuthResult> future : accepted) {
            assertFalse("accepted: it waits for the refresh in flight", future.isDone());
        }
        server.deferCommand = null;
        server.releaseDeferredCommands();

        first.get(5, TimeUnit.SECONDS);
        for (CompletableFuture<AuthResult> future : accepted) {
            future.get(5, TimeUnit.SECONDS);
        }
    }


    @Test
    public void refreshWhileDisconnectedSetsTheTokenOfTheNextConnect() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        client.disconnect();
        // The token the connection had is expired: a connect with it is refused.
        server.onCommand = (Protocol.Command cmd) -> {
            if (cmd.hasConnect() && "token-1".equals(cmd.getConnect().getToken())) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setError(Protocol.Error.newBuilder().setCode(TOKEN_EXPIRED).setMessage("token expired"))
                        .build();
            }
            if (cmd.hasRefresh()) {
                return Protocol.Reply.newBuilder()
                        .setId(cmd.getId())
                        .setRefresh(Protocol.RefreshResult.newBuilder().setUser("u2").setAuthClass("authenticated"))
                        .build();
            }
            return null;
        };

        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        connect(client);

        assertEquals("u2", refreshFuture.get(5, TimeUnit.SECONDS).getUser());
        assertEquals("token-2", lastConnectRequest().getToken());
    }


    @Test
    public void refusedRefreshWhileDisconnectedRestoresThePreviousToken() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        client.disconnect();
        refuseRefreshes();
        CompletableFuture<AuthResult> refused = refresh(client, "token-2");

        connect(client);

        assertEquals(PERMISSION_DENIED, ((ReplyError) errorOf(refused)).getCode());
        client.disconnect();
        server.onCommand = null;
        connect(client);
        assertEquals("the next connect carries the previous token", "token-1", lastConnectRequest().getToken());
    }


    @Test
    public void tokenOfAnAcceptedConnectSurvivesADroppedRefresh() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        client.disconnect();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshed = refresh(client, "token-2");

        connect(client);
        awaitCondition("refresh sent after the connect", () -> refreshTokens().size() == 1);
        server.closeConnection();

        assertTrue("the dropped refresh fails", errorOf(refreshed) instanceof IOException);
        awaitCondition("reconnected", () -> connectedEvents.size() >= 3);
        assertEquals("the connect accepted token-2, so the next connect carries it", "token-2", lastConnectRequest().getToken());
    }


    @Test
    public void droppedRefreshKeepsTheQueuedTokenOfAnotherRefresh() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        client.disconnect();
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> first = refresh(client, "token-2");
        CompletableFuture<AuthResult> second = refresh(client, "token-3");

        connect(client);
        awaitCondition("first refresh sent", () -> refreshTokens().size() == 1);
        server.closeConnection();

        assertTrue("the first refresh fails with the dropped transport", errorOf(first) instanceof IOException);
        awaitCondition("reconnected", () -> connectedEvents.size() >= 3);
        assertEquals("the queued refresh keeps its token for the next connect", "token-3", lastConnectRequest().getToken());
    }


    @Test
    public void refusedRefreshAfterAnAcceptedConnectKeepsTheConnectedToken() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setToken("token-1");
        newAuthClient(options);
        connect(client);
        client.disconnect();
        answerRefreshes("u1", "authenticated");
        CompletableFuture<AuthResult> first = refresh(client, "token-2");
        connect(client);
        first.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        refuseRefreshes();

        ReplyError error = (ReplyError) errorOf(refresh(client, "token-3"));

        assertEquals(PERMISSION_DENIED, error.getCode());
        server.onCommand = null;
        server.closeConnection();
        awaitCondition("reconnected", () -> connectedEvents.size() >= 3);
        assertEquals("the connection runs on token-2, so the next connect carries it", "token-2", lastConnectRequest().getToken());
    }


    @Test
    public void manualRefreshNearExpiryKeepsOneTimerChain() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        List<Long> fetchTimes = new CopyOnWriteArrayList<>();
        newAuthClient(optionsWithTokenGetter(fetchTimes));
        connect(client);
        answerRefreshWith(Protocol.Reply.newBuilder()
                .setRefresh(Protocol.RefreshResult.newBuilder().setUser("u1").setAuthClass("authenticated").setExpires(true).setTtl(1)));

        refresh(client, "token-manual").get(5, TimeUnit.SECONDS);

        awaitCondition("three timer refreshes", () -> fetchTimes.size() >= 3);
        for (int i = 1; i < fetchTimes.size(); i++) {
            assertTrue("one timer chain: refreshes come one ttl apart", fetchTimes.get(i) - fetchTimes.get(i - 1) > ONE_SECOND_NANOS / 2);
        }
    }


    @Test
    public void timerRefreshAndManualRefreshRunOneAfterTheOther() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        newAuthClient(optionsWithTokenGetter(new CopyOnWriteArrayList<Long>()));
        connect(client);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();
        awaitCondition("timer refresh sent", () -> refreshTokens().size() == 1);

        CompletableFuture<AuthResult> manual = refresh(client, "token-manual");
        syncWithServer();
        assertEquals("one refresh is outstanding", 1, refreshTokens().size());

        server.releaseDeferredCommands();
        awaitCondition("manual refresh sent", () -> refreshTokens().size() == 2);
        server.releaseDeferredCommands();

        manual.get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("token-timer", "token-manual"), refreshTokens());
    }


    @Test
    public void twoConcurrentRefreshCallsAreSentOneAfterAnother() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) ->cmd.hasRefresh();

        CompletableFuture<AuthResult> first = refresh(client, "token-a");
        CompletableFuture<AuthResult> second = refresh(client, "token-b");
        awaitCondition("first refresh sent", () -> refreshTokens().size() == 1);
        syncWithServer();
        assertEquals(Arrays.asList("token-a"), refreshTokens());

        server.releaseDeferredCommands();
        awaitCondition("second refresh sent", () -> refreshTokens().size() == 2);
        server.releaseDeferredCommands();

        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("token-a", "token-b"), refreshTokens());
    }


    @Test
    public void authEventsFireOnTheClientExecutor() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        answerRefreshes("u2", "authenticated");

        refresh(client, "token-2").get(5, TimeUnit.SECONDS);

        awaitCondition("auth on connect and on refresh", () -> authThreads.size() == 2);
        Thread executorThread = client.getExecutor().submit(() -> Thread.currentThread()).get(5, TimeUnit.SECONDS);
        assertSame("the connect reply is handled on the executor", executorThread, authThreads.get(0));
        assertSame("the refresh reply is handled on the executor", executorThread, authThreads.get(1));
    }


    @Test
    public void connectedEventCarriesTheIdentity() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());

        connect(client);

        assertEquals("u1", connectedEvents.get(0).getUser());
        assertEquals("authenticated", connectedEvents.get(0).getAuthClass());
    }


    @Test
    public void legacyServerReportsEmptyIdentity() throws Exception {
        newAuthClient(new Options());

        connect(client);

        assertEquals("", connectedEvents.get(0).getUser());
        assertEquals("", connectedEvents.get(0).getAuthClass());
        awaitCondition("auth on connect", () -> authLog.size() == 1);
        assertEquals(Arrays.asList("/"), authLog);

        answerRefreshes("", "");
        refresh(client, "token-2").get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("/"), authLog);
    }


    @Test
    public void refreshesWaitingForAnInFlightRefreshFailOnClose() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();
        CompletableFuture<AuthResult> first = refresh(client, "token-a");
        CompletableFuture<AuthResult> second = refresh(client, "token-b");
        awaitCondition("first refresh sent", () -> refreshTokens().size() == 1);

        client.close(1000);

        assertTrue("the refresh in flight fails with the close", errorOf(first) instanceof IOException);
        assertTrue("the waiting refresh fails with the close", errorOf(second) instanceof IOException);
        assertEquals("the waiting refresh is never sent", Arrays.asList("token-a"), refreshTokens());
    }


    @Test
    public void refreshWhileDisconnectedIsFailedOnceByClose() throws Exception {
        newAuthClient(new Options());
        AtomicInteger callbacks = new AtomicInteger();
        CompletableFuture<Throwable> failure = new CompletableFuture<>();

        client.refresh("token-2", (Throwable error, AuthResult result) -> {
            callbacks.incrementAndGet();
            failure.complete(error);
        });
        client.close(1000);

        assertTrue("close fails the refresh", failure.get(5, TimeUnit.SECONDS) instanceof IOException);
        assertEquals("the callback runs once", 1, callbacks.get());
    }


    @Test
    public void refreshDeadlineIsReportedAsTimeoutException() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        Options options = new Options();
        options.setTimeout(500);
        newAuthClient(options);
        connect(client);
        server.deferCommand = (Protocol.Command cmd) -> cmd.hasRefresh();

        assertTrue("the deadline reports a timeout", errorOf(refresh(client, "token-2")) instanceof TimeoutException);
    }


    @Test
    public void silentTokenGetterDoesNotHoldRpcCalls() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        answerRpcs("after-timer");
        List<Long> fetchTimes = new CopyOnWriteArrayList<>();
        newAuthClient(optionsWithSilentTokenGetter(fetchTimes));
        connect(client);
        awaitCondition("timer started a token fetch", () -> fetchTimes.size() == 1);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(1000).build(), into(future));

        assertEquals("after-timer", new String(future.get(5, TimeUnit.SECONDS).getData()));
    }


    @Test
    public void silentTokenGetterIsReportedAtTheClientTimeout() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        List<Long> fetchTimes = new CopyOnWriteArrayList<>();
        Options options = optionsWithSilentTokenGetter(fetchTimes);
        options.setTimeout(300);
        newAuthClient(options);
        connect(client);

        awaitCondition("token fetch reported as timed out", () -> tokenErrorOf(errorEvents) != null);
        assertTrue(tokenErrorOf(errorEvents).getError() instanceof TimeoutException);
        assertEquals(ClientState.CONNECTED, client.getState());
    }


    @Test
    public void explicitRefreshIsAnsweredAfterATokenGetterThrowsOnTheTimer() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        ManualScheduler clock = new ManualScheduler();
        AtomicBoolean failing = new AtomicBoolean(true);
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                if (failing.get()) {
                    throw new IllegalStateException("getter failed");
                }
                cb.Done(null, "token-timer");
            }
        });
        newAuthClientOnClock(options, clock);
        connect(client);
        awaitCondition("the timer is scheduled one ttl after the connect", () -> clock.requestedDelays().contains(1000L));
        answerRefreshes("u1", "authenticated");

        clock.advance(1000);
        awaitCondition("the getter failure is reported", () -> tokenErrorOf(errorEvents) != null);
        AuthResult result = refresh(client, "token-2").get(5, TimeUnit.SECONDS);

        assertEquals("u1", result.getUser());
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
    }


    @Test
    public void refreshIsAnsweredAfterTheAuthListenerThrowsOnATimerRefresh() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        ManualScheduler clock = new ManualScheduler();
        Options options = optionsWithTokenGetter(new CopyOnWriteArrayList<Long>());
        client = new Client(server.url(), options, throwingAuthListener(), FAST_RETRY_MIN_MILLIS, FAST_RETRY_MAX_MILLIS, clock);
        connect(client);
        awaitCondition("the timer is scheduled one ttl after the connect", () -> clock.requestedDelays().contains(1000L));
        answerRefreshes("u2", "authenticated");

        clock.advance(1000);
        awaitCondition("the timer refresh is sent", () -> refreshTokens().size() == 1);
        AuthResult result = refresh(client, "token-2").get(5, TimeUnit.SECONDS);

        assertEquals("u2", result.getUser());
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
    }


    @Test(expected = NullPointerException.class)
    public void refreshRejectsNullToken() {
        newAuthClient(new Options());

        client.refresh(null, (Throwable error, AuthResult result) -> {
        });
    }


    private void newAuthClient(Options options) {
        newAuthClient(options, Client.AUTH_RETRY_MIN_MILLIS, Client.AUTH_RETRY_MAX_MILLIS);
    }


    private void newAuthClient(Options options, int retryMinMillis, int retryMaxMillis) {
        client = new Client(server.url(), options, authListener(), retryMinMillis, retryMaxMillis, Executors.newScheduledThreadPool(1));
    }


    // A client whose timers run on the clock: the test moves it, so the refresh timer fires when the test says.
    private void newAuthClientOnClock(Options options, ManualScheduler clock) {
        client = new Client(server.url(), options, authListener(), FAST_RETRY_MIN_MILLIS, FAST_RETRY_MAX_MILLIS, clock);
    }


    private EventListener authListener() {
        return new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedEvents.add(e);
                connectedSignals.release();
            }


            @Override
            public void onAuth(Client c, AuthEvent e) {
                authThreads.add(Thread.currentThread());
                authLog.add(e.getUser() + "/" + e.getAuthClass());
            }


            @Override
            public void onError(Client c, ErrorEvent e) {
                errorEvents.add(e);
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedEvents.add(e);
                disconnectedSignals.release();
            }
        };
    }


    // A listener which fails on the identity "u2", after the connect and the disconnect signals are recorded.
    private EventListener throwingAuthListener() {
        return new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }


            @Override
            public void onAuth(Client c, AuthEvent e) {
                if ("u2".equals(e.getUser())) {
                    throw new IllegalStateException("listener failed");
                }
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        };
    }


    // A token getter which answers "token-timer", and records the time of each call. The
    // connection starts with a token of its own, so the connect does not call the getter.
    private static Options optionsWithTokenGetter(final List<Long> fetchTimes) {
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                fetchTimes.add(System.nanoTime());
                cb.Done(null, "token-timer");
            }
        });
        return options;
    }


    // A token getter which records each call and never answers, like a getter which lost its request.
    private static Options optionsWithSilentTokenGetter(final List<Long> fetchTimes) {
        Options options = new Options();
        options.setToken("token-initial");
        options.setTokenGetter(new ConnectionTokenGetter() {
            @Override
            public void getConnectionToken(ConnectionTokenEvent event, TokenCallback cb) {
                fetchTimes.add(System.nanoTime());
            }
        });
        return options;
    }


    // The token error the client reported, or null when none was reported.
    private static TokenError tokenErrorOf(List<ErrorEvent> events) {
        for (ErrorEvent event : events) {
            if (event.getError() instanceof TokenError) {
                return (TokenError) event.getError();
            }
        }
        return null;
    }


    private static Protocol.ConnectResult connectResultFor(String user, String authClass) {
        return Protocol.ConnectResult.newBuilder()
                .setClient("fake-client")
                .setVersion("0.0.0")
                .setPing(25)
                .setUser(user)
                .setAuthClass(authClass)
                .build();
    }


    private static Protocol.ConnectResult expiringResult(String user, String authClass, int ttl) {
        return Protocol.ConnectResult.newBuilder(connectResultFor(user, authClass))
                .setExpires(true)
                .setTtl(ttl)
                .build();
    }


    private void answerRefreshes(String user, String authClass) {
        answerRefreshWith(Protocol.Reply.newBuilder()
                .setRefresh(Protocol.RefreshResult.newBuilder().setUser(user).setAuthClass(authClass)));
    }


    // Answers every refresh with the refused transition: the connection keeps its identity and its token.
    private void refuseRefreshes() {
        answerRefreshWith(Protocol.Reply.newBuilder()
                .setError(Protocol.Error.newBuilder().setCode(PERMISSION_DENIED).setMessage("denied")));
    }


    private static CompletableFuture<AuthResult> refresh(Client c, String token) {
        CompletableFuture<AuthResult> future = new CompletableFuture<>();
        c.refresh(token, completeInto(future));
        return future;
    }


    // The tokens of the refresh commands the fake server received, in order.
    private List<String> refreshTokens() {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command command : server.received()) {
            if (command.hasRefresh()) {
                tokens.add(command.getRefresh().getToken());
            }
        }
        return tokens;
    }


    private String lastRefreshToken() {
        List<String> tokens = refreshTokens();
        return tokens.isEmpty() ? null : tokens.get(tokens.size() - 1);
    }
}
