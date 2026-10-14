package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * refresh() changes the identity of a connection in place: the listener hears it through
 * onAuth, rpc calls wait for an in-flight refresh, and a refused refresh keeps the identity and
 * the token. The refresh timer keeps one chain and keeps the connection on a refused refresh.
 * A server without the identity fields reports empty values.
 */
public class AuthTest extends RpcTestBase {
    private static final int PERMISSION_DENIED = 103;

    private static final long ONE_SECOND_NANOS = 1_000_000_000L;


    private final List<String> authLog = new CopyOnWriteArrayList<>();


    private final List<ConnectedEvent> connectedEvents = new CopyOnWriteArrayList<>();


    private final List<ErrorEvent> errorEvents = new CopyOnWriteArrayList<>();


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
        server.deferCommand = cmd -> cmd.hasRefresh();

        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, completing(rpcFuture));

        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        drainExecutor();
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
        options.setMaxPendingRpc(2);
        newAuthClient(options);
        connect(client);
        server.deferCommand = cmd -> cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        awaitCondition("refresh sent", () -> lastRefreshToken() != null);

        List<CompletableFuture<RPCResult>> rpcs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<RPCResult> future = new CompletableFuture<>();
            rpcs.add(future);
            client.rpc("method", EMPTY_PAYLOAD, completing(future));
        }

        assertEquals(ReplyError.LIMIT_EXCEEDED, ((ReplyError) errorOf(rpcs.get(2))).getCode());
        server.releaseDeferredCommands();
        refreshFuture.get(5, TimeUnit.SECONDS);
        assertEquals("ok", new String(rpcs.get(0).get(5, TimeUnit.SECONDS).getData()));
        assertEquals("ok", new String(rpcs.get(1).get(5, TimeUnit.SECONDS).getData()));
        assertEquals(2, sentRpcRequests().size());
    }


    @Test
    public void deadlineOfAnRpcIssuedDuringARefreshStartsWhenIssued() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRpcs("ok");
        newAuthClient(new Options());
        connect(client);
        server.deferCommand = cmd -> cmd.hasRefresh();
        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();

        client.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().timeout(200).build(), completing(rpcFuture));

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
        server.onCommand = cmd -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setError(Protocol.Error.newBuilder().setCode(PERMISSION_DENIED).setMessage("denied"))
                    .build();
        };

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
        server.onCommand = cmd -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setError(Protocol.Error.newBuilder().setCode(PERMISSION_DENIED).setMessage("denied"))
                    .build();
        };

        awaitCondition("refresh error reported", () -> !errorEvents.isEmpty());
        drainExecutor();

        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
        assertEquals(Arrays.asList("u1/authenticated"), authLog);
    }


    @Test
    public void manualRefreshNearExpiryKeepsOneTimerChain() throws Exception {
        server.connectResult = expiringResult("u1", "authenticated", 1);
        List<Long> fetchTimes = new CopyOnWriteArrayList<>();
        newAuthClient(optionsWithTokenGetter(fetchTimes));
        connect(client);
        server.onCommand = cmd -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRefresh(Protocol.RefreshResult.newBuilder().setUser("u1").setAuthClass("authenticated").setExpires(true).setTtl(1))
                    .build();
        };

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
        server.deferCommand = cmd -> cmd.hasRefresh();
        awaitCondition("timer refresh sent", () -> refreshTokens().size() == 1);

        CompletableFuture<AuthResult> manual = refresh(client, "token-manual");
        drainExecutor();
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
        server.deferCommand = cmd -> cmd.hasRefresh();

        CompletableFuture<AuthResult> first = refresh(client, "token-a");
        CompletableFuture<AuthResult> second = refresh(client, "token-b");
        awaitCondition("first refresh sent", () -> refreshTokens().size() == 1);
        drainExecutor();
        assertEquals(Arrays.asList("token-a"), refreshTokens());

        server.releaseDeferredCommands();
        awaitCondition("second refresh sent", () -> refreshTokens().size() == 2);
        server.releaseDeferredCommands();

        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("token-a", "token-b"), refreshTokens());
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
        assertEquals(Arrays.asList("/"), authLog);

        answerRefreshes("", "");
        refresh(client, "token-2").get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("/"), authLog);
    }


    @Test(expected = NullPointerException.class)
    public void refreshRejectsNullToken() {
        newAuthClient(new Options());

        client.refresh(null, (error, result) -> {
        });
    }


    private void newAuthClient(Options options) {
        newClient(options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedEvents.add(e);
                connectedSignals.release();
            }


            @Override
            public void onAuth(Client c, AuthEvent e) {
                authLog.add(e.getUser() + "/" + e.getAuthClass());
            }


            @Override
            public void onError(Client c, ErrorEvent e) {
                errorEvents.add(e);
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        });
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
        server.onCommand = cmd -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRefresh(Protocol.RefreshResult.newBuilder().setUser(user).setAuthClass(authClass))
                    .build();
        };
    }


    private static CompletableFuture<AuthResult> refresh(Client c, String token) {
        CompletableFuture<AuthResult> future = new CompletableFuture<>();
        c.refresh(token, completing(future));
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
