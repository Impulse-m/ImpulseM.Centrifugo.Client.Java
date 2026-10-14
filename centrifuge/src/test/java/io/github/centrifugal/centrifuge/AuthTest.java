package io.github.centrifugal.centrifuge;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * refresh() changes the identity of a connection in place: the listener hears it through
 * onAuth, rpc calls wait for an in-flight refresh, and a refused refresh keeps the identity.
 * A server without the identity fields reports empty values.
 */
public class AuthTest extends FakeServerTestBase {
    private static final int PERMISSION_DENIED = 103;

    private static final long WAIT_MILLIS = 5_000L;


    private final List<String> authLog = new CopyOnWriteArrayList<>();


    private final List<ConnectedEvent> connectedEvents = new CopyOnWriteArrayList<>();


    @Test
    public void refreshWhileConnectedReturnsTheNewIdentity() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient();
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
        newAuthClient();

        CompletableFuture<AuthResult> future = refresh(client, "token-2");
        connect(client);

        assertEquals("u2", future.get(5, TimeUnit.SECONDS).getUser());
        assertEquals("token-2", lastRefreshToken());
    }


    @Test
    public void rpcWaitsForInFlightRefresh() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        answerRpcs("after-refresh");
        newAuthClient();
        connect(client);
        server.deferCommand = cmd -> cmd.hasRefresh();

        CompletableFuture<AuthResult> refreshFuture = refresh(client, "token-2");
        CompletableFuture<RPCResult> rpcFuture = new CompletableFuture<>();
        client.rpc("method", EMPTY_PAYLOAD, into(rpcFuture));

        awaitCondition("refresh sent", () -> lastRefreshToken() != null);
        Thread.sleep(200L);
        assertEquals("rpc must wait for the refresh reply", 0, rpcCount());
        assertFalse(rpcFuture.isDone());

        server.releaseDeferredCommands();

        refreshFuture.get(5, TimeUnit.SECONDS);
        assertEquals("after-refresh", new String(rpcFuture.get(5, TimeUnit.SECONDS).getData()));
        assertEquals(1, rpcCount());
    }


    @Test
    public void authEventFiresOnConnectAndWhenTheIdentityChanges() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient();
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
    public void refusedRefreshKeepsTheIdentity() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient();
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

        ReplyError error = authErrorOf(refresh(client, "token-2"));

        assertEquals(PERMISSION_DENIED, error.getCode());
        assertEquals(Arrays.asList("u1/authenticated"), authLog);
        assertEquals(ClientState.CONNECTED, client.getState());
        assertFalse(disconnectedSignals.tryAcquire());
    }


    @Test
    public void connectedEventCarriesTheIdentity() throws Exception {
        server.connectResult = connectResultFor("u1", "authenticated");
        newAuthClient();

        connect(client);

        assertEquals("u1", connectedEvents.get(0).getUser());
        assertEquals("authenticated", connectedEvents.get(0).getAuthClass());
    }


    @Test
    public void legacyServerReportsEmptyIdentity() throws Exception {
        newAuthClient();

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
        newAuthClient();

        client.refresh(null, (error, result) -> {
        });
    }


    private void newAuthClient() {
        client = new Client(server.url(), new Options(), new EventListener() {
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
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        });
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
        c.refresh(token, (error, result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        });
        return future;
    }


    private static ReplyError authErrorOf(CompletableFuture<AuthResult> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (ReplyError) e.getCause();
        }
        throw new AssertionError("refresh succeeded, expected a reply error");
    }


    private String lastRefreshToken() {
        List<Protocol.Command> commands = server.received();
        for (int i = commands.size() - 1; i >= 0; i--) {
            if (commands.get(i).hasRefresh()) {
                return commands.get(i).getRefresh().getToken();
            }
        }
        return null;
    }


    private int rpcCount() {
        int count = 0;
        for (Protocol.Command command : server.received()) {
            if (command.hasRpc()) {
                count++;
            }
        }
        return count;
    }


    private static void awaitCondition(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_MILLIS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + description);
            }
            Thread.sleep(10L);
        }
    }
}
