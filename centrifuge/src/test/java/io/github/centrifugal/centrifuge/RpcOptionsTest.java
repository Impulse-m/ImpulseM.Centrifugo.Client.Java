package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RPC options over the fork protocol: the connection capability, per-call headers,
 * the error fields of a reply error. Checked over the wire against the in-process
 * {@link FakeCentrifugoServer}.
 */
public class RpcOptionsTest {

    private static final byte[] EMPTY_PAYLOAD = "{}".getBytes(StandardCharsets.UTF_8);

    private FakeCentrifugoServer server;
    private Client client;
    private final Semaphore connectedSignals = new Semaphore(0);
    private final Semaphore disconnectedSignals = new Semaphore(0);

    @Before
    public void setUp() throws IOException {
        server = new FakeCentrifugoServer();
        server.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (client != null) {
            client.close(1000);
        }
        server.stop();
    }

    private Client newClient() {
        client = new Client(server.url(), new Options(), new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }

            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        });
        return client;
    }

    private void connect(Client c) throws InterruptedException {
        c.connect();
        assertTrue("connected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
    }

    private void confirmRpcOptions() {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setFlag(Extensions.RPC_OPTIONS_FLAG).build();
    }

    @Test
    public void connectRequestAdvertisesRpcOptionsBit() throws Exception {
        Client c = newClient();
        connect(c);

        Protocol.ConnectRequest connectRequest = null;
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                connectRequest = cmd.getConnect();
            }
        }
        assertEquals(Extensions.RPC_OPTIONS_FLAG, connectRequest.getFlag());
    }

    @Test
    public void capabilityIsOnWhenConnectReplyHasRpcOptionsBit() throws Exception {
        confirmRpcOptions();
        Client c = newClient();
        connect(c);

        assertTrue(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilityIsOnWhenRpcOptionsBitComesWithOtherBits() throws Exception {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setFlag(Extensions.RPC_OPTIONS_FLAG | 1L).build();
        Client c = newClient();
        connect(c);

        assertTrue(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilityIsOffWhenConnectReplyHasNoRpcOptionsBit() throws Exception {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setFlag(1L).build();
        Client c = newClient();
        connect(c);

        assertFalse(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilityIsOffWhenConnectReplyHasNoFlag() throws Exception {
        Client c = newClient();
        connect(c);

        assertFalse(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilityIsClearedOnReconnectUntilConnectReplyConfirmsIt() throws Exception {
        confirmRpcOptions();
        Client c = newClient();
        connect(c);
        assertTrue(c.getCapabilities().isRpcOptionsSupported());

        // Hold the reconnect's connect reply, so the capability can be read while the
        // new transport is open but not yet confirmed.
        Semaphore reconnectHeld = new Semaphore(0);
        server.deferCommand = cmd -> {
            if (cmd.hasConnect()) {
                reconnectHeld.release();
                return true;
            }
            return false;
        };
        server.closeConnection();

        assertTrue("reconnect connect command sent", reconnectHeld.tryAcquire(5, TimeUnit.SECONDS));
        assertFalse(c.getCapabilities().isRpcOptionsSupported());

        server.releaseDeferredCommands();
        assertTrue("reconnected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        assertTrue(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilityIsClearedOnDisconnect() throws Exception {
        confirmRpcOptions();
        Client c = newClient();
        connect(c);
        assertTrue(c.getCapabilities().isRpcOptionsSupported());

        c.disconnect();
        assertTrue("disconnected", disconnectedSignals.tryAcquire(5, TimeUnit.SECONDS));

        assertFalse(c.getCapabilities().isRpcOptionsSupported());
    }

    @Test
    public void capabilitySnapshotDoesNotChangeAfterwards() throws Exception {
        confirmRpcOptions();
        Client c = newClient();
        connect(c);
        ClientCapabilities before = c.getCapabilities();

        c.disconnect();
        assertTrue("disconnected", disconnectedSignals.tryAcquire(5, TimeUnit.SECONDS));

        assertTrue(before.isRpcOptionsSupported());
        assertFalse(c.getCapabilities().isRpcOptionsSupported());
    }

    private static ResultCallback<RPCResult> into(CompletableFuture<RPCResult> future) {
        return (error, result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        };
    }

    private static RpcOptions headerOptions() {
        return new RpcOptions.Builder().header("x-request-source", "test").build();
    }

    private static ReplyError replyErrorOf(CompletableFuture<RPCResult> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (ReplyError) e.getCause();
        }
        throw new AssertionError("rpc succeeded, expected a reply error");
    }

    private List<Protocol.RPCRequest> rpcRequests() {
        List<Protocol.RPCRequest> requests = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRpc()) {
                requests.add(cmd.getRpc());
            }
        }
        return requests;
    }

    // Answers every rpc with a result carrying the payload, other commands by default.
    private void answerRpcs(String payload) {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRpc(Protocol.RPCResult.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8(payload)))
                    .build();
        };
    }

    @Test
    public void rpcWithHeadersIsSentAndResultDelivered() throws Exception {
        confirmRpcOptions();
        answerRpcs("pong");
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, headerOptions(), into(future));

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("pong", new String(result.getData(), StandardCharsets.UTF_8));
        List<Protocol.RPCRequest> sent = rpcRequests();
        assertEquals(1, sent.size());
        assertEquals("test", sent.get(0).getHeadersMap().get("x-request-source"));
        assertEquals(1, sent.get(0).getHeadersCount());
    }

    @Test
    public void rpcWithHeadersFailsWithoutCapabilityAndIsNotSent() throws Exception {
        answerRpcs("pong");
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, headerOptions(), into(future));

        ReplyError error = replyErrorOf(future);
        assertEquals(ReplyError.UNSUPPORTED, error.getCode());
        assertEquals("rpc headers are not supported by the server", error.getMessage());
        assertTrue("nothing sent", rpcRequests().isEmpty());
    }

    @Test
    public void plainRpcWorksWithoutCapability() throws Exception {
        answerRpcs("pong");
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("pong", new String(result.getData(), StandardCharsets.UTF_8));
        assertEquals(1, rpcRequests().size());
        assertEquals(0, rpcRequests().get(0).getHeadersCount());
    }

    @Test
    public void plainRpcWorksWithCapability() throws Exception {
        confirmRpcOptions();
        answerRpcs("pong");
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, null, into(future));

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("pong", new String(result.getData(), StandardCharsets.UTF_8));
        assertEquals(0, rpcRequests().get(0).getHeadersCount());
    }

    @Test
    public void emptyOptionsAreAPlainCallWithoutCapability() throws Exception {
        answerRpcs("pong");
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, new RpcOptions.Builder().build(), into(future));

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("pong", new String(result.getData(), StandardCharsets.UTF_8));
        assertEquals(0, rpcRequests().get(0).getHeadersCount());
    }

    // Starts a connect that the server holds, queues an rpc while it is connecting, then
    // lets the connect reply through. Returns once the rpc is queued.
    private CompletableFuture<RPCResult> rpcQueuedWhileConnecting(Client c, RpcOptions options) throws Exception {
        Semaphore connectHeld = new Semaphore(0);
        server.deferCommand = cmd -> {
            if (cmd.hasConnect()) {
                connectHeld.release();
                return true;
            }
            return false;
        };
        c.connect();
        assertTrue("connect held", connectHeld.tryAcquire(5, TimeUnit.SECONDS));

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, options, into(future));
        // Tasks run in submission order on the client executor, so this returns once the
        // rpc above has been queued.
        c.getExecutor().submit(() -> {
        }).get(5, TimeUnit.SECONDS);

        server.releaseDeferredCommands();
        assertTrue("connected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
        return future;
    }

    @Test
    public void rpcWithHeadersQueuedWhileConnectingIsSentWhenConnectReplyConfirmsIt() throws Exception {
        confirmRpcOptions();
        answerRpcs("pong");
        Client c = newClient();

        CompletableFuture<RPCResult> future = rpcQueuedWhileConnecting(c, headerOptions());

        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("pong", new String(result.getData(), StandardCharsets.UTF_8));
        assertEquals("test", rpcRequests().get(0).getHeadersMap().get("x-request-source"));
    }

    @Test
    public void rpcWithHeadersQueuedWhileConnectingFailsWhenConnectReplyDoesNotConfirmIt() throws Exception {
        answerRpcs("pong");
        Client c = newClient();

        CompletableFuture<RPCResult> future = rpcQueuedWhileConnecting(c, headerOptions());

        ReplyError error = replyErrorOf(future);
        assertEquals(ReplyError.UNSUPPORTED, error.getCode());
        assertTrue("nothing sent", rpcRequests().isEmpty());
    }

    @Test
    public void replyErrorCarriesApplicationFields() throws Exception {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setError(Protocol.Error.newBuilder()
                            .setCode(107)
                            .setMessage("bad request")
                            .setAppCode(42)
                            .setAppType("example")
                            .putMetadata("reason", "demo"))
                    .build();
        };
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);
        assertEquals(107, error.getCode());
        assertEquals("bad request", error.getMessage());
        assertEquals(42, error.getAppCode());
        assertEquals("example", error.getAppType());
        assertEquals(1, error.getMetadata().size());
        assertEquals("demo", error.getMetadata().get("reason"));
    }

    @Test
    public void replyErrorApplicationFieldsAreEmptyWhenAbsent() throws Exception {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setError(Protocol.Error.newBuilder().setCode(107).setMessage("bad request"))
                    .build();
        };
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);
        assertEquals(0, error.getAppCode());
        assertEquals("", error.getAppType());
        assertTrue(error.getMetadata().isEmpty());
    }

    @Test
    public void replyErrorMetadataIsUnmodifiable() throws Exception {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setError(Protocol.Error.newBuilder().setCode(107).putMetadata("reason", "demo"))
                    .build();
        };
        Client c = newClient();
        connect(c);

        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        c.rpc("method", EMPTY_PAYLOAD, into(future));

        ReplyError error = replyErrorOf(future);
        try {
            error.getMetadata().put("other", "value");
            throw new AssertionError("metadata map is modifiable");
        } catch (UnsupportedOperationException expected) {
            assertEquals(1, error.getMetadata().size());
        }
    }
}
