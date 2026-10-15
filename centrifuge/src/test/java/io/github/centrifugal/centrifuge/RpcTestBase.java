package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Harness of the suites for exactly-once calls and the connection identity. On top of
 * {@link FakeServerTestBase} it adds the RPC options handshake, the requests the fake server
 * received, clients with their own listener, and polling of the fake server.
 */
abstract class RpcTestBase extends FakeServerTestBase {
    protected static final long WAIT_MILLIS = 5_000L;
    // The instance the exactly-once tests set, so that their calls are tracked.
    protected static final String TEST_INSTANCE = "abcdefghijklmnopqrstuv";


    /** Makes the fake server confirm RPC options in its connect reply. */
    protected void enableRpcOptions() {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setFlag(Extensions.RPC_OPTIONS_FLAG).build();
    }


    /**
     * Answers every rpc with a result carrying the payload and the server time, read when each
     * answer is sent, other commands by default.
     */
    protected void answerRpcs(String payload, LongSupplier serverTime) {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setTime(serverTime.getAsLong())
                    .setRpc(Protocol.RPCResult.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8(payload)))
                    .build();
        };
    }


    /** The rpc requests the client sent, in order. */
    protected List<Protocol.RPCRequest> sentRpcRequests() {
        List<Protocol.RPCRequest> requests = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasRpc()) {
                requests.add(cmd.getRpc());
            }
        }
        return requests;
    }


    /** The connect request the client sent last. Fails when the client sent none. */
    protected Protocol.ConnectRequest lastConnectRequest() {
        List<Protocol.Command> commands = server.received();
        for (int i = commands.size() - 1; i >= 0; i--) {
            if (commands.get(i).hasConnect()) {
                return commands.get(i).getConnect();
            }
        }
        throw new AssertionError("no connect request was sent");
    }


    /**
     * Creates the client for the fake server with the instance set, whose connect and disconnect are
     * signalled. Its calls are tracked once the server confirms RPC options.
     */
    @Override
    protected Client newClient() {
        return newClient(instanceOptions());
    }


    /** Options with the test instance set. */
    protected static Options instanceOptions() {
        Options options = new Options();
        options.setInstance(TEST_INSTANCE);
        return options;
    }


    /** Creates the client for the fake server with the options and the listener; it is not connected yet. */
    protected Client newClient(Options options, EventListener listener) {
        client = new Client(server.url(), options, listener);
        return client;
    }


    /** Creates the client for the fake server with the options, whose connect and disconnect are signalled. */
    protected Client newClient(Options options) {
        return newClient(options, signallingListener());
    }


    /**
     * Creates the client for the fake server with the options, whose timers run on the clock: the test
     * moves the clock with {@link ManualScheduler#advance}. The client is not connected yet.
     */
    protected Client newClientOnClock(Options options, ManualScheduler clock) {
        client = new Client(server.url(), options, signallingListener(), AuthRefresh.RETRY_MIN_MILLIS, AuthRefresh.RETRY_MAX_MILLIS, clock);
        return client;
    }


    /** A listener which signals the connect and the disconnect, for a test which adds its own callbacks around it. */
    protected EventListener signallingListener() {
        return new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        };
    }


    /** Waits until the condition holds, or fails after {@link #WAIT_MILLIS}. */
    protected static void awaitCondition(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_MILLIS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + description);
            }
            Thread.sleep(10L);
        }
    }


    /** Waits until the fake server has received the given number of rpc requests. */
    protected void awaitRequestCount(int count) throws InterruptedException {
        awaitCondition(count + " rpc requests", () -> sentRpcRequests().size() >= count);
        assertEquals(count, sentRpcRequests().size());
    }


    /** Waits until the fake server has received the cancel of the msg_id, and returns it. */
    protected Protocol.RPCRequest awaitCancel(long msgId) throws InterruptedException {
        awaitCondition("cancel of msg_id " + msgId, () -> findCancel(msgId) != null);
        return findCancel(msgId);
    }


    /** Adapts a callback to a future: an error completes it exceptionally. */
    protected static <T> ResultCallback<T> completeInto(CompletableFuture<T> future) {
        return (Throwable error, T result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        };
    }


    /** The error of a call which failed. Fails when the call succeeded. */
    protected static <T> Throwable errorOf(CompletableFuture<T> future) throws Exception {
        try {
            future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        throw new AssertionError("the call succeeded, expected an error");
    }


    /**
     * Returns once every command submitted to the client executor before this call has run, so
     * a test can assert on the client state after its calls were handled.
     */
    protected void drainExecutor() throws Exception {
        client.getExecutor().submit(() -> {
        }).get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }


    /**
     * Returns once the fake server has handled every command the client wrote before this call.
     * The server handles the commands of a connection in order, so the reply to a marker command
     * comes after them. Use it before asserting that the server did not receive a command.
     */
    protected void syncWithServer() throws Exception {
        CompletableFuture<PresenceResult> marker = new CompletableFuture<>();
        client.presence("sync", completeInto(marker));
        marker.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }


    /** Answers every refresh with the reply, other commands by default. The id of each answer is the id of its command. */
    protected void answerRefreshWith(Protocol.Reply.Builder reply) {
        server.onCommand = (Protocol.Command cmd) -> {
            if (!cmd.hasRefresh()) {
                return null;
            }
            return reply.clone().setId(cmd.getId()).build();
        };
    }


    private Protocol.RPCRequest findCancel(long msgId) {
        for (Protocol.RPCRequest request : sentRpcRequests()) {
            if (request.getCancel() == msgId) {
                return request;
            }
        }
        return null;
    }
}
