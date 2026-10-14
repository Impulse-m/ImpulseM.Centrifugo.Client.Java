package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

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


    /** Makes the fake server confirm RPC options in its connect reply. */
    protected void enableRpcOptions() {
        server.connectResult = Protocol.ConnectResult.newBuilder()
                .setClient("fake-client").setVersion("0.0.0").setPing(25)
                .setFlag(Extensions.RPC_OPTIONS_FLAG).build();
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


    /** Creates the client for the fake server with the default options, whose connect and disconnect are signalled. */
    @Override
    protected Client newClient() {
        return newClient(new Options());
    }


    /** Creates the client for the fake server with the options and the listener; it is not connected yet. */
    protected Client newClient(Options options, EventListener listener) {
        client = new Client(server.url(), options, listener);
        return client;
    }


    /** Creates the client for the fake server with the options, whose connect and disconnect are signalled. */
    protected Client newClient(Options options) {
        return newClient(options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }


            @Override
            public void onDisconnected(Client c, DisconnectedEvent e) {
                disconnectedSignals.release();
            }
        });
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
        client.presence("sync", into(marker));
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
