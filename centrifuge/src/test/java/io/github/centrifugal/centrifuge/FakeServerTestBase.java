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

import static org.junit.Assert.assertTrue;


/**
 * Shared harness of the suites that talk to the in-process {@link FakeCentrifugoServer}:
 * starts and stops the server, creates clients whose connect and disconnect are signalled,
 * and adapts rpc callbacks to futures.
 */
abstract class FakeServerTestBase {
    protected static final byte[] EMPTY_PAYLOAD = "{}".getBytes(StandardCharsets.UTF_8);


    protected FakeCentrifugoServer server;


    protected Client client;


    protected final Semaphore connectedSignals = new Semaphore(0);


    protected final Semaphore disconnectedSignals = new Semaphore(0);


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


    /** Creates the client for the fake server; it is not connected yet. */
    protected Client newClient() {
        return newClient(new Options());
    }


    /** Creates the client for the fake server with the given options; it is not connected yet. */
    protected Client newClient(Options options) {
        client = new Client(server.url(), options, new EventListener() {
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


    /** Connects the client and waits until it reports connected. */
    protected void connect(Client c) throws InterruptedException {
        c.connect();
        assertTrue("connected", connectedSignals.tryAcquire(5, TimeUnit.SECONDS));
    }


    /** Creates a client and connects it. */
    protected Client connectClient() throws InterruptedException {
        newClient();
        connect(client);
        return client;
    }


    /** Answers every rpc with a result carrying the payload and the server time, other commands by default. */
    protected void answerRpcs(String payload, long time) {
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setTime(time)
                    .setRpc(Protocol.RPCResult.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8(payload)))
                    .build();
        };
    }


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


    /** Answers every rpc with a result carrying the payload, without a server time. */
    protected void answerRpcs(String payload) {
        answerRpcs(payload, 0L);
    }


    /** Adapts an rpc callback to a future: a reply error completes it exceptionally. */
    protected static ResultCallback<RPCResult> into(CompletableFuture<RPCResult> future) {
        return (error, result) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        };
    }


    /** The reply error of a failed rpc future. */
    protected static ReplyError replyErrorOf(CompletableFuture<RPCResult> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (ReplyError) e.getCause();
        }
        throw new AssertionError("rpc succeeded, expected a reply error");
    }
}
