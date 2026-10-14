package io.github.centrifugal.centrifuge;

import java.io.IOException;
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
}
