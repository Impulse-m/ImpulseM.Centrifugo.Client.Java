package io.github.centrifugal.centrifuge;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * The connect path with a listener which throws. The connect handshake goes on whatever the listener
 * does with its own failure, so the calls queued for the connect are answered.
 */
public class ConnectListenerTest extends RpcTestBase {
    // A listener which throws on connect is reported, and the handshake goes on: the calls queued for the connect are written.
    @Test
    public void aListenerWhichThrowsOnConnectDoesNotSkipTheHandshake() throws Exception {
        enableRpcOptions();
        answerRpcs("ok", System::currentTimeMillis);
        BlockingQueue<Throwable> reported = new LinkedBlockingQueue<>();
        EventListener listener = new EventListener() {
            @Override
            public void onConnected(Client client, ConnectedEvent event) {
                throw new IllegalStateException("listener failed");
            }


            @Override
            public void onError(Client client, ErrorEvent event) {
                reported.add(event.getError());
            }
        };
        Client c = newClient(instanceOptions(), listener);
        CompletableFuture<RPCResult> queued = new CompletableFuture<>();
        c.rpcCall("m", EMPTY_PAYLOAD, into(queued));

        c.connect();

        RPCResult result = queued.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals("ok", new String(result.getData(), StandardCharsets.UTF_8));
        Throwable failure = reported.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("the failure of the listener is reported", failure instanceof UnclassifiedError);
        assertTrue("the listener's exception is its cause", ((UnclassifiedError) failure).getError() instanceof IllegalStateException);
    }
}
