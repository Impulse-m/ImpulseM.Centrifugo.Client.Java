package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Frames of one megabyte through a pool with a window of two: every frame is received whole and answered,
 * and the calls in flight never exceed the window.
 */
public class RpcPoolLargeFrameTest extends RpcTestBase {
    private static final int PART_BYTES = 1024 * 1024;
    private static final int PARTS = 4;


    @Test
    public void largeFramesAreAnsweredWithinTheWindow() throws Exception {
        enableRpcOptions();
        List<Integer> receivedSizes = Collections.synchronizedList(new ArrayList<Integer>());
        server.onCommand = cmd -> {
            if (!cmd.hasRpc()) {
                return null;
            }
            receivedSizes.add(cmd.getRpc().getData().size());
            return Protocol.Reply.newBuilder()
                    .setId(cmd.getId())
                    .setRpc(Protocol.RPCResult.newBuilder())
                    .build();
        };
        RpcPool pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .maxConnectionsPerEndpoint(1)
                .maxInflightPerConnection(2)
                .maxQueued(PARTS)
                .build();
        try {
            byte[] part = new byte[PART_BYTES];
            List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
            for (int i = 0; i < PARTS; i++) {
                CompletableFuture<RPCResult> future = new CompletableFuture<>();
                pool.rpc("svc.writePart", part, null, into(future));
                futures.add(future);
            }
            for (CompletableFuture<RPCResult> future : futures) {
                future.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
                assertTrue("the window is never exceeded", pool.stats().getInflight() <= 2);
            }
            assertEquals(PARTS, receivedSizes.size());
            for (Integer size : receivedSizes) {
                assertEquals("each frame arrives whole", PART_BYTES, size.intValue());
            }
        } finally {
            pool.close();
        }
    }
}
