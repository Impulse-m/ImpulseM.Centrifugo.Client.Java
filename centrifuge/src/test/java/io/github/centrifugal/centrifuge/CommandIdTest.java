package io.github.centrifugal.centrifuge;

import java.util.concurrent.TimeUnit;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;
import java8.util.concurrent.CompletableFuture;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Command ids are uint32 on the wire, so after 2^31 commands they are negative as int. The counter must still skip 0,
 * which is the id of a push or a ping, and a reply with such an id must still answer its call.
 */
public class CommandIdTest extends RpcTestBase {

    @Test
    public void commandIdWrapsWithoutTrapping() throws Exception {
        // The id after 2^32 - 1 commands is 1: the int -1 is the largest uint32, and the next id is never 0.
        assertEquals(-1, Client.nextCommandId(-2));
        assertEquals(1, Client.nextCommandId(-1));
        assertNotEquals(0, Client.nextCommandId(-1));
        assertEquals(Integer.MIN_VALUE, Client.nextCommandId(Integer.MAX_VALUE));

        // A reply with an id above 2^31 (negative as int) completes the call it answers.
        Client c = newClient(new Options());
        connect(c);
        final int id = (int) 0x80000001L;
        CompletableFuture<Protocol.Reply> future = new CompletableFuture<>();
        CompletableFuture<Void> routed = new CompletableFuture<>();
        c.getExecutor().execute(() -> {
            try {
                c.futures.put(id, future);
                c.processReply(Protocol.Reply.newBuilder().setId(id).build());
                routed.complete(null);
            } catch (Exception e) {
                routed.completeExceptionally(e);
            }
        });
        routed.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue("the reply above 2^31 completed its call", future.isDone());
        assertEquals(id, future.get().getId());
    }
}
