package io.github.centrifugal.centrifuge;

import java.util.concurrent.CompletableFuture;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ReplyErrorTest extends FakeServerTestBase {
    @Test
    public void nullApplicationFieldsAreTreatedAsAbsent() {
        ReplyError error = new ReplyError(107, "bad request", false, 0, null, null);

        assertEquals(0, error.getAppCode());
        assertEquals("", error.getAppType());
        assertTrue(error.getMetadata().isEmpty());
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
        Client c = connectClient();

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
        Client c = connectClient();

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
        Client c = connectClient();

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
