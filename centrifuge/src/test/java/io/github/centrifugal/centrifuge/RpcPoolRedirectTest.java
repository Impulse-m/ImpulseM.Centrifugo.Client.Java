package io.github.centrifugal.centrifuge;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class RpcPoolRedirectTest extends RpcTestBase {
    private MockWebServer blobs;
    private RpcPool pool;


    @Before
    public void startBlobs() throws Exception {
        blobs = new MockWebServer();
        blobs.start();
        answerRpcs("ref");
    }


    @After
    public void stopBlobs() throws Exception {
        if (pool != null) {
            pool.close();
        }
        blobs.shutdown();
    }


    @Test
    public void aRedirectIsFetchedAndDeliveredAsTheResult() throws Exception {
        blobs.enqueue(new MockResponse().setBody("chunk-bytes"));
        pool = redirectingPool(RpcPool.Redirect.builder(blobs.url("/blob").toString()).build());
        call();
        assertEquals("/blob", blobs.takeRequest(5, TimeUnit.SECONDS).getPath());
    }


    @Test
    public void aRedirectBodyOverItsBoundFailsWithRedirectFailed() throws Exception {
        blobs.enqueue(new MockResponse().setBody("too-long-body"));
        pool = redirectingPool(RpcPool.Redirect.builder(blobs.url("/blob").toString()).maxBytes(4).build());
        assertEquals(ReplyError.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectAnsweredWithAnErrorStatusFailsWithRedirectFailed() throws Exception {
        blobs.enqueue(new MockResponse().setResponseCode(404));
        pool = redirectingPool(RpcPool.Redirect.builder(blobs.url("/blob").toString()).build());
        assertEquals(ReplyError.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectToAUrlTheEndpointDoesNotAllowFailsWithRedirectFailed() throws Exception {
        // The endpoint is a plain ws:// URL, so the redirect to a non-web scheme is refused before any fetch.
        pool = redirectingPool(RpcPool.Redirect.builder("ftp://blobs.example/blob").build());
        assertEquals(ReplyError.REDIRECT_FAILED, failureOf().getCode());
        assertEquals("no fetch was started", 0, blobs.getRequestCount());
    }


    @Test
    public void theAllowedSchemesFollowTheEndpoint() {
        assertTrue(RpcPoolRedirect.allowed("ws://host/connection/websocket", "http://blobs.example/x"));
        assertTrue(RpcPoolRedirect.allowed("ws://host/connection/websocket", "https://blobs.example/x"));
        assertTrue(RpcPoolRedirect.allowed("wss://host/connection/websocket", "https://blobs.example/x"));
        assertFalse("a secure endpoint takes no plain http redirect", RpcPoolRedirect.allowed("wss://host/connection/websocket", "http://blobs.example/x"));
        assertFalse(RpcPoolRedirect.allowed("ws://host/connection/websocket", "ftp://blobs.example/x"));
    }


    private RpcPool redirectingPool(final RpcPool.Redirect redirect) {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .redirect((reply, error) -> reply != null ? redirect : null)
                .build();
    }


    private RPCResult call() throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("chunk-bytes", new String(result.getData(), StandardCharsets.UTF_8));
        return result;
    }


    private ReplyError failureOf() throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(future));
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (ReplyError) e.getCause();
        }
        fail("the redirect must fail");
        return null;
    }
}
