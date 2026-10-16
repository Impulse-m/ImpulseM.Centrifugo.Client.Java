package io.github.centrifugal.centrifuge;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.OkHttpClient;
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
    private static final char[] TEST_PASSWORD = "changeit".toCharArray();

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
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectAnsweredWithAnErrorStatusFailsWithRedirectFailed() throws Exception {
        blobs.enqueue(new MockResponse().setResponseCode(404));
        pool = redirectingPool(RpcPool.Redirect.builder(blobs.url("/blob").toString()).build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectToAUrlTheEndpointDoesNotAllowFailsWithRedirectFailed() throws Exception {
        // The endpoint is a plain ws:// URL, so the redirect to a non-web scheme is refused before any fetch.
        pool = redirectingPool(RpcPool.Redirect.builder("ftp://blobs.example/blob").build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        assertEquals("no fetch was started", 0, blobs.getRequestCount());
    }


    @Test
    public void aRedirectFromHttpsToHttpIsNotFollowed() throws Exception {
        KeyStore store = testKeyStore();
        SSLContext tls = testTls(store);
        MockWebServer secure = new MockWebServer();
        secure.useHttps(tls.getSocketFactory(), false);
        secure.start();
        try {
            secure.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", blobs.url("/blob").toString()));
            pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                    .random(maximumDraws())
                    .okHttpClient(trustingClient(tls, store))
                    .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(secure.url("/blob").toString()).build() : null)
                    .build();
            assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
            assertEquals("the plain hop is never requested", 0, blobs.getRequestCount());
        } finally {
            secure.shutdown();
        }
    }


    @Test
    public void theRedirectFetchesHoldTheSlotsOfThePool() throws Exception {
        blobs.enqueue(new MockResponse().setBody("one").setBodyDelay(300L, TimeUnit.MILLISECONDS));
        blobs.enqueue(new MockResponse().setBody("two").setBodyDelay(300L, TimeUnit.MILLISECONDS));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxRedirectsInflight(1)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(blobs.url("/blob").toString()).build() : null)
                .build();
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(first));
        pool.rpc("blob.read", EMPTY_PAYLOAD, null, into(second));
        awaitCondition("the first fetch is on the wire", () -> blobs.getRequestCount() == 1);
        assertEquals("one fetch in flight", 1, pool.stats().getRedirects());
        assertEquals("the second fetch waits for the slot", 1, blobs.getRequestCount());
        assertEquals("one", new String(first.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("two", new String(second.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, blobs.getRequestCount());
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


    // The self-signed certificate of the test keystore, for localhost: the server presents it and the client trusts it.
    private static KeyStore testKeyStore() throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = RpcPoolRedirectTest.class.getResourceAsStream("/rpc-pool-test.p12")) {
            store.load(in, TEST_PASSWORD);
        }
        return store;
    }


    private static SSLContext testTls(KeyStore store) throws Exception {
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, TEST_PASSWORD);
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), trust.getTrustManagers(), null);
        return context;
    }


    private static OkHttpClient trustingClient(SSLContext tls, KeyStore store) throws Exception {
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        return new OkHttpClient.Builder()
                .sslSocketFactory(tls.getSocketFactory(), (X509TrustManager) trust.getTrustManagers()[0])
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
