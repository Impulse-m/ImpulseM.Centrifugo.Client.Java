package io.github.centrifugal.centrifuge;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import okio.BufferedSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class RpcPoolRedirectTest extends RpcTestBase {
    // The password of the test keystore, which holds a self-signed certificate for localhost and its key. A test fixture only.
    private static final char[] TEST_PASSWORD = "changeit".toCharArray();

    private MockWebServer redirects;
    private RpcPool pool;


    @Before
    public void startBlobs() throws Exception {
        redirects = new MockWebServer();
        redirects.start();
        answerRpcs("ref");
    }


    @After
    public void stopBlobs() throws Exception {
        if (pool != null) {
            pool.close();
        }
        redirects.shutdown();
    }


    @Test
    public void aRedirectIsFetchedAndDeliveredAsTheResult() throws Exception {
        redirects.enqueue(new MockResponse().setBody("chunk-bytes"));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build());
        call();
        assertEquals("/data", redirects.takeRequest(5, TimeUnit.SECONDS).getPath());
    }


    @Test
    public void aRedirectBodyOverItsBoundFailsWithRedirectFailed() throws Exception {
        redirects.enqueue(new MockResponse().setBody("too-long-body"));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).maxBytes(4).build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectAnsweredWithAnErrorStatusFailsWithRedirectFailed() throws Exception {
        redirects.enqueue(new MockResponse().setResponseCode(404));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
    }


    @Test
    public void aRedirectToAUrlTheEndpointDoesNotAllowFailsWithRedirectFailed() throws Exception {
        // The endpoint is a plain ws:// URL, so the redirect to a non-web scheme is refused before any fetch.
        pool = redirectingPool(RpcPool.Redirect.builder("ftp://host.example/blob").build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        assertEquals("no fetch was started", 0, redirects.getRequestCount());
    }


    @Test
    public void aRedirectFromHttpsToHttpIsNotFollowed() throws Exception {
        KeyStore store = testKeyStore();
        SSLContext tls = testTls(store);
        MockWebServer secure = new MockWebServer();
        secure.useHttps(tls.getSocketFactory(), false);
        secure.start();
        try {
            secure.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", redirects.url("/data").toString()));
            pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                    .random(maximumDraws())
                    .okHttpClient(trustingClient(tls, store))
                    .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(secure.url("/data").toString()).build() : null)
                    .build();
            assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
            assertEquals("the plain hop is never requested", 0, redirects.getRequestCount());
        } finally {
            secure.shutdown();
        }
    }


    @Test
    public void theHeadersOfTheRedirectGoWithItsRequest() throws Exception {
        redirects.enqueue(new MockResponse().setBody("chunk-bytes"));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).header("x-example-key", "secret").build());
        call();
        assertEquals("secret", redirects.takeRequest(5, TimeUnit.SECONDS).getHeader("x-example-key"));
    }


    @Test
    public void redirect3xxFailsTheFetch() throws Exception {
        MockWebServer other = new MockWebServer();
        other.start();
        try {
            redirects.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", other.url("/final").toString()));
            other.enqueue(new MockResponse().setBody("chunk-bytes"));
            pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build());
            assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
            assertEquals("the redirect answer is the only request", 1, redirects.getRequestCount());
            assertEquals("the URL the answer names is never requested", 0, other.getRequestCount());
        } finally {
            other.shutdown();
        }
    }


    @Test
    public void everyRedirectStatusFailsWithRedirectFailed() throws Exception {
        for (int status : new int[] {301, 302, 303, 307, 308}) {
            redirects.enqueue(new MockResponse().setResponseCode(status).setHeader("Location", "/data"));
        }
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build());
        for (int call = 0; call < 5; call++) {
            assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        }
        assertEquals("each answer is one request, none is followed", 5, redirects.getRequestCount());
    }


    @Test
    public void redirectWaiterCountsInMaxQueued() throws Exception {
        // With maxQueued 0 a second fetch has no room to wait: it fails at once, and the first one is not disturbed.
        RpcPool none = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxInflightPerConnection(8)
                .maxQueued(0)
                .maxRedirectsInflight(1)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(redirects.url("/data").toString()).build() : null)
                .build();
        try {
            redirects.enqueue(new MockResponse().setBody("slow").setBodyDelay(1000L, TimeUnit.MILLISECONDS));
            CompletableFuture<RPCResult> slow = new CompletableFuture<>();
            none.rpc("svc.read", EMPTY_PAYLOAD, null, into(slow));
            awaitCondition("the first fetch is on the wire", () -> none.stats().getRedirects() == 1);
            CompletableFuture<RPCResult> refused = new CompletableFuture<>();
            none.rpc("svc.read", EMPTY_PAYLOAD, null, into(refused));
            assertEquals(ReplyError.LIMIT_EXCEEDED, ((ReplyError) errorOf(refused)).getCode());
            assertEquals("slow", new String(slow.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        } finally {
            none.close();
        }
        redirects.enqueue(new MockResponse().setBody("one").setBodyDelay(1000L, TimeUnit.MILLISECONDS));
        redirects.enqueue(new MockResponse().setBody("two"));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxInflightPerConnection(8)
                .maxQueued(1)
                .maxRedirectsInflight(1)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(redirects.url("/data").toString()).build() : null)
                .build();
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        awaitCondition("the first fetch is on the wire", () -> pool.stats().getRedirects() == 1);
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        awaitCondition("the second fetch waits and is counted as queued", () -> pool.stats().getQueued() == 1);
        assertEquals("the fetch over the bound is refused", ReplyError.LIMIT_EXCEEDED, failureOf().getCode());
        assertEquals("one", new String(first.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("two", new String(second.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("the refused fetches were never requested", 3, redirects.getRequestCount());
        assertEquals(0, pool.stats().getQueued());
    }


    @Test
    public void aFetchWithAFreeSlotIsNotRefusedWhenMaxQueuedIsZero() throws Exception {
        redirects.enqueue(new MockResponse().setBody("chunk-bytes"));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxQueued(0)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(redirects.url("/data").toString()).build() : null)
                .build();
        call();
    }


    @Test
    public void aRedirectWithoutALocationFailsWithRedirectFailed() throws Exception {
        redirects.enqueue(new MockResponse().setResponseCode(302));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build());
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        assertEquals(1, redirects.getRequestCount());
    }


    @Test
    public void theRedirectFetchesHoldTheSlotsOfThePool() throws Exception {
        redirects.enqueue(new MockResponse().setBody("one").setBodyDelay(300L, TimeUnit.MILLISECONDS));
        redirects.enqueue(new MockResponse().setBody("two").setBodyDelay(300L, TimeUnit.MILLISECONDS));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxRedirectsInflight(1)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(redirects.url("/data").toString()).build() : null)
                .build();
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        awaitCondition("the first fetch is on the wire", () -> redirects.getRequestCount() == 1);
        assertEquals("one fetch in flight", 1, pool.stats().getRedirects());
        assertEquals("the second fetch waits for the slot", 1, redirects.getRequestCount());
        assertEquals("one", new String(first.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals("two", new String(second.get(5, TimeUnit.SECONDS).getData(), StandardCharsets.UTF_8));
        assertEquals(2, redirects.getRequestCount());
    }


    @Test
    public void httpRedirectIsRefusedFromASecureEndpoint() {
        assertTrue(RpcPoolRedirect.allowed("ws://host/connection/websocket", "http://host.example/x"));
        assertTrue(RpcPoolRedirect.allowed("ws://host/connection/websocket", "https://host.example/x"));
        assertTrue(RpcPoolRedirect.allowed("wss://host/connection/websocket", "https://host.example/x"));
        assertFalse("a wss endpoint takes no plain http redirect", RpcPoolRedirect.allowed("wss://host/connection/websocket", "http://host.example/x"));
        assertFalse("an https endpoint takes no plain http redirect", RpcPoolRedirect.allowed("https://host/x", "http://host.example/x"));
        assertFalse(RpcPoolRedirect.allowed("ws://host/connection/websocket", "ftp://host.example/x"));
    }


    // T34
    @Test
    public void redirectFetchCarriesNoApplicationCredentials() throws Exception {
        OkHttpClient application = new OkHttpClient.Builder()
                .addInterceptor(chain -> chain.proceed(chain.request().newBuilder().header("Authorization", "app-secret").build()))
                .cookieJar(new CookieJar() {
                    @Override
                    public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
                    }


                    @Override
                    public List<Cookie> loadForRequest(HttpUrl url) {
                        return Collections.singletonList(new Cookie.Builder().name("session").value("secret").domain(url.host()).build());
                    }
                })
                .authenticator((route, response) -> response.request().newBuilder().header("Authorization", "retry-secret").build())
                .build();
        redirects.enqueue(new MockResponse().setResponseCode(401));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .okHttpClient(application)
                .redirect((reply, error) -> reply != null
                        ? RpcPool.Redirect.builder(redirects.url("/data").toString()).header("x-example-key", "secret").build()
                        : null)
                .build();
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        RecordedRequest request = redirects.takeRequest(5, TimeUnit.SECONDS);
        assertNull("no application header", request.getHeader("Authorization"));
        assertNull("no cookie of the application", request.getHeader("Cookie"));
        assertEquals("the redirect header goes with the request", "secret", request.getHeader("x-example-key"));
        assertEquals("the 401 is not retried with credentials", 1, redirects.getRequestCount());
    }


    @Test
    public void closeSettlesAWaitingFetchWithTheClosedError() throws Exception {
        // The first fetch is held open by the fixture, so the second call waits for the one fetch slot.
        redirects.enqueue(new MockResponse().setBody("held").setBodyDelay(5, TimeUnit.SECONDS));
        pool = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .maxRedirectsInflight(1)
                .redirect((reply, error) -> reply != null ? RpcPool.Redirect.builder(redirects.url("/data").toString()).build() : null)
                .build();
        CompletableFuture<RPCResult> first = new CompletableFuture<>();
        CompletableFuture<RPCResult> second = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(first));
        awaitCondition("the first fetch is on the wire", () -> redirects.getRequestCount() == 1);
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(second));
        awaitCondition("the second fetch waits for the slot", () -> pool.stats().getQueued() == 1);
        pool.close();
        assertEquals("rpc pool closed", errorOf(first).getMessage());
        assertEquals("a waiting fetch settles with the closed error too", "rpc pool closed", errorOf(second).getMessage());
        assertEquals("the second fetch is never requested", 1, redirects.getRequestCount());
    }


    // T43
    @Test
    public void redirectFailureKeepsItsCauseAndReleasesTheBody() throws Exception {
        // Each answer of a fetch passes through the spy, which wraps its body and records the close of that body.
        BodyCounter bodies = new BodyCounter();
        // A body which breaks after its first chunk: the read error stays the cause of the redirect error.
        redirects.enqueue(new MockResponse().setBody("first-chunk").setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).build(), null, bodies);
        ReplyError broken = failureOf();
        assertEquals(RpcPool.REDIRECT_FAILED, broken.getCode());
        assertNotNull("the read error is the cause", broken.getCause());
        assertEquals("the broken fetch opened one body", 1, bodies.opened.get());
        assertEquals("the body of the broken fetch is closed by the pool", 1, bodies.closed.get());
        // A declared length over the bound: refused before the body is read, and its body is closed.
        pool.close();
        redirects.enqueue(new MockResponse().setBody("chunk-bytes"));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).maxBytes(10L).build(), null, bodies);
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        assertEquals("the refused fetch opened one body", 2, bodies.opened.get());
        assertEquals("the body of the refused fetch is closed by the pool", 2, bodies.closed.get());
        // A chunked body which passes the bound while it is read.
        pool.close();
        redirects.enqueue(new MockResponse().setChunkedBody("chunk-bytes", 4));
        pool = redirectingPool(RpcPool.Redirect.builder(redirects.url("/data").toString()).maxBytes(10L).build(), null, bodies);
        assertEquals(RpcPool.REDIRECT_FAILED, failureOf().getCode());
        assertEquals("the bounded fetch opened one body", 3, bodies.opened.get());
        assertEquals("the body of the bounded fetch is closed by the pool", 3, bodies.closed.get());
    }


    private RpcPool redirectingPool(final RpcPool.Redirect redirect) {
        return redirectingPool(redirect, null);
    }


    private RpcPool redirectingPool(final RpcPool.Redirect redirect, final OkHttpClient client) {
        return redirectingPool(redirect, client, null);
    }


    private RpcPool redirectingPool(final RpcPool.Redirect redirect, final OkHttpClient client, final BodyCounter bodies) {
        RpcPool.Builder builder = new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .random(maximumDraws())
                .redirect((reply, error) -> reply != null ? redirect : null);
        if (client != null) {
            builder.okHttpClient(client);
        }
        if (bodies != null) {
            builder.responseTap(bodies::wrap);
        }
        return builder.build();
    }


    // Wraps the body of each answer of a fetch in a spy, which counts the bodies opened and the bodies closed.
    private static final class BodyCounter {
        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();


        Response wrap(final Response answer) {
            opened.incrementAndGet();
            return answer.newBuilder().body(new BodySpy(answer.body(), this)).build();
        }
    }


    // A body which records its close, and passes everything else to the body it wraps.
    private static final class BodySpy extends ResponseBody {
        private final ResponseBody delegate;
        private final BodyCounter counter;


        BodySpy(ResponseBody delegate, BodyCounter counter) {
            this.delegate = delegate;
            this.counter = counter;
        }


        @Override
        public MediaType contentType() {
            return delegate.contentType();
        }


        @Override
        public long contentLength() {
            return delegate.contentLength();
        }


        @Override
        public BufferedSource source() {
            return delegate.source();
        }


        @Override
        public void close() {
            counter.closed.incrementAndGet();
            delegate.close();
        }
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
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("chunk-bytes", new String(result.getData(), StandardCharsets.UTF_8));
        return result;
    }


    private ReplyError failureOf() throws Exception {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (ReplyError) e.getCause();
        }
        fail("the redirect must fail");
        return null;
    }
}
