package io.github.centrifugal.centrifuge;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * The rpc pool which takes its token from the main client (tokenFrom), on the fake server. The main client holds a
 * static token or none, so these tests cover the lending of the token, the waiting for the main client, and the
 * configuration. The renewal, identity and logout rules are covered by the token exchange tests of the flow.
 */
public class RpcPoolTokenTest extends RpcTestBase {
    private static final String ACCESS_TOKEN = "access-1";

    private final List<RpcPool> pools = new ArrayList<>();


    @After
    public void closePools() {
        for (RpcPool pool : pools) {
            pool.close();
        }
    }


    @Test
    public void poolConnectionsConnectWithTheClientsToken() throws Exception {
        server.deferCommand = cmd -> cmd.hasRpc();
        Client main = clientWithToken(ACCESS_TOKEN);
        connect(main);
        RpcPool pool = build(builder(main).maxConnectionsPerEndpoint(2));
        List<CompletableFuture<RPCResult>> futures = new ArrayList<>();
        futures.add(submit(pool));
        futures.add(submit(pool));
        awaitCondition("two connections of the pool", () -> pool.stats().getConnections() == 2);
        // The main connection carries the token too, and the two pool connections follow it.
        awaitCondition("three connects", () -> connectTokens().size() == 3);
        List<String> tokens = connectTokens();
        for (String token : tokens) {
            assertEquals(ACCESS_TOKEN, token);
        }
        awaitCondition("every call answered", () -> {
            server.releaseDeferredCommands();
            for (CompletableFuture<RPCResult> future : futures) {
                if (!future.isDone()) {
                    return false;
                }
            }
            return true;
        });
    }


    @Test
    public void aPoolConnectionWaitsForTheMainClientToConnect() throws Exception {
        answerRpcs("payload");
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool pool = build(builder(main));
        CompletableFuture<RPCResult> future = submit(pool);
        // The main client has not connected yet: the pool connection asks for a token and waits for it.
        awaitCondition("the pool connection is opened", () -> pool.stats().getConnections() == 1);
        connect(main);
        RPCResult result = future.get(5, TimeUnit.SECONDS);
        assertEquals("payload", new String(result.getData(), StandardCharsets.UTF_8));
        List<String> tokens = connectTokens();
        assertEquals(ACCESS_TOKEN, tokens.get(tokens.size() - 1));
    }


    @Test
    public void anAnonymousMainClientGivesAnonymousPoolConnections() throws Exception {
        answerRpcs("payload");
        Client main = clientWithToken("");
        connect(main);
        RpcPool pool = build(builder(main));
        RPCResult result = submit(pool).get(5, TimeUnit.SECONDS);
        assertEquals("payload", new String(result.getData(), StandardCharsets.UTF_8));
        List<String> tokens = connectTokens();
        assertEquals("", tokens.get(tokens.size() - 1));
    }


    @Test
    public void theSourceIsSharedByTheClientsPools() {
        Client main = clientWithToken(ACCESS_TOKEN);
        RpcPool first = build(builder(main));
        RpcPool second = build(builder(main));
        assertNotNull(first);
        assertNotNull(second);
        assertSame(main.auth.tokenSource(), main.auth.tokenSource());
    }


    @Test
    public void tokenFromIsExclusiveWithAnOwnToken() {
        Client main = clientWithToken(ACCESS_TOKEN);
        assertThrowsIllegalArgument(builder(main).token("other"));
        assertThrowsIllegalArgument(builder(main).tokenGetter(new ConnectionTokenGetter() {
        }));
        assertThrowsIllegalArgument(new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .tokenFrom(main)
                .tokenGetter(new ConnectionTokenGetter() {
                }));
    }


    @Test
    public void tokenFromNeedsAClient() {
        try {
            new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url())).tokenFrom(null);
            fail("a null client is refused");
        } catch (NullPointerException expected) {
            assertTrue(expected.getMessage() != null);
        }
    }


    private static void assertThrowsIllegalArgument(RpcPool.Builder builder) {
        try {
            builder.build();
            fail("the builder refuses an own token with tokenFrom");
        } catch (IllegalArgumentException expected) {
            assertEquals("tokenFrom is exclusive with token and tokenGetter", expected.getMessage());
        }
    }


    private Client clientWithToken(String token) {
        Options options = new Options();
        options.setToken(token);
        client = new Client(server.url(), options, new EventListener() {
            @Override
            public void onConnected(Client c, ConnectedEvent e) {
                connectedSignals.release();
            }
        });
        return client;
    }


    private RpcPool.Builder builder(Client main) {
        return new RpcPool.Builder(endpointKey -> Collections.singletonList(server.url()))
                .tokenFrom(main);
    }


    private RpcPool build(RpcPool.Builder builder) {
        RpcPool pool = builder.build();
        pools.add(pool);
        return pool;
    }


    private static CompletableFuture<RPCResult> submit(RpcPool pool) {
        CompletableFuture<RPCResult> future = new CompletableFuture<>();
        pool.rpc("svc.read", EMPTY_PAYLOAD, null, into(future));
        return future;
    }


    // The tokens of the connect commands the server received, in order.
    private List<String> connectTokens() {
        List<String> tokens = new ArrayList<>();
        for (Protocol.Command cmd : server.received()) {
            if (cmd.hasConnect()) {
                tokens.add(cmd.getConnect().getToken());
            }
        }
        return tokens;
    }
}
