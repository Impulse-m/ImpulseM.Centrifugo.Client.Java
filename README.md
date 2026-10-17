# ImpulseM.Centrifugo.Client.Java

Android and Java client for the ImpulseM build of [Centrifugo](https://github.com/Impulse-m/ImpulseM.Centrifugo) (Docker: `ghcr.io/impulse-m/impulsem.centrifugo`).

This is the ImpulseM fork of [centrifuge-java](https://github.com/centrifugal/centrifuge-java). The API matches upstream.

Sibling ImpulseM SDKs:

- JavaScript: [ImpulseM.Centrifugo.Client.JS](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS), npm `@impulsem/centrifuge`
- Swift: [ImpulseM.Centrifugo.Client.Swift](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Swift), Swift Package Manager

There is no v1 release of this library yet – API still evolves. At the moment patch version updates only contain backwards compatible changes, minor version updates can have backwards incompatible API changes.

Check out [client SDK API specification](https://centrifugal.dev/docs/transports/client_api) to learn how this SDK behaves. It's recommended to read that before starting to work with this SDK as the spec covers common SDK behavior - describes client and subscription state transitions, main options and methods. Also check out examples folder.

The features implemented by this SDK can be found in [SDK feature matrix](https://centrifugal.dev/docs/transports/client_sdk#sdk-feature-matrix).

> **The latest `centrifuge-java` is compatible with [Centrifugo](https://github.com/centrifugal/centrifugo) server v6, v5 and v4 and [Centrifuge](https://github.com/centrifugal/centrifuge) >= 0.25.0. For Centrifugo v2, Centrifugo v3 and Centrifuge < 0.25.0 you should use `centrifuge-java` v0.1.0.**

## Installation

The library is published to a Maven repository hosted on GitHub Pages. No account or token is needed.

Gradle (Groovy DSL):

```groovy
repositories {
    maven { url "https://impulse-m.github.io/ImpulseM.Centrifugo.Client.Java/" }
}

dependencies {
    implementation "com.github.Impulse-m:ImpulseM.Centrifugo.Client.Java:1.0.0"
}
```

Gradle (Kotlin DSL):

```kotlin
repositories {
    maven("https://impulse-m.github.io/ImpulseM.Centrifugo.Client.Java/")
}
```

Released versions are listed in [GitHub Releases](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java/releases). Each release is also available in the Maven repository above, with sources and Javadoc jars alongside the library.

This library depends on `streamsupport-minifuture`. In case your project has a dependency to `streamsupport-cfuture` and you have a class name conflicts, you can exclude minifuture safely. Example for Gradle:

```
implementation('com.github.Impulse-m:ImpulseM.Centrifugo.Client.Java:{version}') {
    exclude group: 'net.sourceforge.streamsupport', module: 'streamsupport-minifuture'
}
```

## Javadoc

Javadoc is published as `ImpulseM.Centrifugo.Client.Java-{version}-javadoc.jar` in the Maven repository above.

## Obfuscation with ProGuard

Centrifuge-java library uses Protobuf library (Lite version) for client protocol. This fact and the fact that Protobuf Lite uses reflection internally can cause connection errors when releasing your application with Android shrinking and obfuscation features enabled. See [protocolbuffers/protobuf#6463](https://github.com/protocolbuffers/protobuf/issues/6463) for details. To deal with this centrifuge-java comes with Proguard rules [included in the jar](centrifuge/src/main/resources/META-INF/proguard).

More information [about Android shrinking](https://developer.android.com/studio/build/shrink-code).

## Protobuf classpath conflicts

Centrifuge-java pulls in `com.google.protobuf:protobuf-javalite`. If your app already depends on the full `com.google.protobuf:protobuf-java` (commonly via gRPC, Firebase, or another library), the two artifacts contain overlapping classes and the build will fail with `Duplicate class com.google.protobuf.*` errors, or — worse — the app builds but a `NoSuchMethodError` / `NoSuchFieldError` is thrown at runtime when the SDK builds its protobuf messages, leaving the connection in a `stale` state with no obvious reason in logs.

If you hit this, exclude the *other* library's bundled protobuf so that the lite version provided by centrifuge-java is the only one on the classpath:

```gradle
implementation('com.example:other-library:x.y.z') {
    exclude group: 'com.google.protobuf', module: 'protobuf-java'
    exclude group: 'com.google.protobuf', module: 'protobuf-lite'
}
```

Starting in `0.6.0`, an `Error` (e.g. `NoSuchMethodError`) thrown from generated protobuf code is surfaced through the SDK's `onError` event and disconnects with code `bad protocol`, instead of silently failing in the executor — so a misconfigured classpath produces an actionable stack trace.

## Basic usage

See example code in [console Java example](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java/blob/master/example/src/main/java/io/github/centrifugal/centrifuge/example/Main.java) or in [demo Android app](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java/blob/master/demo/src/main/java/io/github/centrifugal/centrifuge/demo/MainActivity.java)

To use with Android don't forget to set INTERNET permission to `AndroidManifest.xml`:

```
<uses-permission android:name="android.permission.INTERNET" />
```

## Usage in background

When a mobile application goes to the background there are OS-specific limitations for established persistent connections - which can be silently closed shortly. Thus in most cases you need to disconnect from a server when app moves to the background and connect again when app goes to the foreground.

## Subscription bandwidth

**Publish debouncing.** When the server returns a publish debounce window in the subscribe result, `Subscription.publish` coalesces rapid publishes. The first publish is sent at once and starts a window of that length. Publishes inside the window replace the pending value and complete at once with an empty result. When the window ends with a pending value, the latest one is sent and a new window starts; a window which ends with nothing pending is closed. The window is off unless the server sets it. Debouncing is fire-and-forget: pending data is dropped on `unsubscribe()` and on disconnect, and an error of a publish sent when the window ends is not reported to the application.

**Channel compaction.** The client always offers channel compaction on subscribe. When the server allows it, the subscribe result carries a numeric channel id, and publication, join and leave pushes carry that id instead of the channel name. It is negotiated automatically and needs no option.

**Delta without positioning.** `SubscriptionOptions.setDelta` asks for delta compression. A subscription without positioning or recovery gets delta too, when the server keeps the latest publication of each channel: the first publication after subscribe is full, and each later one is a patch against the previous publication the client received. The publisher must send with delta for this (`delta_publish` on the namespace, or the delta flag of the publish request); otherwise publications stay full.

## RPC options

`rpc` has an overload that takes `RpcOptions`. Headers set in the options are sent with that one call only:

```java
RpcOptions options = new RpcOptions.Builder()
        .header("x-request-source", "mobile")
        .build();

client.rpc("my.method.name", data, options, new ResultCallback<RPCResult>() {
    @Override
    public void onDone(Throwable e, RPCResult result) {
        // result.getData() on success
    }
});
```

Headers need a server that supports RPC options. The client advertises this capability in the connect request, and the server confirms it in the connect reply. Check the confirmed capability on the client:

```java
if (client.getCapabilities().isRpcOptionsSupported()) {
    // per-call headers can be sent
}
```

The capability belongs to the current connection. It is `false` until a connect reply confirms it, and it is cleared when the transport closes. When it is `false`, an `rpc` call with headers fails locally: the callback gets a `ReplyError` with code `ReplyError.UNSUPPORTED` (15), and the call is not sent. A call with empty options, or the `rpc` overload without options, is a plain call and works either way.

Errors from the server can carry application fields. They are available on `ReplyError`, and they are empty or zero when the server did not send them:

* `getAppCode()` - numeric application error code, `0` when not set.
* `getAppType()` - application error type, empty when not set.
* `getMetadata()` - unmodifiable string map of allow-listed server metadata, empty when not set.

```java
public void onDone(Throwable e, RPCResult result) {
    if (e instanceof ReplyError) {
        ReplyError error = (ReplyError) e;
        System.out.println(error.getCode() + " " + error.getAppCode() + " " + error.getAppType() + " " + error.getMetadata());
    }
}
```

`client.serverTimeMillis()` returns the current time in milliseconds on the server clock. It is estimated from the server time in the last reply that carried one, and falls back to local time until a reply has carried a server time.

## Exactly-once calls

With a server that confirms RPC options on the connect reply and an instance set with `Options.setInstance`, every `rpc` call carries a `msg_id`, and the server runs each `msg_id` once. Without the instance a call is plain: it carries no `msg_id`, is not kept across a lost transport, and fails with an `IOException` when the transport is lost. Headers are sent whenever the server confirms RPC options. When the transport drops before the reply arrives, the client keeps the call and sends it again after the next connect reply, with the same `msg_id` and the retransmit flag set. The server answers from its record, so the call is not run twice. The result then has `isCached()` set to `true`. The replies received are acknowledged with the next call, so the server can drop its records early.

A call keeps its original deadline across resends: `Options.setTimeout` by default, or `RpcOptions.Builder.timeout`. A call whose deadline passes is rejected with a `TimeoutException`. A call which was sent with a `msg_id` is also cancelled on the server, as `cancel()` does, so the server does not run it later. The cancel goes out over the current connection, or the next one which confirms RPC options. Without that capability nothing is cancelled, and a timed-out call may still run on the server. A reply which arrives after the deadline is still acknowledged, for one call timeout after the deadline.

`rpcCall` returns an `RpcCall` handle. `cancel()` asks the server to stop the call and rejects it locally with `ReplyError.CANCELLED` (17). A call which was not sent yet is never sent, and is rejected with `ReplyError.CANCELLED` whatever the connection state. For a client without an instance, `cancel()` of a call which was sent rejects it with `ReplyError.UNSUPPORTED` (15) whatever the connection state, and sends nothing. On a connected server without the capability, `cancel()` of a call which was sent is rejected the same way. A null method or data is refused at the call with a `NullPointerException`. While the client is connecting or disconnected the capability is not known, so a call which was sent gets its cancel with the connection which confirms the capability. A cancel issued while the client is disconnected goes out with the next connection which confirms the capability. The cancels which wait for such a connection are kept at most `Options.setMaxPendingRpc` at a time: when the list is full, the oldest cancel is dropped, as the server's record of its call expires anyway. A cancel command gets the client timeout (`Options.setTimeout`) as its reply deadline. A cancel which the server does not answer in time is forgotten and not sent again. A cancel which is lost with its transport before its reply is kept for the next connection which confirms the capability.

```java
RpcCall call = client.rpcCall("my.method.name", data, new ResultCallback<RPCResult>() {
    @Override
    public void onDone(Throwable e, RPCResult result) {
        if (e instanceof ReplyError && ((ReplyError) e).getCode() == ReplyError.CANCELLED) {
            // the call was cancelled
        }
    }
});
call.cancel();
```

For a client with an instance, `Options.setMaxPendingRpc` (32 by default) bounds the rpc calls which are not settled. A call counts from its issue until it settles: a reply, an error, a timeout or a cancel. That includes a call which waits for the connection, and a call kept for a resend across a reconnect. A call beyond the bound fails at once with `ReplyError.LIMIT_EXCEEDED` (16) and is not sent. A client without an instance has no bound. Without the capability or without an instance, calls are sent without a `msg_id`, nothing is resent, and a call lost with its transport fails with an `IOException`.

The `msg_id` carries the server time, corrected by the server time of each reply, and is taken when the call is first written. The time part follows the corrected clock. A correction that moves the clock back holds the time part of the last `msg_id` until the clock passes it, so the `msg_id` values stay unique. A re-sync after a refusal may lower the time part, see below. The sequence of each second starts at a random lane, which makes a repeated `msg_id` improbable, also for two clients which share an instance after a reload. It is not impossible: the server keeps a record of a `msg_id` only for its own window.

The server refuses a call it did not run with a reply error, and sets the metadata key `x-centrifugo-retry` on it (`ReplyError.getMetadata()`). The value `new_msg_id` means the `msg_id` was outside the server window: the client re-syncs its clock to the server time of that reply, and sends the call again at once with a new `msg_id`, at most twice. The value `later` means the call did not run and its `msg_id` is still good: the client sends the same `msg_id` again after a random delay (full jitter): uniform in [0, 250 ms], then in [0, 500 ms], then in [0, 1 s], at most three times, with the retransmit flag set. The deadline of the call bounds all attempts. The delay of a resend is capped at the remaining deadline, so a resend due after the deadline falls on the deadline itself: the call is then rejected with a `TimeoutException` and cancelled on the server, as a sent call is. An error without the key is returned at once, and so is the error of the last attempt, with its metadata. A refused call has not run, so a resend never runs it twice.

### Connection instance

The server scopes the records of a client by a connection instance, which the application supplies with `Options.setInstance`. The SDK never generates it and never stores it. The application must:

* generate a random secret of at least 128 bits: 22 to 64 characters from `A-Za-z0-9_-`;
* keep it per installation or per tab, in storage of its own;
* never log or share it. Whoever knows it can retransmit and cancel calls in its scope.

`setInstance` throws `IllegalArgumentException` for a value outside that pattern. It is the unchecked error the Java API uses for an invalid argument: `ConfigurationError` is a checked `Throwable` reported through `ErrorEvent`, and throwing it would change the signature of `setInstance`. `null` clears the instance. The instance is read when the client is created, so a later change of the options does not change that client. Without an instance a call has no scope on the server, so it is not deduplicated.

```java
byte[] bytes = new byte[16];
new java.security.SecureRandom().nextBytes(bytes);
String instance = String.format("%032x", new java.math.BigInteger(1, bytes));
// persist the value in the application's own storage, then:
options.setInstance(instance);
```

## RPC pool for large payloads

`RpcPool` sends rpc calls with large payloads over several plain connections per endpoint, with a window of calls per connection, a bounded wait queue, and resends. The application names the endpoints through a resolver, keyed by an endpoint key which the server answered (for example a region), and gives each call an optional affinity hint, which the server issued and sends in the header named by `affinityHeader`.

```java
RpcPool pool = new RpcPool.Builder(endpointKey -> Collections.singletonList("wss://blobs.example/connection/websocket"))
        .token(token)
        .affinityHeader("x-example-affinity")
        .maxConnectionsPerEndpoint(2)
        .maxInflightPerConnection(1)
        .build();
pool.rpc("blob.writePart", chunk, RpcPool.CallOptions.builder().endpointKey("eu-2").affinity(hint).build(), (error, result) -> {
    // error is a ReplyError, a TimeoutException, or an IOException when the connection was lost for good
});
```

- The calls are plain: no `msg_id`, no instance. A resent call may run again on the server, so the method must be idempotent.
- The window is `maxInflightPerConnection`, 1 by default. The server must allow it: `client.concurrency` on the server is at least the window plus one, or the server's read loop cannot answer pings while calls run.
- A call is sent again, up to `retries` times (3 by default) counted together, when its connection was lost (`IOException`), when the server refused it with the `x-centrifugo-retry: later` signal (after the same jittered delay as the main client), or when `reroute` maps the server's error to another endpoint key. A timeout is not resent. A connection the server closed for a frame over its size limit (WebSocket 1009) settles its calls with `IOException` and is not resent: the chunk is over the limit every time.
- `redirect` lets the application move the bytes of a reply or an error to a URL (for example a presigned one). The pool fetches it with OkHttp, within the call's deadline, at most `maxRedirectsInflight` at a time, and the body is bounded by `RpcPool.Redirect` `maxBytes` (16 MiB by default). The pool follows no redirect of the fetch: the hook names the final URL, which is checked against the endpoint before the one request, and any 3xx answer fails the fetch. The request headers of `RpcPool.Redirect` go with that request. A fetch that fails, a 3xx answer, or a URL the endpoint does not allow (`https:` for `wss:`) fails the call with `REDIRECT_FAILED`.
- `maxQueued` (64 by default) bounds the calls waiting for a connection; a call beyond it fails at once with `LIMIT_EXCEEDED`. The calls waiting for a redirect fetch slot count in it too, and in `stats().queued`: a fetch which would wait beyond it fails with `LIMIT_EXCEEDED`, while a fetch with a free slot is never refused by it. `idleTimeout` (60 s by default, 0 keeps the connections) closes a connection with no call. `stats()` reports the connections, the calls in flight and waiting, and the redirect fetches.
- Tokens: `tokenFrom(client)` is the way with a token exchange, and it is recommended with a token getter too. The connections take the main client's current token only while the main client is connected: while it connects, a connection waits for it, and once the client is disconnected for good, the connection fails as unauthorized. A connection which is refused a token the main client still holds makes the client renew itself once, and a renewal which gives that token back fails the connections waiting for it at once. The connections never run the exchange or the getter. Exclusive with `token` and `tokenGetter`. After a logout (the client's session ended as unauthorized) calls fail at once with `IOException("unauthorized")` until the client connects again. Cancel transfers when the user changes: calls already in flight complete under the old identity.
- `close()` closes every connection: the calls waiting fail with `CANCELLED`, the calls on the wire with `IOException`.
- The array of `rpc` must not change until the callback: the pool reads it when it sends the call, which can be after the call waited in the queue. The okHttp client of the pool, when given, also serves the redirect fetches with its interceptors. The client itself does not follow redirects, and the pool follows none either: a redirect answer fails the fetch, so the fetch goes through the interceptors once.

## Refreshing the connection token on demand

`Client.refresh(token, callback)` replaces the connection token now, without waiting for the server to ask for one. On success the callback gets the identity the server reports, and the refresh timer is rescheduled from the new TTL. The token of the connection is replaced only by a token the server accepted. An error is passed to the callback unchanged, with its code and metadata. The connection, its identity and its refresh timer stay as they were, and the refresh is not retried: the caller asked for it, so the caller decides. `rpc` calls issued while a refresh is in progress wait for its reply. A refresh with the same token as a pending refresh shares its command. A refresh made while the client is not connected sets the token the next connect sends, and the previous token is restored when the server refuses that refresh. At most 32 refreshes are in progress; a refresh beyond that fails at once with `ReplyError.LIMIT_EXCEEDED` (16). When the connection drops during a refresh, the refresh in flight fails with an `IOException`. A refresh which waits behind it is not failed: its command goes out with the next connect, and its callback gets that refresh's result. The `rpc` calls which waited for a refresh stay queued until then, and go out after the reconnect. The token of a refresh made while disconnected stays for the next connect.

The refresh timer runs before the token expires. Its errors follow the upstream rule, with two deviations from upstream:

| Error of the timer refresh | Behaviour |
|---|---|
| temporary, or a client error (code below 100) | reported as an `ErrorEvent`, retried with backoff and a fresh token (upstream) |
| 109 token expired | reported as an `ErrorEvent`, retried with backoff and a fresh token. **Deviation:** upstream disconnects with 109 on a refresh. The connect treats 109 the same way, as a request for a fresh token. |
| 103 permission denied | reported as an `ErrorEvent`, the connection and its identity are kept, and there is no retry. **Deviation:** upstream disconnects with 103. The server refused the identity change of the token the getter returned, and the same token would be refused again. The server ends the connection at its expiry, and the reconnect fetches a token from the getter, so the identity changes then. An application which needs the change now calls `refresh()` and handles 103. |
| anything else | the connection is disconnected with the server's code, and does not reconnect (upstream) |

### Token exchange over the connection

Instead of a connection token, the application can hand the client a long-lived refresh token, once, and let the client obtain and renew the short access token itself, over the same socket. The application persists the refresh token and supplies three callbacks through `TokenExchange`: `load` reads it, `store` persists a rotated one, and `exchange` trades it for an access token. `Options.setTokenExchange` turns the feature on; it is exclusive with `setTokenGetter`, and a client created with both throws `IllegalArgumentException`.

```java
Path tokenFile = Paths.get(dataDir, "refresh_token");

TokenExchange exchange = new TokenExchange() {
    @Override
    public String load() {
        return readToken(tokenFile);
    }

    @Override
    public void store(String refreshToken) {
        writeToken(tokenFile, refreshToken);
    }

    @Override
    public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
        // Encode the request with your own codec. The rpc goes over the connection the exchange runs on.
        event.rpc("auth.exchange", encodeRequest(event.getRefreshToken()), (Throwable err, RPCResult result) -> {
            if (err != null) {
                // A refusal arrives as TokenExchangeRefusedException, because its app type is in refusalTypes().
                cb.onDone(err, null);
                return;
            }
            // Decode the reply with your own codec: the access token, the rotated refresh token if there is one,
            // and the grace in seconds the issuer keeps the consumed refresh token for.
            cb.onDone(null, decodeReply(result.getData()));
        });
    }

    @Override
    public Set<String> refusalTypes() {
        // The app type your backend sets on a refused refresh token, see below. Any name the backend sends.
        return Collections.singleton("REFRESH_TOKEN_REFUSED");
    }

    @Override
    public int grace() {
        // Used until the issuer publishes one in a result. The default is 30 seconds.
        return 30;
    }
};

Options options = new Options();
options.setTokenExchange(exchange);
Client client = new Client(endpoint, options, listener);
client.connect();
```

The backend answers a refused refresh token with an error whose app type is `REFRESH_TOKEN_REFUSED` (any gRPC status; the app type goes in the `app-type` trailer). The client classifies a refusal by that name, from `refusalTypes()`, and never by the error code or the HTTP status. An rpc rejected with a name in the list fails with `TokenExchangeRefusedException` (an `UnauthorizedException`, with the server error as `getReplyError()`); an rpc rejected with any other name is an ordinary error, and the exchange is retried with backoff. The list is empty by default: the client then classifies nothing, and your `exchange` may throw `UnauthorizedException` itself, for a refusal it finds in the reply body.

The flow: when the connect reply is anonymous (no user), whether the connect sent a token or not, the client runs the exchange on the new socket before it reports the connection as connected. It then sets the access token as the connection token and sends a `refresh` command with it, which binds the connection to the user. The `connected` event fires once, with that user. Every rotated refresh token goes to `store` before the access token issued with it is used. The client renews the access token over the connected socket at a random moment between 70% and 90% of its TTL, so a fleet which connected together spreads its renewals. Without the option, the refresh timer runs at the full TTL, as before.

Several tabs or processes of one installation can share one refresh token, with no lock in the client. The client reads `load` immediately before each exchange and stores the rotated token immediately after. The backend keeps a grace for the previous refresh token, so two clients which exchange the same token inside the grace both get the same rotated refresh token, and each its own access token: each client binds its own connection with its own access token, and both store the same rotated refresh token.

#### Refusal run and grace

A refused refresh token is not the end of the session at once. When the issuer refuses the token the client presented, a refusal run starts: the client reads `load` again, presents the token it finds at once when it changed (another client rotated it meanwhile), and presents the same token again once otherwise: while connecting at a random moment inside the remaining grace, while renewing after the refresh retry delay. A second refusal of a token that did not change ends the session, and so does any refusal at the horizon. Every changed value is presented at once, with no limit per run; the horizon is checked only when the token read is the one refused. The run ends with an accepted exchange, with a connect which carried the access token and got a user back, with an accepted `refresh` call (the server accepted the token the connection holds), with the session's end, or with `disconnect()`. A transport loss does not end it: a client which reconnects in a loop still ends the session at the second refusal of an unchanged value, at the latest at the horizon. The outcome of an exchange which was superseded, abandoned by a transport loss, or late past its timeout opens and ends no run: its refusal is dropped, and only its rotation is stored, while the storage still holds the token it used.

The horizon is the grace, measured from the first refusal of the run on the scheduler's monotonic clock; the wall clock does not move it. The grace is in seconds. An accepted result publishes it (`new TokenExchangeResult(token, refreshToken, graceSeconds)`; the two-argument constructor publishes none), and `TokenExchange.grace()` gives it until the first one arrives, and again after an accepted result which publishes none (the absent value -1 counts as none; another negative value is invalid and keeps the grace in use). A value above 300 is clamped to 300. The session is dead at the second refusal of a token that did not change, at a refusal at the horizon, or when `load` returns nothing. The client does not reconnect then, and reports the `DisconnectedEvent` with code 1.

Within the run a refusal is reported as an `ErrorEvent` with `TokenError`, with the message `refresh token refused, retrying within the grace`, which names no token. A dead session costs two exchanges: its first refusal and one re-presentation; the changed values of other clients are presented on top, each once.

#### Several processes: the file lock

The client holds no lock, and that is enough for a rotation which the next attempt can recover from. An installation whose processes must not present different refresh tokens (one live refresh token per installation) puts the whole rotation inside `exchange`, under a lock of the storage. The lock is taken before the stored token is read, and released after the rotated token is written, so that no other process reads or writes between them. The recipe, with `java.nio` only:

```java
private static final ConcurrentMap<Path, Semaphore> PROCESS_LOCKS = new ConcurrentHashMap<>();
private final Path lockFile = Paths.get(dataDir, "refresh_token.lock");

@Override
public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
    // Both waits end at the client timeout, the deadline of the rpc. A failure of either fails the callback.
    Semaphore processLock = PROCESS_LOCKS.computeIfAbsent(lockFile.toAbsolutePath(), (Path path) -> new Semaphore(1));
    FileLock fileLock;
    try {
        fileLock = acquire(processLock, lockFile, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
    } catch (IOException | InterruptedException e) {
        cb.onDone(e, null);
        return;
    }
    // Another process may have rotated the token since load: present what the storage holds under the lock.
    String presented;
    try {
        presented = readToken(tokenFile);
    } catch (IOException e) {
        release(processLock, fileLock.channel());
        cb.onDone(e, null);
        return;
    }
    if (presented == null) {
        release(processLock, fileLock.channel());
        cb.onDone(new UnauthorizedException(), null);
        return;
    }
    // Any failure before the rpc is accepted releases the locks and fails the callback: a lock left held blocks every later exchange of the JVM.
    try {
        // The callback runs once, on the thread which completes the rpc: after close that is not the client's thread.
        // A semaphore may be released on any thread, so the callback releases the locks wherever it runs.
        event.rpc("auth.exchange", encodeRequest(presented), (Throwable err, RPCResult result) -> {
            Throwable failure = err;
            TokenExchangeResult outcome = null;
            try {
                if (err == null) {
                    TokenExchangeResult reply = decodeReply(result.getData());
                    if (reply.getRefreshToken() != null) {
                        // A temporary file and a rename: another process never reads a torn value.
                        writeTokenAtomically(tokenFile, reply.getRefreshToken());
                    }
                    // Written under the lock, so the result carries no refresh token: the client stores nothing after it.
                    outcome = new TokenExchangeResult(reply.getToken(), null, reply.getGrace());
                }
            } catch (IOException | RuntimeException e) {
                failure = e;
            } finally {
                release(processLock, fileLock.channel());
            }
            cb.onDone(failure, outcome);
        });
    } catch (RuntimeException e) {
        release(processLock, fileLock.channel());
        cb.onDone(e, null);
    }
}

// Takes the process lock, then the file lock, both before the deadline. A failure leaves no lock taken. The file lock is
// polled, so the deadline ends the wait.
private static FileLock acquire(Semaphore processLock, Path file, long deadline) throws IOException, InterruptedException {
    if (!processLock.tryAcquire(deadline - System.nanoTime(), TimeUnit.NANOSECONDS)) {
        throw new IOException("the process lock timed out");
    }
    FileChannel channel = null;
    try {
        channel = FileChannel.open(file, CREATE, READ, WRITE);
        while (true) {
            FileLock fileLock = channel.tryLock();
            if (fileLock != null) {
                return fileLock;
            }
            if (System.nanoTime() >= deadline) {
                throw new IOException("the file lock timed out");
            }
            Thread.sleep(20);
        }
    } catch (OverlappingFileLockException e) {
        release(processLock, channel);
        throw new IOException("the lock file is held by this process", e);
    } catch (IOException | InterruptedException | RuntimeException e) {
        release(processLock, channel);
        throw e;
    }
}

// Closes the channel, which releases its file lock, and then releases the process lock. Null channel: only the process lock.
private static void release(Semaphore processLock, FileChannel channel) {
    try {
        if (channel != null) {
            channel.close();
        }
    } catch (IOException e) {
        // The lock is gone with the channel: nothing else to undo.
    } finally {
        processLock.release();
    }
}
```

The store of the client runs after `exchange` returns, outside the lock, and writes without comparing: the recipe therefore never returns a refresh token it has written itself. The alternative is a `store` which takes the same lock and writes only when the file still holds the token the exchange presented. `load` reads without the lock: the rename makes it atomic. The lock is held for at most the client timeout, since the rpc fails at it. The wait for the lock runs on the client executor, because `exchange` is invoked there: while another process holds the lock, the client does no other work, pings included, for at most the client timeout. An application which cannot accept that waits on its own thread, and calls the callback from there. A client timeout above the ping delay which the server allows can therefore drop the connection with no ping, so it stays below that delay, or the wait moves to an application thread. The process lock is a `Semaphore` and not a `ReentrantLock`, because the callback releases it on the thread which completes the rpc, and only the owner thread may release a `ReentrantLock`.

The process lock is shared by every client of the process which names the same lock file, so two clients of one JVM wait for each other: a second `FileChannel` lock in one JVM would throw `OverlappingFileLockException`, which the recipe reports as an `IOException`. A client closed during an exchange is safe: its callback still runs once and releases both locks, and a client which then uses the same `TokenExchange` takes them as usual.

#### The attribution claim (`meta`)

The access token is a connection JWT. Its `meta` claim is the issuer's attribution of the connection, for example an installation or a device. The server stores it at connect, and replaces it at every renewal and rebind whose access token carries the claim; a token without it keeps the attribution the connection has. An issuer which wants a stable attribution keeps `meta` the same across the access tokens of one session. An issuer which wants to drop the attribution sends an empty object (`"meta": {}`), which replaces it. The client never reads the claim.

On logout, disconnect or close the client before clearing the stored refresh token: a renewal still in flight while the client stays connected stores its rotated token when it completes.

| Event | When |
|---|---|
| `ConnectedEvent` with an empty user | `load` returned no refresh token: the connection stays anonymous |
| `ErrorEvent` with `TokenError` | an exchange or `store` failed; the exchange is retried with backoff, or the connection goes on. A refused refresh token inside the grace is reported with the message `refresh token refused, retrying within the grace`, and retried. A bootstrap refresh which timed out or failed to write has an unknown outcome: the connection is closed and the client reconnects |
| `ErrorEvent` with `RefreshError` | a renewal refresh was refused; the error table of the timer refresh applies |
| `ErrorEvent` with `ReplyError` | a bootstrap refresh was refused with a temporary error, an error below 100 or 109: the exchange is retried with backoff |
| `DisconnectedEvent` with code 1 (`unauthorized`) | the session is dead: a second refusal of a token that did not change, a refusal at the grace horizon of the refusal run, `load` threw `UnauthorizedException`, or `load` returned nothing at renewal or after a refusal (at bootstrap an empty `load` connects anonymously). The client does not reconnect |

The server must accept a connection without a token (`client.allow_anonymous_connect_without_token: true`), must allow a refresh which changes the user (`client.rebind.enabled: true`), and the exchange method must be callable by an anonymous connection: a registry entry with `auth_class: anonymous`. The calls of that method carry no connection token, so the issuer authenticates the request by the refresh token alone. The access token is the connection token: the usual `client.token` verification applies, and its `exp` drives the renewal.

## Client error codes

The client reports the calls it did not send, or cancelled, with the codes of `ReplyError`. They are client-local, and the server never sends them:

| Code | `ReplyError` constant | Meaning |
|---|---|---|
| 15 | `UNSUPPORTED` | the call was not sent: the connection cannot carry what it asks for (headers, or a cancel) |
| 16 | `LIMIT_EXCEEDED` | the call was not sent: the calls of a client with an instance reached `Options.setMaxPendingRpc`, or the refreshes in progress reached 32 |
| 17 | `CANCELLED` | the call was cancelled with `RpcCall.cancel()`, or `RpcPool.Call.cancel()`: a call which was not sent yet is never sent |
| 12 | `RpcPool.BAD_CONFIGURATION` | the call of an `RpcPool` was refused: an option of the call, or an endpoint key which resolves to no endpoint |
| 18 | `RpcPool.REDIRECT_FAILED` | the redirect of an `RpcPool` call could not be fetched, see `RpcPool.RedirectHook` |

The codes of this section are client-local: a server never sends them.

## CI status

[![Test](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java/actions/workflows/ci.yml/badge.svg)](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java/actions/workflows/ci.yml)

## License

Library is available under the MIT license. See LICENSE for details.

## For Contributors

This section contains an information for library contributors. You don't need generating protobuf code if you just want to use `centrifuge-java` in your project.

### Generate proto

The protobuf definitions are located in `centrifuge/main/proto` directory.
`Protocol` class is generated automatically during project compilation by `protobuf` gradle plugin.

### Running tests

The unit tests run with no external services:

```shell
./gradlew :centrifuge:test
```

Integration tests exercise the SDK against a real Centrifugo server brought up via `docker-compose.yml`. The repo's `Makefile` wraps both:

```shell
make test-unit         # unit tests only
make test-integration  # starts Centrifugo, then runs integration tests
make test              # both
make down              # stop Centrifugo when done
```

The Gradle wrapper is pinned at 7.5.1, which supports JDK 8–17. The `Makefile` auto-discovers `openjdk@17` from a Homebrew install (`brew install openjdk@17`); set `JAVA_HOME` yourself if you'd rather pick the JDK explicitly. CI runs both jobs on Ubuntu — see `.github/workflows/ci.yml`.

### Check API signatures

We use [metalava-gradle](https://github.com/tylerbwong/metalava-gradle) to ensure we are aware of breaking API changes in the library.

All PRs check API signatures for compatibility. If you see an error, it may indicate there is a breaking change.
Regenerate API signature with the following command and include an updated `api.txt` in your PR:
```shell
./gradlew :centrifuge:metalavaGenerateSignature
```

Also indicate a breaking change in changelog.

To verify API compatibility locally, run the following command:
```shell
./gradlew :centrifuge:metalavaCheckCompatibility
```

## For maintainer

### Automatic publishing

Run the `release` workflow from the Actions tab (`workflow_dispatch`, input `version` without the `v`, e.g. `1.1.0`), or push a `v*` tag. The workflow:

1. Validates the version and runs the tests.
2. Builds the artifacts.
3. Publishes the Maven repository to the `gh-pages` branch and pushes it.
4. Waits until the POM is served at https://impulse-m.github.io/ImpulseM.Centrifugo.Client.Java/.
5. Creates the tag (manual dispatch only) and the GitHub Release.

GitHub Pages must be enabled for this repository with source "Deploy from a branch" and branch `gh-pages`.

Maven versions are immutable: a version that already exists in `gh-pages` is refused, so bump the version first.

### Publishing locally

Publish into a local checkout of the `gh-pages` branch, or any directory, with:

```shell
VERSION=v1.1.0 ./gradlew :centrifuge:publishAllPublicationsToGhPagesRepository -PghPagesRepo=/path/to/gh-pages
```

`-PghPagesRepo` (or the `GH_PAGES_REPO` environment variable) defaults to `centrifuge/build/gh-pages-maven`.

## Special thanks

* Thanks to [Maks Atygaev](https://github.com/atygaev) for initial library boilerplate
