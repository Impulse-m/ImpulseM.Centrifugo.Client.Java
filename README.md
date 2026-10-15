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

The server refuses a call it did not run with a reply error, and sets the metadata key `x-centrifugo-retry` on it (`ReplyError.getMetadata()`). The value `new_msg_id` means the `msg_id` was outside the server window: the client re-syncs its clock to the server time of that reply, and sends the call again at once with a new `msg_id`, at most twice. The value `later` means the call did not run and its `msg_id` is still good: the client sends the same `msg_id` again after 250 ms, 500 ms and then 1 s, at most three times, with the retransmit flag set. The deadline of the call bounds all attempts. The delay of a resend is capped at the remaining deadline, so a resend due after the deadline falls on the deadline itself: the call is then rejected with a `TimeoutException` and cancelled on the server, as a sent call is. An error without the key is returned at once, and so is the error of the last attempt, with its metadata. A refused call has not run, so a resend never runs it twice.

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

## Refreshing the connection token on demand

`Client.refresh(token, callback)` replaces the connection token now, without waiting for the server to ask for one. On success the callback gets the identity the server reports, and the refresh timer is rescheduled from the new TTL. The token of the connection is replaced only by a token the server accepted. An error is passed to the callback unchanged, with its code and metadata. The connection, its identity and its refresh timer stay as they were, and the refresh is not retried: the caller asked for it, so the caller decides. `rpc` calls issued while a refresh is in progress wait for its reply. A refresh with the same token as a pending refresh shares its command. A refresh made while the client is not connected sets the token the next connect sends, and the previous token is restored when the server refuses that refresh. At most 32 refreshes are in progress; a refresh beyond that fails at once with `ReplyError.LIMIT_EXCEEDED` (16). When the connection drops during a refresh, the refresh in flight fails with an `IOException`, and so does a refresh which waits behind it, while the `rpc` calls which waited for that refresh stay queued and go out after the reconnect. The token of a refresh made while disconnected stays for the next connect.

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
SharedPreferences prefs = context.getSharedPreferences("auth", Context.MODE_PRIVATE);

TokenExchange exchange = new TokenExchange() {
    @Override
    public String load() {
        return prefs.getString("refresh_token", null);
    }

    @Override
    public void store(String refreshToken) {
        prefs.edit().putString("refresh_token", refreshToken).apply();
    }

    @Override
    public void exchange(TokenExchangeEvent event, ResultCallback<TokenExchangeResult> cb) {
        // Encode the request with your own codec. The rpc goes over the connection the exchange runs on.
        event.rpc("auth.exchange", encodeRequest(event.getRefreshToken()), (Throwable err, RPCResult result) -> {
            if (err instanceof ReplyError && isRefusal((ReplyError) err)) {
                // The issuer refused the refresh token, by the code your backend uses: the session is dead.
                cb.onDone(new UnauthorizedException(), null);
                return;
            }
            if (err != null) {
                cb.onDone(err, null);
                return;
            }
            // Decode the reply with your own codec: the access token, and the rotated refresh token if there is one.
            cb.onDone(null, decodeReply(result.getData()));
        });
    }
};

Options options = new Options();
options.setTokenExchange(exchange);
Client client = new Client(endpoint, options, listener);
client.connect();
```

The flow: when the connection is anonymous (no token), the client runs the exchange on the new socket before it reports the connection as connected. It then sets the access token as the connection token and sends a `refresh` command with it, which binds the connection to the user. The `connected` event fires once, with that user. Every rotated refresh token goes to `store` before the access token issued with it is used. The client renews the access token over the connected socket at a random moment between 70% and 90% of its TTL, so a fleet which connected together spreads its renewals. Without the option, the refresh timer runs at the full TTL, as before.

Several tabs or processes of one installation can share one refresh token, with no lock in the client. The client reads `load` immediately before each exchange and stores the rotated token immediately after. An exchange which is refused is retried once, with the token read again, when another one rotated the token meanwhile. The backend keeps a short grace for the previous refresh token, so two clients which exchange at the same moment both get the same rotated pair.

| Event | When |
|---|---|
| `ConnectedEvent` with an empty user | `load` returned no refresh token: the connection stays anonymous |
| `ErrorEvent` with `TokenError` | an exchange or `store` failed; the exchange is retried with backoff, or the connection goes on |
| `ErrorEvent` with `RefreshError` | a renewal refresh was refused; the error table of the timer refresh applies |
| `DisconnectedEvent` with code 1 (`unauthorized`) | the session is dead: the issuer refused the refresh token again, or `load` returned nothing at renewal. The client does not reconnect |

The server must accept a connection without a token (`client.allow_anonymous_connect_without_token: true`), must allow a refresh which changes the user (`client.rebind.enabled: true`), and the exchange method must be callable by an anonymous connection: a registry entry with `auth_class: anonymous`. The calls of that method carry no connection token, so the issuer authenticates the request by the refresh token alone. The access token is the connection token: the usual `client.token` verification applies, and its `exp` drives the renewal.

## Client error codes

The client reports the calls it did not send, or cancelled, with the codes of `ReplyError`. They are client-local, and the server never sends them:

| Code | `ReplyError` constant | Meaning |
|---|---|---|
| 15 | `UNSUPPORTED` | the call was not sent: the connection cannot carry what it asks for (headers, or a cancel) |
| 16 | `LIMIT_EXCEEDED` | the call was not sent: the calls of a client with an instance reached `Options.setMaxPendingRpc`, or the refreshes in progress reached 32 |
| 17 | `CANCELLED` | the call was cancelled with `RpcCall.cancel()`: a call which was not sent yet is never sent |

Codes 15 to 19 are reserved for client-local errors of the fork SDKs; a server never sends them.

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
