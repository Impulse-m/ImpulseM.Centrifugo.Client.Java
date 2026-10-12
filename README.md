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
