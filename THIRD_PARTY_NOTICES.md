# Third-party notices

The library itself is under the MIT license, see [LICENSE](LICENSE). The files below come from other projects. Each one keeps the license header it had upstream, and nothing here changes that header.

| Component | Files in this repository | License | Source |
|---|---|---|---|
| Gradle wrapper | `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar` | Apache License 2.0 | https://github.com/gradle/gradle |

The Apache License 2.0 text, which the Gradle wrapper files require to be carried with them, is in [licenses/Apache-2.0.txt](licenses/Apache-2.0.txt).

Module dependencies (for example OkHttp and protobuf-javalite) are not copied into this repository. They are declared in [centrifuge/build.gradle](centrifuge/build.gradle) and keep their own licenses in their own artifacts.
