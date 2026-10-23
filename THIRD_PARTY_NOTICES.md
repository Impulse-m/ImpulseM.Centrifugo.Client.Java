# Third-party notices

The library itself is under the MIT license, see [LICENSE](LICENSE). The files below come from other projects. Each one keeps the license header it had upstream, and nothing here changes that header.

| Component | Files in this repository | License | Source |
|---|---|---|---|
| Gradle wrapper | `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar` | Apache License 2.0 | https://github.com/gradle/gradle |
| Fossil delta decoder | `centrifuge/src/main/java/io/github/centrifugal/centrifuge/Fossil.java` | BSD 2-Clause | https://github.com/dchest/fossil-delta-js |
| Fossil delta test data | `centrifuge/src/test/resources/data/1` to `data/5` | BSD 2-Clause | https://github.com/dchest/fossil-delta-js (`test/data`) |

The Apache License 2.0 text, which the Gradle wrapper files require to be carried with them, is in [licenses/Apache-2.0.txt](licenses/Apache-2.0.txt). It is the `LICENSE` file of https://github.com/gradle/gradle, copied unchanged, so its appendix is the unfilled boilerplate.

`Fossil.java` decodes and applies deltas in the Fossil SCM delta format. Its code follows the JavaScript port `fossil-delta-js` by Dmitry Chestnykh, which is itself based on the original C version by D. Richard Hipp. The test data in `centrifuge/src/test/resources/data/1` to `data/5` (`origin`, `target` and `delta` files) is byte-identical to the test data of the same port. Both keep the BSD 2-Clause license below, copied verbatim from https://github.com/dchest/fossil-delta-js/blob/master/LICENSE.

```text
Copyright 2014-2024 Dmitry Chestnykh (JavaScript port)
Copyright 2007 D. Richard Hipp  (original C version)
All rights reserved.

Redistribution and use in source and binary forms, with or
without modification, are permitted provided that the
following conditions are met:

  1. Redistributions of source code must retain the above
     copyright notice, this list of conditions and the
     following disclaimer.

  2. Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the
     following disclaimer in the documentation and/or other
     materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND ANY EXPRESS
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation
are those of the authors and contributors and should not be interpreted
as representing official policies, either expressed or implied, of anybody
else.
```

Module dependencies (for example OkHttp and protobuf-javalite) are not copied into this repository. They are declared in [centrifuge/build.gradle](centrifuge/build.gradle) and keep their own licenses in their own artifacts.
