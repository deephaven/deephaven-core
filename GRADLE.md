# Gradle

The Deephaven Community Core project is built using [Gradle](https://docs.gradle.org/8.7/userguide/userguide.html).
A full rundown of the ins-and-outs of Gradle and the Deephaven configuration of Gradle is out-of-scope of this document, but the following may be helpful context:

* [Configuring the build environment](https://docs.gradle.org/8.7/userguide/build_environment.html)
* [Toolchains for JVM projects](https://docs.gradle.org/8.7/userguide/toolchains.html)
* [io.deephaven.java-toolchain-conventions.gradle](buildSrc/src/main/groovy/io.deephaven.java-toolchain-conventions.gradle)

## Toolchains

This project uses toolchains to configure how different Gradle tasks are run.
This means that the JDK used for the Gradle daemon may not be the same JDK used for the Gradle client (where tasks are executed).
By default, Gradle is configured to download the necessary toolchains if it is unable to find a suitable one locally.
To disable this behavior, you can set the gradle property `org.gradle.java.installations.auto-download=false`.

For example, our [CI unit tests properties](.github/scripts/gradle-properties.sh) turns off auto-download and auto-detect,
and explicitly sets the Java install paths:

```properties
org.gradle.java.installations.auto-download=false
org.gradle.java.installations.auto-detect=false
org.gradle.java.installations.paths=${JAVA_INSTALL_PATHS}
```

The command `./gradlew -q javaToolchains` may be useful to debug and diagnose toolchain related issues.

### Runtime Toolchain

The runtime toolchains are used to invoke "normal" Java executable tasks; that is, tasks of `org.gradle.api.tasks.JavaExec`.
The most visible task is the `server-jetty-app:run` task, but other tasks like code generation use the runtime toolchain
as well.

* `deephaven.runtimeVersion`: the runtime Java version to use; by default is 11
* `deephaven.runtimeVendor`: the runtime Java vendor to use

For example, the following would ensure that the server is run with JDK 21 from a JVM vendor name that contains "azul":

`./gradlew server-jetty-app:run -Pdeephaven.runtimeVersion=21 -Pdeephaven.runtimeVendor=azul`

### Test Runtime Toolchain

The test runtime toolchains invoke "test" Java tasks; that is, tasks of `org.gradle.api.tasks.testing.Test`.

* `deephaven.testRuntimeVersion`: the test runtime Java version to use, by default is 11
* `deephaven.testRuntimeVendor`: the test runtime Java vendor to use

The [nightly checks](.github/workflows/nightly-check-ci.yml) takes advantage of this to test multiple Java version for
unit testing.

For example, the following will run the test tasks with JDK 21 from a JVM vendor name that contains "amazon":

`./gradlew test -Pdeephaven.testRuntimeVersion=21 -Pdeephaven.runtimeVendor=amazon`

### Compiler Toolchain

The compiler toolchains invoke "compile" Java tasks; that is, tasks of `org.gradle.api.tasks.compile.JavaCompile`.

* `deephaven.compilerVersion`: the compile Java version to use, by default is 11
* `deephaven.compilerVendor`: the compile Java vendor to use

Note: these gradle properties control both compiling of the main source sets and test source sets.

### Language Levels

A language level is the Java language level the source is written for. This dictates the minimum version required to run
the specific project. Depending on context, some subprojects have a different language level than other subprojects. For
example, `server-jetty-app` has a language level of 11 - it must be run with Java 11 or greater;
`java-client-session` has a language level of 8 - it must be run with Java 8 or greater.

* `deephaven.languageLevel`: the language level for the main source set
* `deephaven.testLanguageLevel`: the language level for the test source set

The language levels are mostly internal properties of the individual subprojects.

### Application options

* `deephaven.javaOpts`: the "generally applicable and recommended JVM options" for the application. Currently, defaults
to `-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication`.

This property is mainly meant to serve as the defaults for `org.gradle.api.plugins.JavaApplication` tasks, which in
turn serves as the defaults for users running the application (it also applies to `org.gradle.api.tasks.JavaExec`
and `org.gradle.api.tasks.testing.Test` tasks). Overly specific options do not belong here. For example, heap settings
(`-Xmx4g`) or system properties (`-Dkey=value`) should not be set here.

For example, the following will create an application tar that uses Generational ZGC by default:

`./gradlew server-jetty-app:distTar -Pdeephaven.javaOpts="-XX:+UseZGC -XX:+ZGenerational"`
