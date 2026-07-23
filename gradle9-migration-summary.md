# Gradle 9.6.1 Migration Summary

## Overview

Migration from Gradle 8.14.3 to 9.6.1 for deephaven-core. All configuration, compilation, and test issues caused by the version bump have been resolved.

## Changes

### settings.gradle

- **Removed** `buildCache { local { removeUnusedEntriesAfterDays = 4 } }` — API removed in Gradle 9. Cache retention is now only configurable via init scripts. Gradle 9 defaults to 7-day retention.
- **Bumped** `com.gradle.develocity` plugin from `4.4.0` to `4.4.2`.
- **Removed** Jetty 11 project includes (removed upstream).
- **Added** `engine-read-tracker` project include (added upstream).

### buildSrc/build.gradle

- **Lowered** `buildSrcCompilerVersion` from `21` to `17`. Gradle 9's Groovy compiler does not respect `options.release` for bytecode targeting, so the compiled classes had Java 21 bytecode that the Java 17 daemon couldn't load.
- **Bumped** `de.esoco.gwt:gwt-gradle-plugin` from `1.2.0` to `1.3.1`. The old version used `JavaPluginConvention`, which was removed in Gradle 9.

### buildSrc/src/main/groovy/Docker.groovy

- **Changed** inner class `DockerTaskConfig` fields from `private` to package-private (`copyIn`, `copyOut`, `dockerfileFile`, `dockerfileAction`). Gradle 9's stricter Groovy `@CompileStatic` no longer allows outer-class access to private inner-class fields.
- **Changed** `onNext` closure parameters from `InspectImageResponse message` to `Object message` with a cast inside. The stricter type checker doesn't allow typed closure params for the `onNext` callback.

### buildSrc/src/main/groovy/io/deephaven/project/util/JavaDependencies.groovy

- **Replaced** all uses of `dependency.dependencyProject` with `project.project(dependency.path)`. The `ProjectDependency.getDependencyProject()` method was removed in Gradle 9. The replacement `ProjectDependency.getPath()` was introduced in Gradle 8.11.

### buildSrc/src/main/groovy/io/deephaven/tools/docker/DeephavenInDockerExtension.groovy

- **Changed** `onNext` closure parameter from `InspectContainerResponse inspect` to `Object obj` with a cast inside. Same `@CompileStatic` issue as Docker.groovy.

### java-client/{barrage,flight,session}-examples/build.gradle

- **Replaced** `fileMode = 0755` with `filePermissions { unix('0755') }`. The `fileMode` property was removed from `CopySpec` in Gradle 9.

### py/jpy-integration/build.gradle

- **Replaced** `testSourceDirs` and `testResourceDirs` with `testSources.from()` and `testResources.from()`. The IDEA plugin's `testSourceDirs`/`testResourceDirs` properties were removed in Gradle 9.

### engine/benchmark/build.gradle and extensions/parquet/benchmark/build.gradle

- **Added** `test { failOnNoDiscoveredTests = false }`. Gradle 9 now fails by default when a test task discovers no tests. These benchmark projects have JMH sources in the test source set but no JUnit tests.

## Pre-existing Failures (Not Gradle 9 Related)

The following failures also occur on `main` with Gradle 8.14.3:

- **`:java-client-session-dagger:test`** — 25 failures, all `TimeoutException` or `TableHandleException`. Embedded test server not responding to requests.
- **`:server:test`** — `ObjectServiceTest.myObject` times out.
- **`:Integrations:composeUp`** — Docker compose not available/configured.

## Open Questions

- **buildSrc toolchain 21→17**: The original design compiled buildSrc with Java 21 (matching the main project) and targeted Java 17 bytecode via `options.release`. Gradle 9's Groovy compiler ignores this flag. If the team wants to restore the Java 21 compiler for buildSrc, the Gradle daemon would need to run on Java 21+ (`JAVA_HOME` or `org.gradle.java.home`).
- **Gradle 10 deprecations**: The build reports deprecated features that will be incompatible with Gradle 10. Run `./gradlew help --warning-mode all` to enumerate them.
