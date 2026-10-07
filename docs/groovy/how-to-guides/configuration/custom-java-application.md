---
title: Run Deephaven from your own Java application
---

The Deephaven server is a Java library. The [production application](../../getting-started/production-application.md) and the [Docker images](./docker-application.md) are prebuilt applications that start it, but any JVM can start a Deephaven server. This guide shows you how to build your own Java application that starts a Deephaven server, so that you control the classpath, the startup code, and which server components are included.

Build a custom application when you want to:

- Package Deephaven together with your own Java code and dependencies in a single application.
- Create tables in Java when the server starts and publish them to clients and the web UI.
- Replace or remove server components, such as the authorization rules.

If you only need to add JARs to a standard server, you don't need a custom application. See [Install and use Java packages](../install-and-use-java-packages.md) instead.

## Prerequisites

- Java 17 or later. Deephaven's web server, Jetty 12, requires Java 17.
- A build tool that resolves Maven artifacts, such as Gradle or Maven. The examples in this guide use Gradle.
- Familiarity with [Dagger](https://dagger.dev/), the dependency injection framework that assembles the Deephaven server. You need only the basics: components, modules, and the `@Provides`, `@Binds`, and `@IntoSet` annotations.

## How a Deephaven server starts

The production application's entry point, `io.deephaven.server.jetty.JettyMain`, is short. A custom application follows the same three steps:

1. Call `MainHelper.init`. It initializes logging, loads the Deephaven [configuration](./config-file.md), and installs process-wide handlers for shutdown and uncaught errors. Call it before anything else touches Deephaven classes.
2. Build a Dagger component. The component wires together every server service: the gRPC and Arrow Flight APIs, the web UI, authentication, the console, and Application Mode. A component factory, a subclass of `ComponentFactoryBase`, builds the component from the configuration.
3. Get the server from the component and call `run`, which starts the server and returns once it is listening. Call `join` to block until the server shuts down.

The production application's component factory, `CommunityComponentFactory`, belongs to the `server-jetty-app` project, which isn't published to Maven Central. A custom application defines its own component and factory using the modules in the published `deephaven-server-jetty` artifact. The rest of this guide walks through doing that.

> [!TIP]
> The `deephaven-core` repository contains a complete example of a custom application in [`server/jetty-app-custom`](https://github.com/deephaven/deephaven-core/tree/main/server/jetty-app-custom). It also replaces the authorization provider. If you have a local clone, run it with `./gradlew server-jetty-app-custom:run -Pgroovy`.

## Set up the build

The following `build.gradle` file creates an application named `my-deephaven-app`. Replace `<version>` with the Deephaven version you want, such as the [latest release](https://github.com/deephaven/deephaven-core/releases/latest) without its leading `v`.

```groovy skip-test
plugins {
    id 'application'
}

repositories {
    mavenCentral()
    // Deephaven's Kafka integration depends on artifacts published only to Confluent's repository.
    maven {
        url = 'https://packages.confluent.io/maven/'
        content {
            includeGroup 'io.confluent'
            includeGroup 'org.apache.kafka'
        }
    }
}

def deephavenVersion = '<version>'
def daggerVersion = '<dagger-version>'

dependencies {
    // The Deephaven bill of materials (BOM) keeps every Deephaven artifact on the same version.
    implementation platform("io.deephaven:deephaven-bom:${deephavenVersion}")

    implementation 'io.deephaven:deephaven-server-jetty'

    implementation "com.google.dagger:dagger:${daggerVersion}"
    annotationProcessor "com.google.dagger:dagger-compiler:${daggerVersion}"

    // Logging
    runtimeOnly 'io.deephaven:deephaven-log-to-slf4j'
    runtimeOnly 'io.deephaven:deephaven-logback-print-stream-globals'
    runtimeOnly 'io.deephaven:deephaven-logback-logbuffer'
    runtimeOnly 'ch.qos.logback:logback-classic:<logback-version>'

    // Optional: JVM-specific implementations for memory and GC metrics and a high-resolution clock
    runtimeOnly 'io.deephaven:deephaven-hotspot-impl'
    runtimeOnly 'io.deephaven:deephaven-clock-impl'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = 'com.example.MyServerMain'
    applicationDefaultJvmArgs = [
            '--add-opens', 'java.base/java.nio=ALL-UNNAMED',
            '--add-exports', 'java.management/sun.management=ALL-UNNAMED',
            '--add-exports', 'java.base/jdk.internal.misc=ALL-UNNAMED',
            '-Dio.netty.noUnsafe=false',
            '-Ddeephaven.console.type=groovy',
    ]
}
```

Keep the following in mind:

- **The Confluent repository is required.** `deephaven-server-jetty` depends on Deephaven's Kafka integration at runtime, and some of its dependencies are published only to Confluent's Maven repository. Without that repository, dependency resolution fails with errors such as `Could not find org.apache.kafka:kafka-clients`.
- **Match the Dagger version to Deephaven's.** Use the Dagger version listed as a dependency in the `deephaven-server` POM for your Deephaven version. Code generated by a newer Dagger compiler may not run against an older Dagger runtime.
- **The JVM arguments are required.** These are the same arguments that the production application's `start` script passes. Apache Arrow needs `--add-opens java.base/java.nio=ALL-UNNAMED`. The two `--add-exports` arguments are needed only if you include `deephaven-hotspot-impl` and `deephaven-clock-impl`, respectively. `-Dio.netty.noUnsafe=false` is required on Java 25 and later, and harmless on earlier versions. If you launch the application another way, such as from your IDE or a container, pass the same arguments.
- **Pick a console language.** The server's default console language is Python, which requires a Python environment that the JVM can load. Setting `deephaven.console.type` to `groovy` gives you a Groovy console with no extra setup. Set it to `none` to disable the console entirely.

Deephaven's optional integrations are separate artifacts. Add the ones you use as runtime dependencies. For example, the production application also includes `deephaven-engine-sql`, `deephaven-extensions-s3`, `deephaven-extensions-iceberg-s3`, `deephaven-extensions-json-jackson`, and `deephaven-extensions-flight-sql`.

## Write the component factory

The component factory builds the Dagger component. Dagger generates the component's implementation at compile time, naming the generated class after the nesting of the interface: `MyComponentFactory.MyComponent` becomes `DaggerMyComponentFactory_MyComponent`.

```java
package com.example;

import dagger.Binds;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.jetty.JettyServerComponent;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentFactoryBase;
import io.deephaven.server.session.ClientChannelFactoryModule;
import io.deephaven.server.session.ClientChannelFactoryModule.UserAgent;
import io.deephaven.server.session.SslConfigModule;

import javax.inject.Singleton;
import java.io.PrintStream;
import java.util.List;

public final class MyComponentFactory extends ComponentFactoryBase<MyComponentFactory.MyComponent> {

    @Override
    public MyComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        // Reads http.port, http.host, ssl.* and other web server properties from the configuration.
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(configuration).build();
        return DaggerMyComponentFactory_MyComponent.builder()
                .withOut(out)
                .withErr(err)
                .withJettyConfig(jettyConfig)
                .build();
    }

    @Singleton
    @Component(modules = MyModule.class)
    public interface MyComponent extends JettyServerComponent {
        @Component.Builder
        interface Builder extends JettyServerComponent.Builder<Builder, MyComponent> {
        }
    }

    @Module(includes = {
            JettyServerModule.class,
            CommunityDefaultsModule.class,
            CommunityAuthorizationModule.class,
            ClientChannelFactoryModule.class,
            SslConfigModule.class,
    })
    public interface MyModule {

        // Identifies this server when it connects to other Deephaven servers, for example through URIs.
        @Provides
        @UserAgent
        static String providesUserAgent() {
            return BarrageSessionFactoryConfig.userAgent(List.of("my-deephaven-app"));
        }

        // Registers MyApplication, so the server runs it at startup.
        @Binds
        @IntoSet
        ApplicationState.Factory bindsMyApplication(MyApplication app);
    }
}
```

The included modules supply the server:

| Module                         | Provides                                                                                                   |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| `JettyServerModule`            | The Jetty web server, which serves the gRPC APIs and the web UI.                                           |
| `CommunityDefaultsModule`      | The standard set of Deephaven services, such as sessions, tables, consoles, plugins, and Application Mode. |
| `CommunityAuthorizationModule` | The default authorization provider, which lets every authenticated user do everything.                     |
| `ClientChannelFactoryModule`   | Outgoing connections from this server to other Deephaven servers. It requires the `@UserAgent` string.     |
| `SslConfigModule`              | The TLS configuration for those outgoing connections.                                                      |

To change a part of the server, replace the module that provides it. For example, the `jetty-app-custom` example drops `CommunityAuthorizationModule` and binds its own `AuthorizationProvider` that disables the input table service.

## Publish tables at startup

An `ApplicationState.Factory` creates objects when the server starts and exposes them to clients by name. It is the Java equivalent of an [Application Mode script](../application-mode-script.md). The factory runs with the server's [execution context](../../conceptual/execution-context.md) already open.

```java
package com.example;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;

public final class MyApplication implements ApplicationState.Factory {

    // Keeps the application's tables alive for the life of the server.
    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;

    @Inject
    public MyApplication() {}

    @Override
    public ApplicationState create(ApplicationState.Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, MyApplication.class.getName(), "My application");
        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            // Operations on refreshing tables must hold the update graph's shared lock.
            final Table ticking = updateGraph.sharedLock().computeLocked(
                    () -> TableTools.timeTable("PT1s").update("X = ii"));
            state.setField("ticking", ticking);
            state.setField("static_table", TableTools.emptyTable(5).update("Y = i * 2"));
        }
        return state;
    }
}
```

Two details matter here:

- **Keep the tables alive.** Deephaven releases tables that nothing references. Creating the tables inside a [liveness scope](../../conceptual/liveness-scope-concept.md) that the factory holds in a field keeps them alive for as long as the server runs.
- **Lock the update graph for refreshing tables.** Operations on a refreshing table, such as calling `update` on a [time table](../../reference/table-operations/create/timeTable.md), must run under the update graph's shared lock. Without it, the server fails to start with `IllegalStateException: May not initiate serial table operations`. Operations on static tables, such as `emptyTable`, don't need the lock.

The application's ID is its first constructor argument, here `com.example.MyApplication`. In the web UI, the fields appear in the **Panels** menu. Clients fetch them with an application ticket made from the ID and the field name.

## Write the main class

The main class follows the three steps from [How a Deephaven server starts](#how-a-deephaven-server-starts):

```java
package com.example;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.runner.MainHelper;

public final class MyServerMain {
    public static void main(String[] args) throws Exception {
        // Initialize logging, configuration, and process-wide state. Call this first.
        final Configuration configuration = MainHelper.init(args, MyServerMain.class);

        // Build the Dagger component and start the server. run() returns once the server is listening.
        final DeephavenApiServer server = new MyComponentFactory()
                .build(configuration)
                .getServer()
                .run();

        // Block until the server shuts down.
        server.join();
    }
}
```

Because `run` doesn't block, your application can do other work after the server starts, then call `join` when it has nothing left to do.

`MainHelper.init` accepts an optional single argument: the path of a properties file whose entries are loaded as system properties before anything else.

## Configure logging

Deephaven logs through [SLF4J](https://www.slf4j.org/). The build above uses Logback. Add a `src/main/resources/logback.xml` file that sends logs to the console and to the log buffer that the web UI's **Log** panel reads:

```xml
<configuration>
  <appender name="STDOUT" class="io.deephaven.logback.PrintStreamGlobalsConsole">
    <encoder>
      <pattern>%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z', UTC} | %-20.20thread | %5level | %-25.25logger{25} | %m%n</pattern>
    </encoder>
  </appender>

  <appender name="LOGBUFFER" class="io.deephaven.logback.LogBufferAppender">
    <encoder>
      <pattern>%-20.20thread | %-25.25logger{25} | %m</pattern>
    </encoder>
  </appender>

  <root level="info">
    <appender-ref ref="STDOUT" />
    <appender-ref ref="LOGBUFFER" />
  </root>
</configuration>
```

## Run the application

Start the application with Gradle:

```bash
./gradlew run
```

Or build a distribution with a launch script, and run that:

```bash
./gradlew installDist
./build/install/my-deephaven-app/bin/my-deephaven-app
```

The generated launch script includes the JVM arguments from `applicationDefaultJvmArgs` and adds anything in the `JAVA_OPTS` environment variable.

When the server is ready, the log shows `Server started on port 10000`. By default, the server uses [pre-shared key authentication](../authentication/auth-psk.md) with a random key, and logs a URL that includes the key:

```text
Connect automatically to Web UI with http://localhost:10000/?psk=<key>
```

Open that URL to use the web UI. The Groovy console works as it does in any Deephaven server, and the **Panels** menu lists the `ticking` and `static_table` fields from `MyApplication`.

## Configure the server

A custom application reads the same configuration as the production application:

- Configuration properties, set as JVM system properties or in a [configuration file](./config-file.md). For example, `-Dhttp.port=8080` changes the port, and `-Dauthentication.psk=<key>` sets the pre-shared key. See [Configuration properties](./configuration-properties.md) for more.
- The bootstrap settings for the application name and the data, cache, and config directories, described in [Configure the production application](./configure-production-application.md#deephaven-server-bootstrap-configuration).
- The authentication handlers, chosen with the `AuthHandlers` property. See the [authentication guides](../authentication/auth-psk.md) for the available handlers.

For example, to set the key and port when using the launch script:

```bash
JAVA_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE -Dhttp.port=8080" ./build/install/my-deephaven-app/bin/my-deephaven-app
```

## Related documentation

- [Install and run the Deephaven production application](../../getting-started/production-application.md)
- [Configure the production application](./configure-production-application.md)
- [Deephaven configuration files](./config-file.md)
- [Configuration properties](./configuration-properties.md)
- [Application Mode](../application-mode.md)
- [Liveness scope concept](../../conceptual/liveness-scope-concept.md)
- [Execution context](../../conceptual/execution-context.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Install and use Java packages](../install-and-use-java-packages.md)
