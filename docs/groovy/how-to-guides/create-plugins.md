---
title: Create your own plugin
---

This guide walks you through creating a plugin in Deephaven using Java or Groovy. Plugins written in Java or Groovy are JVM extensions that add custom functionality to the server, such as custom object types and [JavaScript plugins](./create-js-plugins.md) that the server serves to the web UI.

You implement a Deephaven plugin with the Java plugin API in the [`io.deephaven.plugin`](/core/javadoc/io/deephaven/plugin/package-summary.html) package.

This guide focuses on creating object type plugins, which register custom objects that the server can export and clients can fetch. An object type plugin communicates with clients in one of two ways:

- **Fetch-only**: The plugin extends [`ObjectTypeBase.FetchOnly`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.FetchOnly.html) and serializes the object once, as a single payload, when a client fetches it. The client can't send messages back to the object.
- **Bidirectional**: The plugin extends [`ObjectTypeBase`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html) and implements its [`compatibleClientConnection`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html#compatibleClientConnection(java.lang.Object,io.deephaven.plugin.type.ObjectType.MessageStream)) method to return a custom [`MessageStream`](/core/javadoc/io/deephaven/plugin/type/ObjectType.MessageStream.html). The client and server can then exchange messages for as long as the connection is open, for example to call methods on the server-side object.

This guide builds a fetch-only plugin and a Groovy client that fetches it. To call a bidirectional plugin from the Java client, see [Work with bidirectional plugins](./install-use-plugins.md#work-with-bidirectional-plugins). The [Python version of this guide](/core/docs/how-to-guides/create-plugins/) walks through a complete bidirectional plugin in Python.

## Create the plugin

### Project layout

This example creates an object type plugin class called `ExampleObjectType`, which manages objects of class `ExampleObject`. The project directory, `ExamplePlugin`, does not need to match either name.

Create the following directory and file structure for your plugin project:

```plaintext
ExamplePlugin/
├── build.gradle
├── src/main/java/
│   └── com/example/
│       ├── ExampleObject.java
│       ├── ExampleObjectType.java
│       └── ExamplePluginRegistration.java
└── src/main/resources/
    └── META-INF/services/
        └── io.deephaven.plugin.Registration
```

If you write the plugin in Groovy, use this structure instead:

```plaintext
ExamplePlugin/
├── build.gradle
├── src/main/groovy/
│   └── com/example/
│       ├── ExampleObject.groovy
│       ├── ExampleObjectType.groovy
│       └── ExamplePluginRegistration.groovy
└── src/main/resources/
    └── META-INF/services/
        └── io.deephaven.plugin.Registration
```

> [!NOTE]
> This guide uses [Gradle](https://gradle.org/) for building the plugin.

### Plugin components

A plugin project has five pieces, each covered below in the order you create them:

- The object itself (`ExampleObject`).
- The object type (`ExampleObjectType`), an [`ObjectType`](/core/javadoc/io/deephaven/plugin/type/ObjectType.html) that recognizes and serializes your object.
- The registration class (`ExamplePluginRegistration`), a [`Registration`](/core/javadoc/io/deephaven/plugin/Registration.html) that registers the object type.
- The `ServiceLoader` configuration file that names the registration class.
- The Gradle build file.

#### Create the object

First, create the object class that your plugin manages:

```java
package com.example;

public class ExampleObject {
    private final String message;

    public ExampleObject(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ExampleObject{message='" + message + "'}";
    }
}
```

Or in Groovy:

```groovy skip-test
package com.example

class ExampleObject {
    final String message

    ExampleObject(String message) {
        this.message = message
    }

    @Override
    String toString() {
        return "ExampleObject{message='$message'}"
    }
}
```

#### Create the object type

Next, create the object type class that recognizes and serializes `ExampleObject`. It extends [`ObjectTypeBase.FetchOnly`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.FetchOnly.html), which implements [`compatibleClientConnection`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html#compatibleClientConnection(java.lang.Object,io.deephaven.plugin.type.ObjectType.MessageStream)) for you. You implement [`writeCompatibleObjectTo`](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.FetchOnly.html#writeCompatibleObjectTo(io.deephaven.plugin.type.Exporter,java.lang.Object,java.io.OutputStream)), which writes the object's bytes once. Here's an example in Java:

```java
package com.example;

import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectTypeBase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ExampleObjectType extends ObjectTypeBase.FetchOnly {
    public static final ExampleObjectType INSTANCE = new ExampleObjectType();

    private ExampleObjectType() {
        super();
    }

    @Override
    public String name() {
        return "ExampleObject";
    }

    @Override
    public boolean isType(Object object) {
        return object instanceof ExampleObject;
    }

    @Override
    public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out)
            throws IOException {
        // FetchOnly sends these bytes to the client once and closes the connection
        out.write(((ExampleObject) object).getMessage().getBytes(StandardCharsets.UTF_8));
    }
}
```

Or in Groovy:

```groovy skip-test
package com.example

import io.deephaven.plugin.type.Exporter
import io.deephaven.plugin.type.ObjectTypeBase

import java.nio.charset.StandardCharsets

class ExampleObjectType extends ObjectTypeBase.FetchOnly {
    static final ExampleObjectType INSTANCE = new ExampleObjectType()

    private ExampleObjectType() {
        super()
    }

    @Override
    String name() {
        return "ExampleObject"
    }

    @Override
    boolean isType(Object object) {
        return object instanceof ExampleObject
    }

    @Override
    void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out)
            throws IOException {
        // FetchOnly sends these bytes to the client once and closes the connection
        out.write(((ExampleObject) object).message.getBytes(StandardCharsets.UTF_8))
    }
}
```

The [`name`](/core/javadoc/io/deephaven/plugin/type/ObjectType.html#name()) method returns the type name, `ExampleObject`, that clients use to fetch these objects. If you change it, update the client in [Client implementation](#client-implementation) as well.

#### Create the registration class

Create a registration class that registers `ExampleObjectType` with the server:

```java
package com.example;

import io.deephaven.plugin.Registration;

public class ExamplePluginRegistration implements Registration {
    @Override
    public void registerInto(Callback callback) {
        callback.register(ExampleObjectType.INSTANCE);
    }
}
```

Or in Groovy:

```groovy skip-test
package com.example

import io.deephaven.plugin.Registration

class ExamplePluginRegistration implements Registration {
    @Override
    void registerInto(Callback callback) {
        callback.register(ExampleObjectType.INSTANCE)
    }
}
```

To register more than one object type from the same plugin, call [`callback.register`](/core/javadoc/io/deephaven/plugin/Registration.Callback.html#register(io.deephaven.plugin.Plugin)) once for each:

```java
@Override
public void registerInto(Callback callback) {
    callback.register(ExampleObjectType.INSTANCE);
    callback.register(AnotherObjectType.INSTANCE);
}
```

Or in Groovy:

```groovy skip-test
@Override
void registerInto(Callback callback) {
    callback.register(ExampleObjectType.INSTANCE)
    callback.register(AnotherObjectType.INSTANCE)
}
```

#### Create the ServiceLoader configuration file

Create the `ServiceLoader` configuration file at `src/main/resources/META-INF/services/io.deephaven.plugin.Registration`:

```plaintext
com.example.ExamplePluginRegistration
```

At startup, the server uses Java's [`ServiceLoader`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html) mechanism to read this file and load the registration class it names.

#### Create the Gradle build file

Create a `build.gradle` file for your plugin:

```gradle
plugins {
    id 'java-library'
    id 'groovy'
}

repositories {
    mavenCentral()
}

dependencies {
    // Use the same version as your Deephaven server, which provides these classes at runtime
    compileOnly 'io.deephaven:deephaven-plugin:42.7'

    // Needed only if you write the plugin in Groovy: use the Groovy version that the server runs
    compileOnly 'org.codehaus.groovy:groovy:3.0.25'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}
```

## Build and install the plugin

### Build the plugin

If the project doesn't have a Gradle wrapper yet, create one with `gradle wrapper`. Then build your plugin:

```bash
./gradlew build
```

This creates `build/libs/ExamplePlugin.jar`. The build file sets no version, so Gradle names the JAR after the project folder.

### Install the plugin

To install your plugin, add the JAR to the Deephaven server's classpath and restart the server. The server loads the plugin at startup.

#### Server built from source

If you run Deephaven from a build of the source code:

1. From the root of the deephaven-core repository, build the server and its start script with `./gradlew server-jetty-app:installDist`.
2. Start the server with the `EXTRA_CLASSPATH` environment variable set to the plugin JAR, as in the command below.

The command also passes two options in `START_OPTS`:

- `-Ddeephaven.console.type=groovy` starts the server with a Groovy console instead of the default Python console.
- `-Dauthentication.psk` sets the [pre-shared key](./authentication/auth-psk.md) that clients supply when they connect. Outside of testing, replace `YOUR_PASSWORD_HERE` with a secure key.

```bash
# Add your plugin JAR to the classpath when starting Deephaven
START_OPTS="-Ddeephaven.console.type=groovy -Dauthentication.psk=YOUR_PASSWORD_HERE" EXTRA_CLASSPATH="/path/to/ExamplePlugin/build/libs/ExamplePlugin.jar" ./server/jetty-app/build/install/server-jetty/bin/start
```

See [Install and use Java packages](./install-and-use-java-packages.md) for more about building the server and using `EXTRA_CLASSPATH`.

#### Docker deployment

For Docker deployments, save a Dockerfile in the root folder of the plugin project, `ExamplePlugin/`, that extends Deephaven's Groovy server image, `server-slim`:

```Dockerfile
FROM ghcr.io/deephaven/server-slim:latest

# Copy your plugin JAR into /apps/libs, which is on the server classpath
COPY build/libs/ExamplePlugin.jar /apps/libs/
```

> [!IMPORTANT]
> The Docker Compose file below sets the [pre-shared key](./authentication/auth-psk.md) to `YOUR_PASSWORD_HERE` for demonstration purposes. The pre-shared key is the password that clients supply when they connect to the server. Change this to a more secure key in production.

Save the following Docker Compose file in the same folder:

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

With those two files in hand, `docker compose up --build` builds the image with the plugin installed and starts the server. Use `--build` again whenever you rebuild the plugin JAR.

## Server-side testing

To test your plugin on the server side, create instances of your custom objects so clients can access them. Once the server is running with your plugin installed, locally or in Docker, open the Deephaven IDE at `http://localhost:10000/ide/`, log in with the pre-shared key you set, and run the following script in the Groovy console:

```groovy skip-test
// Create an instance of your custom object
exampleObject = new com.example.ExampleObject("Hello from server plugin!")

// You can create multiple instances with different data
anotherExample = new com.example.ExampleObject("Another test message")

// Your ObjectType plugin's isType method matches these objects,
// so the server can export them to clients
printf("Created example objects: %s, %s%n", exampleObject, anotherExample)
```

Clients can then access these objects through the Deephaven client APIs. The plugin's [`ObjectType`](/core/javadoc/io/deephaven/plugin/type/ObjectType.html) implementation serializes each object.

## Client-side implementation

### Set up the client project

To interact with your plugin from a client application written in Groovy, use the [Java client](./java-client.md) library to connect to and communicate with your plugin objects on the server.

First, create a separate Groovy project for the client. Put the client class from [Client implementation](#client-implementation) at `src/main/groovy/com/example/client/ExamplePluginClient.groovy`, and give the project its own `build.gradle`:

```gradle
plugins {
    id 'groovy'
    id 'application'
}

repositories {
    mavenCentral()
}

dependencies {
    // Use the same version as your Deephaven server
    implementation 'io.deephaven:deephaven-java-client-session:42.7'
    implementation 'org.apache.groovy:groovy-all:4.0.15'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = 'com.example.client.ExamplePluginClient'
}
```

### Client implementation

> [!IMPORTANT]
> Replace `YOUR_PASSWORD_HERE` with the pre-shared key you set when starting the server.

Create a Groovy client that fetches the `exampleObject` variable created by the [server-side test script](#server-side-testing). The client connects with the Java client session API and refers to the server object by a [typed ticket](/core/javadoc/io/deephaven/client/impl/TypedTicket.html), which pairs a client-side reference to the object with the object's type. The type, `ExampleObject`, must match the value that the plugin's [`name`](/core/javadoc/io/deephaven/plugin/type/ObjectType.html#name()) method returns.

For more about using plugin objects from Java, see [Use plugins from the Java client](./install-use-plugins.md#use-plugins-from-the-java-client).

```groovy skip-test
package com.example.client

import io.deephaven.client.impl.ClientConfig
import io.deephaven.client.impl.ScopeId
import io.deephaven.client.impl.SessionConfig
import io.deephaven.client.impl.SessionFactoryConfig
import io.deephaven.client.impl.TypedTicket
import io.deephaven.uri.DeephavenTarget

import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

class ExamplePluginClient {
    static void main(String[] args) {
        def scheduler = Executors.newScheduledThreadPool(4)
        def factory = SessionFactoryConfig.builder()
            .clientConfig(ClientConfig.builder()
                .target(DeephavenTarget.of(URI.create("dh+plain://localhost:10000")))
                .build())
            .sessionConfig(SessionConfig.builder()
                // Replace YOUR_PASSWORD_HERE with the server's pre-shared key
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler YOUR_PASSWORD_HERE")
                .build())
            .scheduler(scheduler)
            .build()
            .factory()

        // Create and connect the session
        def session = factory.newSession()

        try {
            // Fetch the exampleObject variable from the server's query scope
            def fetchable = session.fetchable(new TypedTicket("ExampleObject", new ScopeId("exampleObject"))).get()
            try {
                def serverData = fetchable.fetch().get()
                try {
                    def data = serverData.data()
                    def bytes = new byte[data.remaining()]
                    data.get(bytes)
                    println("Fetched object: ${new String(bytes, StandardCharsets.UTF_8)}")
                } finally {
                    serverData.close()
                }
            } finally {
                fetchable.close()
            }
        } finally {
            // Close the session and release its resources
            session.close()
            factory.managedChannel().shutdown()
            scheduler.shutdown()
        }
    }
}
```

## Client-side testing

To test your client-side implementation:

1. **Start the Deephaven server** with your plugin installed.
2. **Create test objects** on the server with the script in [Server-side testing](#server-side-testing).
3. **Build and run the client**. If the client project doesn't have a Gradle wrapper yet, create one with `gradle wrapper` first:

   ```bash
   # From your client project directory
   ./gradlew build
   ./gradlew run
   ```

The client prints `Fetched object: Hello from server plugin!`, the bytes that `ExampleObjectType` wrote for the `exampleObject` variable.

## Related documentation

- [Configure and use pre-shared key authentication](./authentication/auth-psk.md)
- [Create a JavaScript plugin](./create-js-plugins.md)
- [Extract data with a Java client](./java-client.md)
- [Install and run Deephaven with Docker](../getting-started/docker-install.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Install and use plugins](./install-use-plugins.md)
- [`io.deephaven.plugin` Javadoc](/core/javadoc/io/deephaven/plugin/package-summary.html)
- [`ObjectTypeBase.FetchOnly` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.FetchOnly.html)
- [`ObjectType` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectType.html)
- [`ObjectType.MessageStream` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectType.MessageStream.html)
- [`ObjectTypeBase` Javadoc](/core/javadoc/io/deephaven/plugin/type/ObjectTypeBase.html)
- [`Registration` Javadoc](/core/javadoc/io/deephaven/plugin/Registration.html)
- [`SessionFactoryConfig` Javadoc](/core/javadoc/io/deephaven/client/impl/SessionFactoryConfig.html)
- [`TypedTicket` Javadoc](/core/javadoc/io/deephaven/client/impl/TypedTicket.html)
