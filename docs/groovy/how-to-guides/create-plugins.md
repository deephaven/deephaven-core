---
title: Create your own plugin
---

This guide walks you through creating a plugin in Deephaven using Java or Groovy. Plugins in Deephaven are Java-based extensions that allow you to add custom functionality to the server, including custom object types, JavaScript plugins, and other extensions.

Deephaven plugins are implemented using the Java plugin API in the [`io.deephaven.plugin`](https://docs.deephaven.io/core/javadoc/io/deephaven/plugin/package-summary.html) package. Plugins can extend the server's functionality by registering custom object types, JavaScript plugins, or other extensions.

This guide focuses on creating object type plugins, which allow you to register custom objects that can be exported from the server and accessed by clients.

## Plugin structure

This example creates a plugin called `ExampleObjectType` that demonstrates how to create a custom object type plugin. The plugin will be implemented as a Java/Groovy class that extends the Deephaven plugin API.

Create the following directory and file structure for your plugin project:

```plaintext
ExamplePlugin/
├── build.gradle
├── src/main/java/
│   └── com/example/
│       ├── ExampleObjectType.java
│       └── ExamplePluginRegistration.java
└── src/main/resources/
    └── META-INF/services/
        └── io.deephaven.plugin.Registration
```

Alternatively, if using Groovy:

```plaintext
ExamplePlugin/
├── build.gradle
├── src/main/groovy/
│   └── com/example/
│       ├── ExampleObjectType.groovy
│       └── ExamplePluginRegistration.groovy
└── src/main/resources/
    └── META-INF/services/
        └── io.deephaven.plugin.Registration
```

> [!NOTE]
> This guide uses [Gradle](https://gradle.org/) for building the plugin. The plugin uses Java's ServiceLoader mechanism for registration, which requires the `META-INF/services` configuration.

### Plugin implementation

Deephaven plugins are implemented using the Java plugin API. The main components are:

- **ObjectType**: Defines a custom object type that can be exported from the server
- **Registration**: Registers the plugin with the Deephaven server using Java's ServiceLoader mechanism
- **Build configuration**: Gradle build file to compile and package the plugin

#### Creating an object type plugin

First, create the object type class. Here's an example in Java:

```java
package com.example;

import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;

public class ExampleObjectType extends ObjectTypeBase {
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
    public ObjectType.MessageStream compatibleClientConnection(
            Object object, ObjectType.MessageStream connection)
            throws ObjectCommunicationException {
        // For simple object types that don't need custom client communication,
        // you can use the FetchOnly implementation
        return ObjectTypeBase.FetchOnly.INSTANCE;
    }
}
```

Or in Groovy:

```groovy skip-test
package com.example

import io.deephaven.plugin.type.ObjectType
import io.deephaven.plugin.type.ObjectTypeBase
import io.deephaven.plugin.type.ObjectCommunicationException

class ExampleObjectType extends ObjectTypeBase {
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
    ObjectType.MessageStream compatibleClientConnection(
            Object object, ObjectType.MessageStream connection)
            throws ObjectCommunicationException {
        // For simple object types that don't need custom client communication,
        // you can use the FetchOnly implementation
        return ObjectTypeBase.FetchOnly.INSTANCE
    }
}
```

#### Creating the example object

You'll also need to create the actual object class that your plugin manages:

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

#### Plugin registration

Create a registration class that tells Deephaven about your plugin:

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

#### ServiceLoader configuration

Create the ServiceLoader configuration file at `src/main/resources/META-INF/services/io.deephaven.plugin.Registration`:

```
com.example.ExamplePluginRegistration
```

This file tells Java's ServiceLoader mechanism where to find your plugin registration class.

#### Build configuration

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
    implementation 'io.deephaven:deephaven-engine-api:0.36.1'
    implementation 'io.deephaven:deephaven-plugin:0.36.1'

    // If using Groovy
    implementation 'org.apache.groovy:groovy-all:4.0.15'

    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.2'
}

test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}
```

## Building and installing the plugin

### Build the plugin

To build your plugin, run:

```bash
./gradlew build
```

This creates a JAR file in the `build/libs/` directory.

### Install the plugin

To install your plugin in a Deephaven server:

1. **Copy the JAR file** to the server's classpath or plugin directory
2. **Restart the server** to load the plugin
3. **Create instances** of your custom objects in Groovy scripts

## Server setup

### Local installation

If running Deephaven locally, you can install your plugin by:

1. Building the plugin JAR as shown above
2. Adding the JAR to Deephaven's classpath when starting the server
3. Starting Deephaven with the plugin available

```bash
# Add your plugin JAR to the classpath when starting Deephaven
java -cp "deephaven-server.jar:your-plugin.jar" io.deephaven.server.runner.Main
```

### Docker deployment

For Docker deployments, create a Dockerfile that extends the Deephaven base image:

```dockerfile
FROM ghcr.io/deephaven/server:main

# Copy your plugin JAR
COPY build/libs/your-plugin.jar /opt/deephaven/lib/

# The plugin will be automatically loaded when the server starts
```

Create a Docker Compose file to run the server:

> [!IMPORTANT]
> The Docker Compose file below sets the pre-shared key to `YOUR_PASSWORD_HERE` for demonstration purposes. Change this to a more secure key in production.

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      # Replace 'YOUR_PASSWORD_HERE' with a strong, secure key in production
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

With these files, run `docker compose up` to start the server with your plugin installed.

## Server-side testing

### Creating test objects

To test your plugin on the server side, create instances of your custom objects and export them so clients can access them. Create a Groovy script to set up test objects:

```groovy skip-test
// Create an instance of your custom object
exampleObject = new com.example.ExampleObject("Hello from server plugin!")

// You can create multiple instances with different data
anotherExample = new com.example.ExampleObject("Another test message")

// The objects are automatically recognized by your ObjectType plugin
// and can be exported to clients
printf("Created example objects: %s, %s%n", exampleObject, anotherExample)
```

### Local testing

When running Deephaven locally with your plugin installed, you can test the plugin by:

1. Starting the Deephaven server with your plugin JAR in the classpath
2. Opening the Deephaven IDE in your browser
3. Running the test script above in the Groovy console
4. Verifying that the objects are created and can be exported

### Docker testing

When using Docker, add your test script to the container or run it through the IDE:

```groovy skip-test
// This script can be run in the Deephaven IDE after starting the Docker container
from com.example import ExampleObject

// Create test instances
testObject1 = new ExampleObject("Docker test 1")
testObject2 = new ExampleObject("Docker test 2")

printf("Docker test objects created successfully%n")
```

## Client-side implementation

### Groovy client setup

To interact with your plugin from a Groovy client, you'll need to create a client-side implementation that can connect to and communicate with your plugin objects on the server.

First, create a separate Groovy project for the client with its own `build.gradle`:

```gradle
plugins {
    id 'groovy'
    id 'application'
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'io.deephaven:deephaven-client-api:0.36.1'
    implementation 'org.apache.groovy:groovy-all:4.0.15'

    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.2'
}

test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

application {
    mainClass = 'com.example.client.ExamplePluginClient'
}
```

### Client implementation

Create a Groovy client that can connect to your plugin objects:

```groovy skip-test
package com.example.client

import io.deephaven.client.impl.Session
import io.deephaven.client.impl.SessionConfig
import io.deephaven.client.impl.authentication.ConfigAuthenticationHandler

class ExamplePluginClient {
    static void main(String[] args) {
        // Create a session configuration
        def config = SessionConfig.builder()
            .target("localhost:10000")
            // Replace "YOUR_PASSWORD_HERE" with a secure password in production
            .authenticationHandler(ConfigAuthenticationHandler.psk("YOUR_PASSWORD_HERE"))
            .build()

        // Create and connect the session
        def session = Session.connect(config).get()

        try {
            // Print the objects the server can export
            println("")
            println("Exportable Objects: ${session.exportableObjects().keySet()}")

            // Access exported objects by name
            def exportableObjects = session.exportableObjects()

            exportableObjects.each { name, ticket ->
                println("Found exported object: ${name}")

                // You can fetch the object and work with it
                def objectHandle = session.fetch(ticket)
                println("Fetched object: ${objectHandle}")
            }

        } finally {
            // Close the session
            session.close()
        }
    }
}
```

## Client-side testing

### Running the client

To test your client-side implementation:

1. **Start the Deephaven server** with your plugin installed
2. **Create test objects** on the server using the server-side testing scripts above
3. **Build and run the client**:

```bash
# From your client project directory
./gradlew build
./gradlew run
```

### Complete test example

Here's a complete example that demonstrates the full plugin workflow:

```groovy skip-test
package com.example.client

import io.deephaven.client.impl.Session
import io.deephaven.client.impl.SessionConfig
import io.deephaven.client.impl.authentication.ConfigAuthenticationHandler

class CompletePluginTest {
    static void main(String[] args) {
        println("Starting Deephaven plugin test...")

        // Create session configuration
        def config = SessionConfig.builder()
            .target("localhost:10000")
            .authenticationHandler(ConfigAuthenticationHandler.psk("YOUR_PASSWORD_HERE"))
            .build()

        // Connect to the server
        def session = Session.connect(config).get()
        println("Connected to Deephaven server")

        try {
            // List all exportable objects
            def exportableObjects = session.exportableObjects()
            println("\nAvailable objects on server:")
            exportableObjects.keySet().each { name ->
                println("  - ${name}")
            }

            // Test fetching each object
            exportableObjects.each { name, ticket ->
                try {
                    println("\nTesting object: ${name}")
                    def objectHandle = session.fetch(ticket)
                    println("Successfully fetched: ${objectHandle}")

                    // You can add more specific tests here based on your object type

                } catch (Exception e) {
                    println("Error fetching ${name}: ${e.message}")
                }
            }

            println("\nPlugin test completed successfully!")

        } catch (Exception e) {
            println("Test failed: ${e.message}")
            e.printStackTrace()
        } finally {
            session.close()
            println("Session closed")
        }
    }
}
```

> [!IMPORTANT]
> Replace `YOUR_PASSWORD_HERE` with the pre-shared key you set when starting the server.

### Using the plugin

Once your plugin is installed, you can create and export instances of your custom objects:

```groovy skip-test
// Create an instance of your custom object
exampleObject = new com.example.ExampleObject("Hello from plugin!")

// The object will be automatically recognized by your ObjectType plugin
// and can be exported to clients
```

Clients can then access these objects through the Deephaven client APIs. The object will be serialized according to the ObjectType's implementation.

## Advanced features

### Custom client-server communication

For plugins that need custom client-server communication beyond simple object export, you can implement a custom `MessageStream` in the `compatibleClientConnection` method. This allows you to handle bidirectional communication between the client and server.

The example above uses `ObjectTypeBase.FetchOnly.INSTANCE`, which is suitable for simple object types that only need to be fetched by clients without ongoing communication.

### Multiple object types

A single plugin can register multiple object types by calling `callback.register()` multiple times in the registration class:

```java
@Override
public void registerInto(Callback callback) {
    callback.register(ExampleObjectType.INSTANCE);
    callback.register(AnotherObjectType.INSTANCE);
}
```

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
