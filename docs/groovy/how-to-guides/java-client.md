---
title: Extract data with a Java client
---

This guide shows you how to connect to a Deephaven server with a Java client, subscribe to a ticking table (a table whose rows update as new data arrives), and run code on the server from the client. The example simulates weather readings for a set of cities on the server and displays the latest reading for each city in a Java application.

## Prerequisites

- Run a Deephaven server with Python as its scripting language, such as the `ghcr.io/deephaven/server` image. See the [Python Docker install guide](/core/docs/getting-started/docker-install/). The server script and the client's `console("python")` call need a Python server, so the Groovy image, `server-slim`, can't run them.
- Set up a Java project that depends on `io.deephaven:deephaven-java-client-barrage-dagger`, which includes the [Barrage](../conceptual/what-is-barrage.md), [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), and session client libraries used below.
- Run the client JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`, which Arrow needs on Java 16 and later. With Deephaven 43.0 and later on Java 25 and later, also pass `-Dio.netty.noUnsafe=false`. See [JVM configuration](./java-client-input-tables.md#jvm-configuration).
- Optionally, clone the [Deephaven Community repository](https://github.com/deephaven/deephaven-core) to run the complete client examples described in [The Java client](#the-java-client).

## The application

The application has two parts:

- **The Python weather server**: a Deephaven script that creates a ticking table of simulated weather readings for a set of cities and derives a table that holds the latest reading for each city. The script also defines a Python function that lets the Java client add more cities to watch.
- **The Java client**: a simple Java application that connects to the Deephaven server and displays the ticking results. It can also ask the server to watch more cities.

The following sections walk through each part. The server script is complete and runnable. The complete Java client isn't published, so this guide shows the parts of its code that connect to the server, subscribe to the table, listen for updates, and run code on the server.

## Run the Python weather server

With Deephaven running, open the Deephaven IDE and run the following weather server script.

```python skip-test
from deephaven import input_table, new_table, time_table
from deephaven import dtypes as dht
from deephaven.column import string_col

# An input table that holds the cities to watch, one row per city
cities = input_table(col_defs={"City": dht.string}, key_cols="City")

# Every second, record a simulated reading for each watched city
readings = (
    time_table("PT1s")
    .tail(1)
    .join(cities)
    .update(
        [
            "Temp = 60 + 20 * Math.sin(epochSeconds(Timestamp) / 30.0 + City.hashCode())",
            "Humidity = 50 + 30 * Math.cos(epochSeconds(Timestamp) / 45.0 + City.hashCode())",
        ]
    )
)

# Keep the latest reading for each city
latest_by_city = readings.last_by("City")


# Add a city to the set of watched cities
def begin_watch(city):
    cities.add(new_table([string_col("City", [city])]))
```

The script defines the following:

- `cities` is a keyed [input table](/core/docs/how-to-guides/input-tables/) that holds the cities to watch. `City` is its key column, so adding a city that's already in the table replaces that city's row instead of adding a second one.
- `readings` is the ticking table of weather readings. [`time_table`](/core/docs/reference/table-operations/create/timeTable/) adds a row with a `Timestamp` every second, [`tail(1)`](/core/docs/reference/table-operations/filter/tail/) keeps only the newest of those rows, [`join`](/core/docs/reference/table-operations/join/join/) pairs that timestamp with every watched city, and [`update`](/core/docs/reference/table-operations/select/update/) computes simulated `Temp` and `Humidity` values from the timestamp and the city name.
- `latest_by_city` uses [`last_by`](/core/docs/reference/table-operations/group-and-aggregate/lastBy/) to keep the latest reading for each city. The Java client subscribes to this table.
- `begin_watch` adds a city to `cities`. The Java client calls this function.

At this point, the weather server is ready to provide data to the Java client. The script watches no cities until something calls `begin_watch`. The Java client does that, and you can also call it in the IDE, for example with `begin_watch("Denver")`.

## The Java client

The Java client is a Swing UI that subscribes to the server's `latest_by_city` table and calls `begin_watch` to add cities.

![The Java client UI](../assets/tutorials/java-client/java-app.png)

In the finished application, the user enters the server's host and port, such as `localhost:10000`, in the **Host** text box and clicks **Connect**. Next, the user types a city name, such as `Denver`, in the **Location** text box and clicks **Add**. The UI then displays the latest simulated reading for that city, and the reading updates every second.

For complete, runnable clients, see the examples in the repository's [`java-client/barrage-examples`](https://github.com/deephaven/deephaven-core/tree/main/java-client/barrage-examples) directory, such as [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java).

The Deephaven Java client can get table data from the server in two ways:

- A lightweight API that fetches [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) data streams, for when you want your data in Arrow Flight format. For examples, see the repository's [`java-client/flight-examples`](https://github.com/deephaven/deephaven-core/tree/main/java-client/flight-examples) directory.
- Table subscriptions and snapshots over the [Barrage](../conceptual/what-is-barrage.md) streaming protocol. A subscription delivers a server table to the client as a local, ticking Deephaven table, and a snapshot delivers it as a local static table.

The [Arrow Flight guide](./data-import-export/arrow-flight.md) lists the Arrow types Deephaven supports.

This guide uses table subscriptions to get data, and a console session to run code on the server.

The snippets in the following sections use these variables from the application:

- `host` is the server's host and port, such as `localhost:10000`.
- `plaintext` is `true` when the server doesn't use TLS.
- `psk` is the server's [pre-shared key (PSK)](./authentication/auth-psk.md), which the server prints in its startup log.
- `flightScheduler` is a [`ScheduledExecutorService`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ScheduledExecutorService.html), such as one created with `Executors.newScheduledThreadPool(4)`. It runs the client's background work, including refreshing the session token.
- `city` is the city name the user typed.
- `log` is the application's logger, a Deephaven [`Logger`](/core/javadoc/io/deephaven/io/logger/Logger.html). Substitute your application's own logger.
- `managedChannel`, `bufferAllocator`, and `barrageSession` are fields of type `ManagedChannel`, `BufferAllocator`, and `BarrageSession`, which the snippets below assign.

They also use these imports:

```java
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactory;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.DaggerDeephavenBarrageRoot;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
```

### Connect to the server

The first step is to create a [`ManagedChannel`](https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannel.html), the root connection between your application and the Deephaven server. The Java client creates most objects with builders. Here, the code configures a [`ManagedChannelBuilder`](https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html) and then calls `build`.

```java
final ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(host);
if (plaintext) {
    channelBuilder.usePlaintext();
} else {
    channelBuilder.useTransportSecurity();
}

managedChannel = channelBuilder.build();
```

Once you have a channel to the server, create a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html). `BarrageSession` extends [`FlightSession`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) and adds table subscriptions and snapshots. `FlightSession` adds Arrow Flight data transfer to a Deephaven session.

By default, a Deephaven server requires a PSK. Pass it with `authenticationTypeAndValue`, prefixed by the name of the PSK authentication handler.

```java
bufferAllocator = new RootAllocator();
final BarrageSessionFactory barrageSessionFactory =
        DaggerDeephavenBarrageRoot.create().factoryBuilder()
                .managedChannel(managedChannel)
                .scheduler(flightScheduler)
                .allocator(bufferAllocator)
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + psk)
                .build();

barrageSession = barrageSessionFactory.newBarrageSession();
```

Instead of building the channel yourself, you can create the session from a [`BarrageSessionFactoryConfig`](/core/javadoc/io/deephaven/client/impl/BarrageSessionFactoryConfig.html), as the [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example does.

### Subscribe to the table

The server script stores the result table in a [query scope](/core/docs/how-to-guides/query-scope/) variable named `latest_by_city`. A remote client asks for a server table with a Flight ticket, which identifies one table stream. To build the ticket for a query scope variable, prefix the variable name with `s/`, as in `s/latest_by_city`. A client can fetch any table in the query scope the same way.

The subscription produces a local Deephaven table that the Deephaven engine inside the client process maintains. Before it subscribes, the client creates two objects:

- An [update graph](../conceptual/dag.md), which delivers each update to the local table and its listeners.
- An [execution context](../conceptual/execution-context.md), which gives the subscribing thread access to that update graph.

The code also opens a [liveness scope](../conceptual/liveness-scope-concept.md), which holds the subscription and the subscribed table. Closing the scope releases them, and the server stops spending memory and CPU time on the subscription. Keep the scope open for as long as you read from or listen to the table, and close it when you finish, as the `try`-with-resources block below does.

To subscribe, the application does three things:

1. Calls [`TicketTable.fromQueryScopeField`](/core/javadoc/io/deephaven/qst/table/TicketTable.html) to build the `s/` ticket from the variable name, and asks the server for a [`TableHandle`](/core/javadoc/io/deephaven/client/impl/TableHandle.html) for that ticket.
2. Calls [`subscribe`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html) on the `BarrageSession` to subscribe to that handle.
3. Calls [`entireTable`](/core/javadoc/io/deephaven/client/impl/BarrageSubscription.html), which returns a [`Future`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/Future.html). The `Future` completes with the subscribed [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) once all of its rows arrive.

```java
// The subscribed table is a client-side engine table, so the client needs an update graph and an execution context
final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();
final ExecutionContext executionContext = ExecutionContext.newBuilder()
        .markSystemic()
        .emptyQueryScope()
        .newQueryLibrary()
        .setUpdateGraph(updateGraph)
        .build();

final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();

try (final SafeCloseable ignoredContext = executionContext.open();
        final SafeCloseable ignoredScope = LivenessScopeStack.open();
        final TableHandle handle = barrageSession.session()
                .of(TicketTable.fromQueryScopeField("latest_by_city"))) {
    final BarrageSubscription subscription = barrageSession.subscribe(handle, options);
    final Table latestTable = subscription.entireTable().get();

    // Read from or listen to latestTable here
}
```

Inside the `try` block, `latestTable` is a local copy of the server's `latest_by_city` table, and it updates as the server's data changes. Use it only inside the block: when the block exits, the liveness scope closes and releases the table. This application displays the table directly. Other applications can filter, join, or aggregate it like any other Deephaven table.

The steps above are adapted from the [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example.

### Listen to the table

Once the client has a local ticking table, such as `latestTable` from [Subscribe to the table](#subscribe-to-the-table), it can process the table's data. You can read the data from the table directly. You can also add a listener to the table so that the application responds to each update, as the [table listeners guide](./table-listeners-groovy.md) explains.

Read the table or add the listener inside the `try` block that opened the liveness scope, and keep that block open for as long as the listener should receive updates. The [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example does this by waiting on a [`CountDownLatch`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CountDownLatch.html) inside the block until it has received the number of updates it asked for. When the block exits, the scope closes and releases the subscription and the table, and the listener stops receiving updates.

```java
final InstrumentedTableUpdateListenerAdapter listener = new InstrumentedTableUpdateListenerAdapter(latestTable, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        // Process the update as needed. The TableUpdate describes:
        // 1) What rows have been added
        // 2) What rows have been removed
        // 3) What rows have been modified
        // 4) What rows have been structurally shifted in address space, but without changes to column data
        // 5) What columns were affected by the changes
    }
};
latestTable.addUpdateListener(listener);

// ... keep the try block open for as long as you need updates, then:
latestTable.removeUpdateListener(listener);
```

The `false` argument tells [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html) not to keep its own reference to the listener, so the code must keep the listener reachable itself. Being in scope isn't enough, because the JVM can collect a local variable that the code no longer uses. The final `removeUpdateListener` call uses `listener` at the end, which keeps it reachable until then, as the `SubscribeTable` example does. Without it, the JVM can garbage collect the listener, and the listener stops receiving updates.

### Run code on the server

The Java client can also run code on the server through a [`ConsoleSession`](/core/javadoc/io/deephaven/client/impl/ConsoleSession.html). For example, the client can create more derived tables with the same query language you use in the Deephaven IDE.

This example lets the user add cities to the set of watched cities.

```java
try (final ConsoleSession console = barrageSession.session().console("python").get()) {
    final Changes c = console.executeCode("begin_watch(\"" + escapeString(city) + "\")");
    if (c.errorMessage().isPresent()) {
        log.error().append("Error adding " + city + ": " + c.errorMessage().get()).endl();
    } else {
        log.info().append("Added " + city).endl();
    }
} catch (InterruptedException e) {
    // Restore the interrupt so the calling code can see it
    Thread.currentThread().interrupt();
} catch (ExecutionException | TimeoutException e) {
    e.printStackTrace();
}
```

This code sends the server a Python call to `begin_watch` that passes the escaped city name. It then checks the response for an error. The console type, `"python"`, must match the server's scripting language. The server returns a [`Changes`](/core/javadoc/io/deephaven/client/impl/script/Changes.html) object, which tells the application what changed in the server's scope, such as which tables and variables the command created or updated.

> [!WARNING]
> This example embeds a user string into a command. `escapeString` stands for a helper that you write to escape quotes and other special characters within the string. Without it, a user can inject arbitrary code into the command that the server runs.

To send data from the client without running code on the server, use [input tables](./java-client-input-tables.md).

### Close the connection

When the application finishes, close the session and release the objects it used. Closing the `BarrageSession` doesn't shut down the channel, so shut down the channel and the scheduler separately, and then close the allocator:

```java
// Ask the server to close the session, wait for it to acknowledge, then close the client side
try {
    barrageSession.session().closeFuture().get(5, TimeUnit.SECONDS);
} finally {
    // Release the client-side resources even if the server doesn't acknowledge in time
    barrageSession.close();
    managedChannel.shutdownNow();
    flightScheduler.shutdownNow();
    bufferAllocator.close();
}
```

## Related documentation

- [Send data to Deephaven from a Java client](./java-client-input-tables.md)
- [Arrow Flight](./data-import-export/arrow-flight.md)
- [Servers and clients](../conceptual/client-server-model.md)
