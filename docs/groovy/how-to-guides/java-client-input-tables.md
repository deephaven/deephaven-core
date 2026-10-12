---
title: Send data to Deephaven from a Java client
---

This guide shows how to send data to Deephaven from an external Java application using the Deephaven Java client and input tables. An input table is a Deephaven table that a client can write to. Every type of input table accepts new rows, and keyed input tables also let the client update and delete rows.

> [!NOTE]
> If your data source can run directly on the Deephaven server, write the data from a server-side script instead. You can [create and use input tables](./input-tables.md) there, or use a [`TablePublisher` or `DynamicTableWriter`](./table-publisher.md). Server-side ingestion is generally more efficient because it avoids network overhead.

## When to write to input tables from a Java client

Common reasons to write to an input table from a Java client include:

- **Managing reference data**: Upload configuration, lookup tables, or static datasets that may need updates.
- **Tracking state**: Maintain the latest status per entity (e.g., device status, order state, user preferences).
- **Interactive editing**: Allow users or external systems to add, update, or remove records.
- **Forwarding external data**: Relay data from message queues, APIs, or sensors running in separate JVMs.

## Setup

Add the Deephaven Java client dependencies to your project. Replace `<version>` with the version that matches your Deephaven server (e.g., `42.6`). For Gradle:

```groovy syntax
dependencies {
    implementation 'io.deephaven:deephaven-java-client-flight-dagger:<version>'
    implementation 'io.deephaven:deephaven-qst:<version>'
}
```

### JVM configuration

The Java client transfers data with Apache Arrow, which needs reflective access to `java.nio.Buffer`. Add this JVM option to avoid an `InaccessibleObjectException` on Java 16 and later:

```text syntax
--add-opens=java.base/java.nio=ALL-UNNAMED
```

With Deephaven 43.0 and later on Java 25 and later, also add `-Dio.netty.noUnsafe=false`. Without it, Arrow's Netty allocator fails with a `ClassCastException` (`PooledDirectByteBuf cannot be cast to ... PooledUnsafeDirectByteBuf`).

The following snippets include both options. Keep `-Dio.netty.noUnsafe=false` on earlier Java versions too. It's harmless there because it sets the value Netty already uses.

With Gradle's `application` plugin, set the default JVM arguments, which apply to the `run` task and the generated start scripts:

```groovy syntax
application {
    applicationDefaultJvmArgs = ['--add-opens=java.base/java.nio=ALL-UNNAMED', '-Dio.netty.noUnsafe=false']
}
```

To set them on one `JavaExec` task only, such as `run`, configure that task:

```groovy syntax
tasks.named('run', JavaExec) {
    jvmArgs += ['--add-opens=java.base/java.nio=ALL-UNNAMED', '-Dio.netty.noUnsafe=false']
}
```

### Authentication

By default, the Deephaven server requires [pre-shared key (PSK) authentication](./authentication/auth-psk.md). A Java client that sets no authentication connects as anonymous, and the server rejects it.

To authenticate, pass your key to `authenticationTypeAndValue` when you build the [`FlightSession`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) that connects to the server. The value is `io.deephaven.authentication.psk.PskAuthenticationHandler` followed by a space and the key.

The [complete example](#complete-example) builds the session like this, reading the key from the `DEEPHAVEN_PSK` environment variable. Set that variable to your server's key before you run it.

```java
FlightSession flight = DaggerDeephavenFlightRoot.create().factoryBuilder()
        .managedChannel(channel)
        .scheduler(scheduler)
        .allocator(allocator)
        .authenticationTypeAndValue(
                "io.deephaven.authentication.psk.PskAuthenticationHandler "
                        + System.getenv("DEEPHAVEN_PSK"))
        .build()
        .newFlightSession();
```

If your server has [anonymous authentication](./authentication/auth-anon.md) enabled, remove the `authenticationTypeAndValue` call.

## Basic pattern

The basic pattern for sending data from a Java client is:

1. **Create a [`FlightSession`](/core/javadoc/io/deephaven/client/impl/FlightSession.html)** to connect to the Deephaven server.
2. **Define an input table spec**, a client-side description of the table that doesn't exist on the server until you execute it. List the columns in a [`TableHeader`](/core/javadoc/io/deephaven/qst/table/TableHeader.html) built from [`ColumnHeader`](/core/javadoc/io/deephaven/qst/column/header/ColumnHeader.html)s, then pass the header to [`InMemoryKeyBackedInputTable`](/core/javadoc/io/deephaven/qst/table/InMemoryKeyBackedInputTable.html), [`InMemoryAppendOnlyInputTable`](/core/javadoc/io/deephaven/qst/table/InMemoryAppendOnlyInputTable.html), or [`BlinkInputTable`](/core/javadoc/io/deephaven/qst/table/BlinkInputTable.html), which [Input table types](#input-table-types) compares.
3. **Execute the spec** to create the input table on the server. Execution returns a [`TableHandle`](/core/javadoc/io/deephaven/client/impl/TableHandle.html) for the new table.
4. **Publish the input table** to the [query scope](./query-scope.md) under a variable name. This makes the table visible on the server and keeps a server-side reference to it.
5. **Close the handle** to release the server-side [export](#tablehandle-lifecycle) it holds. The published table stays alive through the query scope.
6. **Build data rows** using [`ColumnHeader`](/core/javadoc/io/deephaven/qst/column/header/ColumnHeader.html) and [`NewTable`](/core/javadoc/io/deephaven/qst/table/NewTable.html).
7. **Add to the input table** with [`FlightSession.addToInputTable`](/core/javadoc/io/deephaven/client/impl/FlightSession.html). Pass a [`ScopeId`](/core/javadoc/io/deephaven/client/impl/ScopeId.html), which refers to the published table by its query-scope variable name (for example, `new ScopeId("device_status")`). For keyed tables, this inserts new rows or updates existing ones.

Repeat steps 6-7 as needed. To remove rows from a keyed input table, call [`FlightSession.deleteFromInputTable`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) with a table of key values instead, as shown in [Delete rows from a keyed input table](#delete-rows-from-a-keyed-input-table).

## Input table types

The Java client supports three types of input tables. Each type's `of` method takes a table schema. The snippets below pass a [`TableHeader`](/core/javadoc/io/deephaven/qst/table/TableHeader.html) named `header`, which lists the column names and types. The [complete example](#complete-example) shows how to build one.

### Keyed

Create a keyed input table with [`InMemoryKeyBackedInputTable`](/core/javadoc/io/deephaven/qst/table/InMemoryKeyBackedInputTable.html). The key columns identify each row. Adding a row with an existing key replaces that row. You can also delete rows, as shown in [Delete rows from a keyed input table](#delete-rows-from-a-keyed-input-table).

```java
// Single key column
TableSpec singleKeySpec = InMemoryKeyBackedInputTable.of(header, java.util.List.of("DeviceId"));

// Multiple key columns
TableSpec multiKeySpec = InMemoryKeyBackedInputTable.of(header, java.util.List.of("DeviceId", "Region"));
```

### Append-only

Create an append-only input table with [`InMemoryAppendOnlyInputTable`](/core/javadoc/io/deephaven/qst/table/InMemoryAppendOnlyInputTable.html). Each new row goes at the end of the table. The table has no key columns and doesn't support updates or deletion. Use this when you need to keep every row you send, such as a log or event history.

```java
TableSpec spec = InMemoryAppendOnlyInputTable.of(header);
```

### Blink

Create a blink input table with [`BlinkInputTable`](/core/javadoc/io/deephaven/qst/table/BlinkInputTable.html). In a [blink table](../conceptual/table-types.md#specialization-3-blink), rows are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. You can't delete rows from a blink input table. Use this for event streams where downstream queries only need each new batch, not the history.

```java
TableSpec spec = BlinkInputTable.of(header);
```

## Complete example

The following example tracks device status using a keyed input table. Each device has a unique ID, and updates replace the previous status for that device.

```java
import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DeviceStatusTracker {

    public static void main(String[] args) throws Exception {
        // Each resource is wrapped so cleanup happens even if later initialization fails
        BufferAllocator allocator = new RootAllocator();
        try {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            try {
                ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:10000")
                        .usePlaintext()
                        .build();
                try {
                    FlightSession flight = DaggerDeephavenFlightRoot.create().factoryBuilder()
                            .managedChannel(channel)
                            .scheduler(scheduler)
                            .allocator(allocator)
                            .authenticationTypeAndValue(
                                    "io.deephaven.authentication.psk.PskAuthenticationHandler "
                                            + System.getenv("DEEPHAVEN_PSK"))
                            .build()
                            .newFlightSession();
                    try {
                        runExample(flight, allocator);
                    } finally {
                        // Wait for session close before shutting down the channel
                        try {
                            flight.session().closeFuture().get(5, TimeUnit.SECONDS);
                        } finally {
                            flight.close();
                        }
                    }
                } finally {
                    channel.shutdownNow();
                }
            } finally {
                scheduler.shutdownNow();
            }
        } finally {
            allocator.close();
        }
    }

    private static void runExample(FlightSession flight, BufferAllocator allocator)
            throws Exception {
        // Define schema with column headers
        ColumnHeader<String> deviceIdCol = ColumnHeader.ofString("DeviceId");
        ColumnHeader<String> statusCol = ColumnHeader.ofString("Status");
        ColumnHeader<Instant> lastSeenCol = ColumnHeader.ofInstant("LastSeen");

        TableHeader header = TableHeader.of(deviceIdCol, statusCol, lastSeenCol);

        // Create a keyed input table - DeviceId is the key
        // Adding a row with an existing DeviceId updates that row
        TableSpec inputTableSpec = InMemoryKeyBackedInputTable.of(
                header, java.util.List.of("DeviceId"));

        // Use try-with-resources to ensure handle is closed even if publish() fails
        ScopeId scopeId;
        try (TableHandle inputTableHandle = flight.session().execute(inputTableSpec)) {
            // Publish so it's visible in the UI (check http://localhost:10000/ide)
            flight.session().publish("device_status", inputTableHandle).get(5, TimeUnit.SECONDS);
            // Handle closes here - the server-side table persists via the query scope
            scopeId = new ScopeId("device_status");
        }

        // Add initial devices
        updateDevice(flight, scopeId, allocator, deviceIdCol, statusCol, lastSeenCol,
                "sensor-001", "online");
        updateDevice(flight, scopeId, allocator, deviceIdCol, statusCol, lastSeenCol,
                "sensor-002", "online");
        updateDevice(flight, scopeId, allocator, deviceIdCol, statusCol, lastSeenCol,
                "sensor-003", "offline");

        // Update sensor-001's status (replaces the previous row)
        updateDevice(flight, scopeId, allocator, deviceIdCol, statusCol, lastSeenCol,
                "sensor-001", "maintenance");

        // Delete sensor-003. The delete table holds only the key column; see
        // "Delete rows from a keyed input table" below
        NewTable keysToDelete = deviceIdCol.row("sensor-003").newTable();
        flight.deleteFromInputTable(scopeId, keysToDelete, allocator)
                .get(5, TimeUnit.SECONDS);

        System.out.println("Device status table updated.");
    }

    private static void updateDevice(
            FlightSession flight, ScopeId scopeId, BufferAllocator allocator,
            ColumnHeader<String> deviceIdCol, ColumnHeader<String> statusCol,
            ColumnHeader<Instant> lastSeenCol,
            String deviceId, String status) throws Exception {

        NewTable row = deviceIdCol.header(statusCol).header(lastSeenCol)
                .row(deviceId, status, Instant.now())
                .newTable();

        // Inserts or updates based on DeviceId
        flight.addToInputTable(scopeId, row, allocator).get(5, TimeUnit.SECONDS);
    }
}
```

## Batch uploads

The [complete example](#complete-example) sends one row per call to show how each update replaces the previous row. When you have several rows ready at once, such as the three initial devices there, send them in a single upload, which is faster. This snippet uses the column headers, `flight`, `allocator`, and `scopeId` from the [complete example](#complete-example):

```java
// Build multiple rows; start() takes the initial capacity (expected row count)
io.deephaven.qst.column.header.ColumnHeaders3<String, String, Instant>.Rows rows =
        deviceIdCol.header(statusCol).header(lastSeenCol).start(3);

rows.row("sensor-001", "online", Instant.now());
rows.row("sensor-002", "online", Instant.now());
rows.row("sensor-003", "offline", Instant.now());

NewTable batch = rows.newTable();

// Upload entire batch at once
flight.addToInputTable(scopeId, batch, allocator).get(5, TimeUnit.SECONDS);
```

## Delete rows from a keyed input table

To delete rows from a keyed input table, call [`FlightSession.deleteFromInputTable`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) with a table that contains every key column and no other columns. The server rejects a delete table that is missing a key column or includes any other column. This snippet uses the `flight`, `allocator`, and `scopeId` from the [complete example](#complete-example) and deletes two devices in one call:

```java
NewTable keysToDelete = ColumnHeader.ofString("DeviceId")
        .row("sensor-001")
        .row("sensor-002")
        .newTable();

flight.deleteFromInputTable(scopeId, keysToDelete, allocator)
        .get(5, TimeUnit.SECONDS);
```

## Resource management

### TableHandle lifecycle

A [`TableHandle`](/core/javadoc/io/deephaven/client/impl/TableHandle.html) refers to an export, which is a table the server keeps alive for your session until you release the handle. `TableHandle` implements `Closeable`, and closing it releases the export. A handle you never close keeps its export alive on the server until the session closes.

That is why steps 3-5 of the [basic pattern](#basic-pattern) publish the input table to the query scope and then close the handle. The query scope keeps the table alive, and later calls reach it through a [`ScopeId`](/core/javadoc/io/deephaven/client/impl/ScopeId.html).

### Data upload lifecycle

The [`FlightSession.addToInputTable`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) method performs these steps in a single call:

1. Uploads the [`NewTable`](/core/javadoc/io/deephaven/qst/table/NewTable.html) to the server as a temporary export that exists only for this call.
2. Calls the input table service to add the data.
3. Releases the temporary export when complete.

If you need more control, do the three steps yourself:

1. Upload the data with [`FlightSession.putExportManual`](/core/javadoc/io/deephaven/client/impl/FlightSession.html), which returns an [`ExportId`](/core/javadoc/io/deephaven/client/impl/ExportId.html).
2. Call [`Session.addToInputTable`](/core/javadoc/io/deephaven/client/impl/Session.html) with that `ExportId`.
3. Release the export with [`FlightSession.release`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) when done.

This snippet uses the column headers, `flight`, `allocator`, and `scopeId` from the [complete example](#complete-example):

```java
// Manual approach (use FlightSession.addToInputTable instead when possible)
// Requires: import io.deephaven.client.impl.ExportId;
NewTable row = deviceIdCol.header(statusCol).header(lastSeenCol)
        .row("sensor-001", "online", Instant.now())
        .newTable();

ExportId exportId = flight.putExportManual(row, allocator);
try {
    flight.session().addToInputTable(scopeId, exportId).get(5, TimeUnit.SECONDS);
} finally {
    flight.release(exportId);
}
```

## Working examples

The Deephaven repository includes complete working examples:

- [`AddToInputTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/tools/AddToInputTable.java) — Append-only input table with server-side validation. Requires a Python server, because it sets up the validator from the Python console.
- [`KeyValueInputTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/KeyValueInputTable.java) — Keyed input table with add and delete.
- [`AddToBlinkTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/AddToBlinkTable.java) — Blink input table.

## Related documentation

- [Create and use input tables](./input-tables.md)
- [Extract data with a Java client](./java-client.md)
- [Write data to an in-memory, real-time table](./table-publisher.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
