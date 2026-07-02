---
title: Send data to Deephaven from a Java client
sidebar_label: Java client input tables
---

This guide shows how to send data to Deephaven from an external Java application using the Deephaven Java client and input tables. Input tables allow a client to add, update, and delete rows in a Deephaven table.

> [!NOTE]
> If your data source can run directly on the Deephaven server, consider using [server-side input tables](./input-tables.md) or [`DynamicTableWriter`](./table-publisher.md) instead. Server-side ingestion is generally more efficient because it avoids network overhead.

## When to use client-side input tables

Use client-side input tables when:

- **Managing reference data**: Upload configuration, lookup tables, or static datasets that may need updates.
- **Tracking state**: Maintain the latest status per entity (e.g., device status, order state, user preferences).
- **Interactive editing**: Allow users or external systems to add, update, or remove records.
- **Forwarding external data**: Relay data from message queues, APIs, or sensors running in separate JVMs.

## Setup

Add the Deephaven Java client dependencies to your project. Use the version that matches your Deephaven server. For Gradle:

```groovy syntax
dependencies {
    implementation 'io.deephaven:deephaven-java-client-session:0.36.1'
    implementation 'io.deephaven:deephaven-java-client-flight:0.36.1'
    implementation 'io.deephaven:deephaven-qst:0.36.1'
}
```

## Basic pattern

The basic pattern for sending data from a Java client is:

1. **Create a `FlightSession`** to connect to the Deephaven server.
2. **Define an input table spec** using `InMemoryKeyBackedInputTable`, `InMemoryAppendOnlyInputTable`, or `BlinkInputTable`.
3. **Execute the spec** to create the input table on the server.
4. **Publish the input table** to the query scope so it's accessible and the server maintains a reference.
5. **Close the handle** to avoid leaking client-side resources (the server-side table persists via the scope).
6. **Build data rows** using `ColumnHeader` and `NewTable`.
7. **Add to the input table** via `ScopeId` with `FlightSession.addToInputTable`. For keyed tables, this inserts new rows or updates existing ones.

Repeat steps 6-7 as needed.

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
        // Connect to Deephaven server
        BufferAllocator allocator = new RootAllocator();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:10000")
                .usePlaintext()
                .build();

        FlightSession flight = DaggerDeephavenFlightRoot.create().factoryBuilder()
                .managedChannel(channel)
                .scheduler(scheduler)
                .allocator(allocator)
                .build()
                .newFlightSession();

        try {
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
                // Publish so it's visible in the UI (check http://localhost:10000)
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

            // Delete sensor-003
            NewTable keysToDelete = deviceIdCol.row("sensor-003").newTable();
            flight.deleteFromInputTable(scopeId, keysToDelete, allocator)
                    .get(5, TimeUnit.SECONDS);

            System.out.println("Device status table updated.");

        } finally {
            flight.close();
            channel.shutdownNow();
            scheduler.shutdownNow();
            allocator.close();
        }
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

## Input table types

The Java client supports three types of input tables:

### Keyed

Rows are identified by key columns. Adding a row with an existing key replaces that row. Deletion is supported.

```java
// Single key column
TableSpec spec = InMemoryKeyBackedInputTable.of(header, java.util.List.of("DeviceId"));

// Multiple key columns
TableSpec spec = InMemoryKeyBackedInputTable.of(header, java.util.List.of("DeviceId", "Region"));
```

With keyed tables, you can delete rows by providing just the key values:

```java
// Delete by key (using ScopeId after publishing and closing handle)
ScopeId scopeId = new ScopeId("device_status");
NewTable keysToDelete = ColumnHeader.ofString("DeviceId")
        .row("sensor-001")
        .row("sensor-003")
        .newTable();

flight.deleteFromInputTable(scopeId, keysToDelete, allocator)
        .get(5, TimeUnit.SECONDS);
```

### Append-only

Rows are added to the end of the table. No key columns, no updates, no deletion. Use this when you need a simple log or event stream.

```java
TableSpec spec = InMemoryAppendOnlyInputTable.of(header);
```

### Blink

Rows are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. Use this for event streams where you only need to process the latest batch.

```java
TableSpec spec = BlinkInputTable.of(header);
```

## Resource management

### TableHandle lifecycle

`TableHandle` represents a managed export and implements `Closeable`. Unclosed handles leak server-side resources for the lifetime of the client. The recommended pattern is:

1. Execute the spec to create the input table.
2. Publish it to the query scope (creates a server-side reference).
3. Close the handle immediately.
4. Send data via `ScopeId`.

This ensures the server-side table persists (via the scope) while avoiding client-side resource leaks.

### Data upload lifecycle

The `FlightSession.addToInputTable` method handles data upload automatically:

1. Uploads the `NewTable` to the server as a temporary export.
2. Calls the input table service to add the data.
3. Releases the temporary export when complete.

If you need more control, you can use the lower-level `Session` API:

```java
// Manual approach (use FlightSession.addToInputTable instead when possible)
ScopeId scopeId = new ScopeId("device_status");
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

## Batch uploads

For better performance, batch multiple rows into a single upload:

```java
// Build multiple rows
io.deephaven.qst.column.header.ColumnHeaders3<String, String, Instant>.Rows rows =
        deviceIdCol.header(statusCol).header(lastSeenCol).start(10);

rows.row("sensor-001", "online", Instant.now());
rows.row("sensor-002", "offline", Instant.now());
rows.row("sensor-003", "maintenance", Instant.now());

NewTable batch = rows.newTable();

// Upload entire batch at once (using ScopeId after publishing and closing handle)
ScopeId scopeId = new ScopeId("device_status");
flight.addToInputTable(scopeId, batch, allocator).get(5, TimeUnit.SECONDS);
```

## Working examples

The Deephaven repository includes complete working examples:

- [`AddToInputTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/AddToInputTable.java) - Append-only input table with validation
- [`KeyValueInputTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/KeyValueInputTable.java) - Keyed input table with add/delete
- [`AddToBlinkTable.java`](https://github.com/deephaven/deephaven-core/blob/main/java-client/flight-examples/src/main/java/io/deephaven/client/examples/AddToBlinkTable.java) - Blink input table

## Related documentation

- [Input tables (server-side)](./input-tables.md)
- [Extract data with a Java client](./java-client.md)
- [DynamicTableWriter](./table-publisher.md)
