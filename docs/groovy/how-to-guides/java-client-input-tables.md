---
title: Stream data from a Java client with input tables
sidebar_label: Java client input tables
---

This guide shows how to stream data into Deephaven from an external Java application using the Deephaven Java client and input tables. This is useful when your data source runs outside the Deephaven server — for example, a separate JVM collecting data from sensors, message queues, or external APIs.

> [!NOTE]
> If your data source can run directly on the Deephaven server, consider using [server-side input tables](./input-tables.md) or [`DynamicTableWriter`](./table-publisher.md) instead. Server-side ingestion is generally more efficient because it avoids network overhead.

## When to use client-side input tables

Use client-side input tables when:

- Your data source runs in a separate JVM or machine.
- You need to forward data from an external system (message queue, API, sensor) to Deephaven.
- You want to keep data collection logic separate from server-side analytics.

## Setup

Add the Deephaven Java client dependencies to your project. For Gradle:

```groovy syntax
dependencies {
    implementation 'io.deephaven:deephaven-java-client-session:0.37.0'
    implementation 'io.deephaven:deephaven-java-client-flight:0.37.0'
    implementation 'io.deephaven:deephaven-qst:0.37.0'
}
```

## The streaming pattern

The basic pattern for streaming data from a Java client is:

1. **Create a `FlightSession`** to connect to the Deephaven server.
2. **Define an input table spec** using `InMemoryAppendOnlyInputTable`, `InMemoryKeyBackedInputTable`, or `BlinkInputTable`.
3. **Execute the spec** to create the input table on the server.
4. **Build data rows** using `ColumnHeader` and `NewTable`.
5. **Add to the input table** with `FlightSession.addToInputTable` (handles upload and release automatically).

Repeat steps 4-5 to continuously stream data.

## Complete example

The following example streams timestamp data to an append-only input table:

```java
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class SensorDataStreamer {

    public static void main(String[] args) throws Exception {
        // Connect to Deephaven server
        BufferAllocator allocator = new RootAllocator();
        FlightSession flight = createFlightSession("localhost:10000");

        try {
            // Define schema with column headers
            ColumnHeader<Instant> timestampCol = ColumnHeader.ofInstant("Timestamp");
            ColumnHeader<String> sensorIdCol = ColumnHeader.ofString("SensorId");
            ColumnHeader<Double> temperatureCol = ColumnHeader.ofDouble("Temperature");

            TableHeader header = TableHeader.of(timestampCol, sensorIdCol, temperatureCol);

            // Create an append-only input table on the server
            TableSpec inputTableSpec = InMemoryAppendOnlyInputTable.of(header);
            TableHandle inputTableHandle = flight.session().execute(inputTableSpec);

            // Publish so it's visible in the UI (check http://localhost:10000)
            flight.session().publish("sensor_data", inputTableHandle).get(5, TimeUnit.SECONDS);

            System.out.println("Streaming sensor data. Press Ctrl+C to stop.");

            // Stream data
            while (true) {
                // Create a row of data
                NewTable newRow = timestampCol.header(sensorIdCol).header(temperatureCol)
                        .row(Instant.now(), "sensor_1", 20.0 + Math.random() * 5)
                        .newTable();

                // Add to input table (upload + add + release handled automatically)
                flight.addToInputTable(inputTableHandle, newRow, allocator)
                        .get(5, TimeUnit.SECONDS);

                Thread.sleep(1000);
            }
        } finally {
            flight.close();
            allocator.close();
        }
    }

    private static FlightSession createFlightSession(String target) {
        // Connection setup omitted for brevity
        // See java-client.md for full connection setup
        return null;
    }
}
```

## Input table types

The Java client supports three types of input tables:

### Append-only

Rows are added to the end of the table. No key columns, no deletion support.

```java
TableSpec spec = InMemoryAppendOnlyInputTable.of(header);
```

### Keyed

Rows are identified by key columns. Adding a row with an existing key replaces that row. Deletion is supported.

```java
// Single key column
TableSpec spec = InMemoryKeyBackedInputTable.of(header, List.of("SensorId"));

// Multiple key columns
TableSpec spec = InMemoryKeyBackedInputTable.of(header, List.of("SensorId", "Region"));
```

With keyed tables, you can delete rows by providing just the key values:

```java
// Delete by key
NewTable keysToDelete = ColumnHeader.ofString("SensorId")
        .row("sensor_1")
        .row("sensor_3")
        .newTable();

flight.deleteFromInputTable(inputTableHandle, keysToDelete, allocator)
        .get(5, TimeUnit.SECONDS);
```

### Blink

Rows are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. This is useful for event streams.

```java
TableSpec spec = BlinkInputTable.of(header);
```

## Resource management

The `FlightSession.addToInputTable` method handles resource management automatically:

1. Uploads the `NewTable` to the server as a temporary export.
2. Calls the input table service to add the data.
3. Releases the temporary export when complete.

If you need more control, you can use the lower-level `Session` API:

```java
// Manual approach (use FlightSession.addToInputTable instead when possible)
ExportId exportId = flight.putExportManual(newTable, allocator);
try {
    flight.session().addToInputTable(inputTableHandle, exportId).get();
} finally {
    flight.release(exportId);
}
```

## Batch uploads

For better performance, batch multiple rows into a single upload:

```java
// Build multiple rows
ColumnHeaders3<Instant, String, Double>.Rows rows = timestampCol.header(sensorIdCol).header(temperatureCol)
        .start(100);

for (int i = 0; i < 100; i++) {
    rows.row(Instant.now(), "sensor_" + (i % 5), 20.0 + Math.random() * 5);
}

NewTable batch = rows.newTable();

// Upload entire batch at once
flight.addToInputTable(inputTableHandle, batch, allocator).get(5, TimeUnit.SECONDS);
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
