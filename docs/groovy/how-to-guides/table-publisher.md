---
title: Write data to an in-memory, real-time table
---

This guide covers two ways to publish data to in-memory [ticking tables](../conceptual/table-update-model.md):

- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)

[`TablePublisher`](../reference/table-operations/create/TablePublisher.md) publishes data to a [blink table](../conceptual/table-types.md#specialization-3-blink), while [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data to an [append-only table](../conceptual/table-types.md#specialization-1-append-only). Both can ingest data from external live sources, such as WebSockets. We recommend `TablePublisher` in most cases: it has a newer, more flexible, and more performant API, and it supports blink tables natively. `DynamicTableWriter` can be more convenient when you add very few rows (for example, one) at a time and want a simple interface.

## Table publisher

Create a [`TablePublisher`](/core/javadoc/io/deephaven/stream/TablePublisher.html) with [`TablePublisher.of`](../reference/table-operations/create/TablePublisher.md#syntax), then call its [`table`](../reference/table-operations/create/TablePublisher.md#methods) method to get the linked [blink table](../conceptual/table-types.md#specialization-3-blink). From there:

- Add data to the [blink table](../conceptual/table-types.md#specialization-3-blink) with [`add`](../reference/table-operations/create/TablePublisher.md#methods).
- (Optionally) Store [data history](#data-history) in a downstream table.
- (Optionally) Shut the publisher down when finished.

More sophisticated use cases add steps but follow the same basic formula.

### Example: getting started with a table publisher

This example uses the four-argument form of [`TablePublisher.of`](../reference/table-operations/create/TablePublisher.md#syntax), which takes:

- The publisher's name.
- A [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) for the blink table's columns.
- An optional on-flush callback that runs at the start of each [update cycle](../conceptual/table-update-model.md).
- An optional on-shutdown callback that runs once when the publisher shuts down.

This example passes `null` for the on-flush callback because it needs none.

The following code creates a blink table with three columns (`X`, `Y`, and `Z`). The columns contain no data until you call [`add`](../reference/table-operations/create/TablePublisher.md#methods).

```groovy test-set=1 order=publishedTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y"),
    ColumnDefinition.ofDouble("Z")
)

shutDown = {println "Table publisher is shut down"}

publisher = TablePublisher.of("Table publisher", definition, null, shutDown)

publishedTable = publisher.table()
```

Add data to the blink table by calling the [`add`](../reference/table-operations/create/TablePublisher.md#methods) method. The rows appear on the next [update cycle](../conceptual/table-update-model.md) after the command that adds them finishes. For details, see [Data appears on the next update cycle](#data-appears-on-the-next-update-cycle).

```groovy ticking-table test-set=1 order=null
publisher.add(emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)", "Z = randomDouble(0.0, 100.0)"))
```

![The `publishedTable` blink table after data has been added](../assets/how-to/table-publisher-getting-started.png)

To stop the [`TablePublisher`](/core/javadoc/io/deephaven/stream/TablePublisher.html), call [`publishFailure`](../reference/table-operations/create/TablePublisher.md#methods). This notifies the blink table's listeners of the failure, so downstream tables show an error. It also invokes the on-shutdown callback, and later calls to [`add`](../reference/table-operations/create/TablePublisher.md#methods) return without publishing.

```groovy test-set=1 order=null
publisher.publishFailure(new RuntimeException("Publisher shut down by user."))
```

### Example: threading

The [getting started example](#example-getting-started-with-a-table-publisher) adds data to its blink table only when you call [`add`](../reference/table-operations/create/TablePublisher.md#methods) by hand. In most real-world use cases, you want to add data automatically at a regular interval. The following example adds between 5 and 10 rows of new data to the publisher with [`emptyTable`](../reference/table-operations/create/emptyTable.md) every second for 5 seconds in a separate thread.

> [!IMPORTANT]
> Table operations that run in a separate thread and compile query strings, such as [`update`](../reference/table-operations/select/update.md), need an [execution context](../conceptual/execution-context.md). Without one, they raise an exception. Capture the current context with [`ExecutionContext.getContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) and open it in the thread with its `open` method in a try-with-resources block.

```groovy ticking-table order=null reset test-set=2
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Finished."}

myPublisher = TablePublisher.of("My Publisher", definition, null, shutDown)

myTable = myPublisher.table()

defaultCtx = ExecutionContext.getContext()

myFunc = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        Random rand = new Random()
        for (int i = 0; i < 5; ++i) {
            int nRows = rand.nextInt(6) + 5
            myPublisher.add(emptyTable(nRows).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)"))
            sleep(1000)
        }
        return
    }
}

thread = new Thread(myFunc)
thread.start()
```

![The above table](../assets/how-to/publisher-threaded.gif)

### Data history

Table publishers create blink tables. Deephaven processes new data in periodic [update cycles](../conceptual/table-update-model.md). A blink table keeps only the rows added in the current cycle and drops them when the next cycle starts, so it stores no data history. In most use cases, you want to store some or all of the rows written during previous update cycles. There are two common ways to do this:

- Store some data history by creating a downstream [ring table](../conceptual/table-types.md#specialization-4-ring) with [`RingTableTools.of`](../reference/table-operations/create/ringTable.md).
- Store all data history by creating a downstream [append-only table](../conceptual/table-types.md#specialization-1-append-only) with [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md).

See the [table types user guide](../conceptual/table-types.md) for more information on these table types, including which one is best suited for your application.

The following code block builds on the [threading example](#example-threading). It reuses that example's `myTable` and `myFunc`, creates a downstream ring table and append-only table from `myTable`, and then starts a new thread that adds more data to `myTable`.

```groovy ticking-table order=null test-set=2
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import io.deephaven.engine.table.impl.BlinkTableTools

// Downstream ring table that stores the most recent 15 rows
myRingTable = RingTableTools.of(myTable, 15, true)

// Downstream append-only table
myAppendOnlyTable = BlinkTableTools.blinkToAppendOnly(myTable)

// Add more data to myTable in a new thread
thread = new Thread(myFunc)
thread.start()
```

![The above `myTable`, `myRingTable`, and `myAppendOnlyTable` tables](../assets/how-to/pub-data-history.gif)

## `DynamicTableWriter`

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data into a live, in-memory table whose column names and data types you define. To use it:

- Create the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md).
- Get the table that the writer writes to.
- Write data to the table, typically from a separate thread so that rows appear while writing continues (see [Data appears on the next update cycle](#data-appears-on-the-next-update-cycle)).
- Close the writer.

### Example: getting started with `DynamicTableWriter`

The following example creates a table with two columns (`A` and `B`). The columns contain randomly generated integers and characters, respectively. A separate thread adds a new row every second for ten seconds. When the thread finishes writing, it closes the writer with [`close`](../reference/table-operations/create/DynamicTableWriter.md#methods).

```groovy ticking-table order=null reset
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter

chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray()

// Create a DynamicTableWriter with two columns: `A` (int) and `B` (char)
tableWriter = new DynamicTableWriter(
    TableDefinition.of(
        ColumnDefinition.ofInt("A"),
        ColumnDefinition.ofChar("B")
    )
)

result = tableWriter.getTable()

def rng = new Random()

// Thread to log data to the dynamic table
def thread = Thread.start {
    // for loop that defines how much data to populate to the table
    for (int i = 0; i < 10; i++) {
        // the data to put into the table
        int a = rng.nextInt(100)
        char b = chars[rng.nextInt(62)]

        // logRow queues a row; it appears on the next update cycle
        tableWriter.logRow(a, b)

        // milliseconds between new rows inserted into the table
        sleep(1000)
    }

    // Close the writer once all rows are written
    tableWriter.close()

    return
}
```

<LoopedVideo src='../assets/how-to/DynamicTableWriter_Video1.mp4' />

### Example: trig functions

The following example writes rows with an `X` column and its sine, cosine, and tangent in the `SinX`, `CosX`, and `TanX` columns, and plots the sine and cosine as the table updates.

```groovy order=null ticking-table reset
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter
import io.deephaven.plot.FigureFactory

import static java.lang.Math.*

// Define the columns
definition = TableDefinition.of(
    ColumnDefinition.ofDouble("X"),
    ColumnDefinition.ofDouble("SinX"),
    ColumnDefinition.ofDouble("CosX"),
    ColumnDefinition.ofDouble("TanX")
)

// Create DynamicTableWriter
tableWriter = new DynamicTableWriter(definition)
trigFunctions = tableWriter.getTable()

// Start data writing thread
Thread.start {
    for (int i = 0; i < 628; i++) {
        long start = System.currentTimeMillis()
        double x = 0.01 * i
        double sinX = sin(x)
        double cosX = cos(x)
        double tanX = tan(x)
        tableWriter.logRow(x, sinX, cosX, tanX)
        long elapsed = System.currentTimeMillis() - start
        sleep(Math.max(0L, 200 - elapsed))
    }

    tableWriter.close()
}

// Create the plot
trigPlot = FigureFactory.figure()
    .plot("Sin(X)", trigFunctions, "X", "SinX")
    .plot("Cos(X)", trigFunctions, "X", "CosX")
    .chartTitle("Trig Functions")
    .show()
```

<LoopedVideo src='../assets/how-to/dtwTrigFunctions.mp4' />

<LoopedVideo src='../assets/how-to/dtwTrigFunctionsPlot.mp4' />

## Data appears on the next update cycle

Neither a table publisher nor a [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) adds rows to its table immediately. A table publisher's [`add`](../reference/table-operations/create/TablePublisher.md#methods) and a writer's [`logRow`](../reference/table-operations/create/DynamicTableWriter.md#methods) both queue the new rows. The queued rows reach the table during the next cycle of the [Update Graph (UG)](../conceptual/dag.md#update-graph-ug-cycles). The UG is the engine component that processes table updates. Rows added from a table publisher's on-flush callback, which runs at the start of a cycle, appear in that same cycle.

The Groovy script session holds the exclusive [UG lock](../conceptual/query-engine/engine-locking.md#query-engine-locks) while a command executes, so that cycle cannot run until the command finishes. As a result, new rows do not appear in output tables while the command that wrote them is still running.

The following example shows this with a [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md), whose append-only table keeps the rows so you can inspect them afterward. What would you expect the `println` statement below to produce?

```groovy ticking-table order=:log test-set=3 reset
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter

tableWriter = new DynamicTableWriter(
    TableDefinition.of(
        ColumnDefinition.ofInt("Numbers"),
        ColumnDefinition.ofString("Words")
    )
)

result = tableWriter.getTable()

tableWriter.logRow(1, "Testing")
tableWriter.logRow(2, "Dynamic")
tableWriter.logRow(3, "Table")
tableWriter.logRow(4, "Writer")

println result.isEmpty()
```

The `println` statement prints `true`: it runs in the same command as the `logRow` calls, so the UG cycle that adds the queued rows to `result` has not run yet.

Run the same `println` statement as a second command, and it prints `false`.

```groovy test-set=3 order=:log
println result.isEmpty()
```

In a standard Deephaven server, the [Periodic Update Graph](../conceptual/periodic-update-graph-configuration.md) drives these update cycles. To learn how update cycles propagate changes through your queries, see [Deephaven's table update model](../conceptual/table-update-model.md).

## Related documentation

- [Create static tables](./new-and-empty-table.md#emptytable)
- [Data types in Deephaven](./data-types.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [Execution Context](../conceptual/execution-context.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html)
- [`TablePublisher` Javadoc](/core/javadoc/io/deephaven/stream/TablePublisher.html)
