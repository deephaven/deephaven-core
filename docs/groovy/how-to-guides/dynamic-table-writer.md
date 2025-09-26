---
title: Write data to an in-memory, real-time table
sidebar_label: Write data to a real-time table
---

This guide covers publishing data to in-memory ticking tables with two classes:

- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)

A Table Publisher publishes data to a [blink table](../conceptual/table-types.md#specialization-3-blink), while a Dynamic Table Writer writes data to an append-only table. Both methods are great ways to ingest and publish data from external sources such as WebSockets and other live data sources. However, we recommend [`TablePublisher`](../reference/table-operations/create/TablePublisher.md) in most cases, as it provides a newer and more refined API, as well as native support for blink tables.

## Table publisher

A table publisher uses the [`TablePublisher.of`](../reference/table-operations/create/TablePublisher.md#methods) method to create an instance of the [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher). Then, call the `TablePublisher.table()` method to return the table publisher's linked [blink table](../conceptual/table-types.md#specialization-3-blink). You can also:

- Add data to the [blink table](../conceptual/table-types.md#specialization-3-blink) with [`add`](../reference/table-operations/create/TablePublisher.md#methods).
- (Optionally) Store [data history](#data-history) in a downstream table.
- (Optionally) Shut the publisher down when finished.

More sophisticated use cases will add steps but follow the same basic formula.

### Construct the Table Publisher and its associated blink table

The [`TablePublisher.of`](../reference/table-operations/create/TablePublisher.md) function returns a [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher). The following code block creates a table publisher named `My publisher` that publishes to a [blink table](../conceptual/table-types.md#specialization-3-blink) with two columns, `X` and `Y`, which are `int` and `double` data types, respectively.

```groovy order=source reset
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y"),
)

shutDown = {println "Finished using My Publisher."}

onShutdown = new MutableBoolean()

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

source = publisher.table()
```

Note that since we have not called `add` yet, the `source` table is empty.

### Example: Getting started

The following example creates a table with three columns (`X`, `Y`, and `Z`). The columns initially contain no data because `addTable` has not yet been called.

```groovy test-set=1 order=publishedTable
import io.deephaven.csv.util.MutableBoolean
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

Add data to the blink table by calling the [`add`](../reference/table-operations/create/TablePublisher.md#methods) method.

```groovy test-set=1 order=publishedTable
publisher.add(emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)", "Z = randomDouble(0.0, 100.0)"))
```

The `TablePublisher` can be shut down by calling [`publishFailure`](../reference/table-operations/create/TablePublisher.md#methods).

```groovy test-set=1 order=null
publisher.publishFailure(new RuntimeException("Publisher shut down by user."))
```

### Example: threading

The following example adds new data to the publisher with [`emptyTable`](./new-and-empty-table.md#emptytable) every second for 5 seconds in a separate thread. Note how the current [execution context](../conceptual/execution-context.md) is captured and used to add data to the publisher. Attempting to perform table operations in a separate thread without specifying an [execution context](../conceptual/execution-context.md) will raise an exception.

> [!IMPORTANT]
> A ticking table in a thread must be updated from within an [execution context](../conceptual/execution-context.md).

```groovy ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Finished."}

onShutdown = new MutableBoolean()

myPublisher = TablePublisher.of("My Publisher", definition, null, shutDown)

myTable = myPublisher.table()

defaultCtx = ExecutionContext.getContext()

myFunc = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        Random rand = new Random()
        for (int i = 0; i < 5; ++i) {
            nRows = rand.nextInt(5) + 5
            myPublisher.add(emptyTable(nRows).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)"))
            sleep(1000)
        }
        return
    }
}

thread = new Thread(myFunc).start()
```

![img](../assets/how-to/publisher-threaded.gif)

## Data history

Table publishers create blink tables. Blink tables do not store any data history - data is gone forever at the start of a new update cycle. In most use cases, you will want to store some or all of the rows written during previous update cycles. There are two ways to do this:

- Store some data history by creating a downstream ring table with [`RingTableTools.of`](../reference/cheat-sheets/simple-table-constructors.md#ringtabletoolsof).
- Store all data history by creating a downstream append-only table with [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md).

See the [table types user guide](../conceptual/table-types.md) for more information on these table types, including which one is best suited for your application.

To show the storage of data history, we will extend the [threading example](#example-threading) by creating a downstream ring table and append-only table.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import io.deephaven.engine.table.impl.BlinkTableTools
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

definition = TableDefinition.of(
    ColumnDefinition.ofInt('X'),
    ColumnDefinition.ofDouble('Y')
)

shutDown = { -> println 'Finished.'}

onShutdown = new MutableBoolean()

myPublisher = TablePublisher.of('My Publisher', definition, null, shutDown)

myTable = myPublisher.table()

defaultCtx = ExecutionContext.getContext()

myFunc = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        Random rand = new Random()
        for (int i = 0; i < 5; ++i) {
            nRows = rand.nextInt(5) + 5
            myPublisher.add(emptyTable(nRows).update('X = randomInt(0, 10)', 'Y = randomDouble(0.0, 100.0)'))
            sleep(1000)
        }
        return
    }
}

thread = new Thread(myFunc).start()

myRingTable = RingTableTools.of(myTable, 15, true)
myAppendOnlyTable = BlinkTableTools.blinkToAppendOnly(myTable)
```

![img](../assets/how-to/pub-data-history.gif)

## DynamicTableWriter

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data into live, in-memory tables by specifying the name and data types of each column. The use of [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) to write data to an in-memory ticking table generally follows a formula:

- Create the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md).
- Get the table that [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) will write data to.
- Write data to the table (done in a separate thread).
- Close the table writer.

> [!IMPORTANT]
> In most cases, a [table publisher](#table-publisher) is the preferred way to write data to a live table. However, it may be more convenient to use [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) if you are adding very few rows (i.e., one) at a time and you prefer a simple interface. It is almost always more flexible and performant to use [`TablePublisher`](../reference/table-operations/create/TablePublisher.md).

### Example: Getting started

The following example creates a table with two columns (`A` and `B`). The columns contain randomly generated integers and strings, respectively. Every second, for ten seconds, a new row is added to the table.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.impl.util.DynamicTableWriter

chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray()

// Create a DynamicTableWriter with two columns: `A` (int) and `B` (String)
columnNames = ["A", "B"] as String[]
columnTypes = [int.class, char.class] as Class[]
tableWriter = new DynamicTableWriter(columnNames, columnTypes)

result = tableWriter.getTable()

def rng = new Random()

// Thread to log data to the dynamic table
def thread = Thread.start {
    // for loop that defines how much data to populate to the table
    for (int i = 0; i < 10; i++) {
        // the data to put into the table
        a = rng.nextInt(100)
        b = chars[rng.nextInt(62)]

        // The logRow method adds a row to the table
        tableWriter.logRow(a, b)

        // milliseconds between new rows inserted into the table
        sleep(1000)
    }

    return
}
```

<LoopedVideo src='../assets/how-to/DynamicTableWriter_Video1.mp4' />

### Example: Trig Functions

The following example writes rows containing `X`, `sin(X)`, `cos(X)`, and `tan(X)` and plots the functions as the table updates.

```groovy order=null ticking-table reset
import io.deephaven.engine.table.impl.util.DynamicTableWriter
import io.deephaven.plot.FigureFactory

import static java.lang.Math.*

// Define column names and types using primitive classes
columnNames = ["X", "SinX", "CosX", "TanX"] as String[]
columnTypes = [double.class, double.class, double.class, double.class] as Class[]

// Create DynamicTableWriter
tableWriter = new DynamicTableWriter(columnNames, columnTypes)
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
        sleep(200 - elapsed)
    }
}

// Create the plot
trig_plot = FigureFactory.figure()
    .plot("Sin(X)", trigFunctions, "X", "SinX")
    .plot("Cos(X)", trigFunctions, "X", "CosX")
    .plot("Tan(X)", trigFunctions, "X", "TanX")
    .chartTitle("Trig Functions")
    .show()
```

<LoopedVideo src='../assets/how-to/dtwTrigFunctions.mp4' />

<LoopedVideo src='../assets/how-to/dtwTrigFunctionsPlot.mp4' />

### DynamicTableWriter and the Update Graph

Both the Python interpreter and the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) require the [Update Graph (UG) lock](../conceptual/query-engine/engine-locking.md#query-engine-locks) to execute. As a result, new rows will not appear in output tables until the next UG cycle. As an example, what would you expect the `print` statement below to produce?

```groovy order=result test-set=1 reset
import io.deephaven.engine.table.impl.util.DynamicTableWriter

columnNames = ["Numbers", "Words"] as String[]
columnTypes = [int.class, String.class] as Class[]
tableWriter = new DynamicTableWriter(columnNames, columnTypes)

result = tableWriter.getTable()

tableWriter.logRow(1, "Testing")
sleep(3000)
tableWriter.logRow(2, "Dynamic")
sleep(3000)
tableWriter.logRow(3, "Table")
sleep(3000)
tableWriter.logRow(4, "Writer")

println result.isEmpty()
```

You may be surprised, but the table does not contain rows when the `print` statement is reached. The Python interpreter holds the UG lock while the code block executes, preventing `result` from being updated with the new rows until the next UG cycle. Because `print` is in the code block, it sees the table before rows are added.

However, calling the same `print` statement as a second command produces the expected result.

```groovy test-set=1
println result.isEmpty()
```

All table updates emanate from the [Periodic Update Graph](/core/docs/conceptual/periodic-update-graph-configuration) <!--TODO: update with Groovy link when https://deephaven.atlassian.net/jira/software/projects/DOC/boards/8?selectedIssue=DOC-805 is complete-->. An understanding of how the Update Graph works can greatly improve query writing.

## Related documentation

- [Create new tables](./new-and-empty-table.md#newtable)
- [Deephaven data types](./data-types.md)
- [Deephaven's table update model](../conceptual/table-update-model.md)
- [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md)
- [Execution Context](../conceptual/execution-context.md)
- [How to use Java packages in query strings](./install-and-use-java-packages.md#use-java-packages-in-query-strings)
- [Query Scope](./queryscope.md)
- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html)
- [`TablePublisher` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html)
