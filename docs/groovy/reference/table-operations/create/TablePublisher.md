---
title: TablePublisher
---

The static [`TablePublisher.of`](/core/javadoc/io/deephaven/stream/TablePublisher.html#of(java.lang.String,io.deephaven.engine.table.TableDefinition,java.util.function.Consumer,java.lang.Runnable)) method creates a [`TablePublisher`](/core/javadoc/io/deephaven/stream/TablePublisher.html). A `TablePublisher` produces a [blink table](../../../conceptual/table-types.md#specialization-3-blink) from the tables passed to its [`add`](#methods) method.

## Syntax

```groovy syntax
TablePublisher.of(name, definition, onFlushCallback, onShutdownCallback)
TablePublisher.of(name, definition, onFlushCallback, onShutdownCallback, updateGraph, chunkSize)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the publisher, used for logging.

</Param>
<Param name="definition" type="TableDefinition">

The blink table's [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html).

</Param>
<Param name="onFlushCallback" type="Consumer<TablePublisher>">

The on-flush callback. If not `null`, the consumer runs once at the beginning of each [update graph cycle](../../../conceptual/dag.md#update-graph-cycles). It receives this `TablePublisher`, so it can call [`add`](#methods) to publish any data it has been batching. The consumer blocks the update cycle until it returns, so it should avoid extraneous work.

</Param>
<Param name="onShutdownCallback" type="Runnable">

The on-shutdown callback. If not `null`, the publisher runs it one time when the caller should stop adding new data and release any related resources — for example, because the caller called [`publishFailure`](#methods), the blink table is no longer reachable, or an internal failure occurred.

</Param>
<Param name="updateGraph" type="UpdateGraph" optional>

The [update graph](../../../conceptual/dag.md) for the blink table. The four-argument overload uses the update graph of the current [execution context](../../../conceptual/execution-context.md).

</Param>
<Param name="chunkSize" type="int" optional>

The maximum number of rows that `add` copies from the added table in each batch, or chunk. Must be positive. The four-argument overload uses 2048 rows.

</Param>
</ParamTable>

## Returns

A new `TablePublisher`. Call its [`table`](#methods) method to get the blink table, and keep a strong reference to the first table that `table` or [`inputTable`](#methods) returns, as the note after [Methods](#methods) explains.

## Methods

`TablePublisher` supports the following methods:

- [`add(table)`](/core/javadoc/io/deephaven/stream/TablePublisher.html#add(io.deephaven.engine.table.Table)) - Adds a snapshot of the data from `table` into the blink table. All of the data from one call is consistent with a single point in time, appears in a single update cycle of the blink table, and is not interleaved with data from any other call to `add`, concurrent or not.
  - `table` must contain every column in the blink table's definition, in any order.
  - `add` ignores columns in `table` that are not in the definition.
- [`definition`](/core/javadoc/io/deephaven/stream/TablePublisher.html#definition()) - Gets the blink table's definition.
- [`inputTable`](/core/javadoc/io/deephaven/stream/TablePublisher.html#inputTable()) - Returns the blink table with an [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html) attribute set, so it works as an [input table](../../../how-to-guides/input-tables.md) that supports adding rows but not deleting them.
- [`isAlive`](/core/javadoc/io/deephaven/stream/TablePublisher.html#isAlive()) - Returns `true` while the publisher is alive. Once it returns `false`, it always returns `false`, and the caller should stop adding data.
- [`publishFailure(failure)`](/core/javadoc/io/deephaven/stream/TablePublisher.html#publishFailure(java.lang.Throwable)) - Indicates that data publication has failed. The publisher notifies the blink table's listeners of the failure, the on-shutdown callback runs if it has not already run, `isAlive` returns `false`, and later calls to `add` return without publishing. These effects can take place asynchronously, on a later update cycle.
- [`table`](/core/javadoc/io/deephaven/stream/TablePublisher.html#table()) - Gets the blink table.

`table` and `inputTable` return different `Table` objects that share one underlying blink table, and the publisher keeps that blink table strongly reachable only until the first call to either method. Keep a strong reference to that first result, or later calls to `table` or `inputTable` may return `null`.

## Examples

This example uses `TablePublisher` to create a [blink table](../../../conceptual/table-types.md#specialization-3-blink) with three columns (`X`, `Y`, and `Z`). The columns are of type `int`, `double`, and `double`, respectively. It passes `null` as the on-flush callback and a `shutDown` closure as the on-shutdown callback. Assigning the result of `table` to `source` keeps the strong reference that the note after [Methods](#methods) requires.

```groovy test-set=1 order=source
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y"),
    ColumnDefinition.ofDouble("Z")
)

shutDown = { println "Finished using My Publisher." }

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

source = publisher.table()
```

To add data to the blink table, call [`add`](/core/javadoc/io/deephaven/stream/TablePublisher.html#add(io.deephaven.engine.table.Table)). Because `source` is a [blink table](../../../conceptual/table-types.md#specialization-3-blink), it shows only the rows added during the most recent [update cycle](../../../conceptual/dag.md#update-graph-cycles).

```groovy ticking-table test-set=1 order=null
publisher.add(emptyTable(10).update("X = randomInt(-100, 100)", "Y = randomDouble(-5.0, 5.0)", "Z = randomDouble(100.0, 1000.0)"))
```

![The `source` blink table after data has been added](../../../assets/reference/table-operations/table-publisher-1.png)

To stop the publisher, call [`publishFailure`](/core/javadoc/io/deephaven/stream/TablePublisher.html#publishFailure(java.lang.Throwable)). Calls to `add` after `publishFailure` return without publishing. On a later update cycle, the publisher notifies the blink table's listeners of the failure, so `source` shows an error, and it invokes the `shutDown` callback defined above.

```groovy test-set=1 order=null
publisher.publishFailure(new RuntimeException("Publisher shut down by user."))
```

## Related documentation

- [Write data to an in-memory, real-time table](../../../how-to-guides/table-publisher.md)
- [`DynamicTableWriter`](./DynamicTableWriter.md)
- [Execution Context](../../../conceptual/execution-context.md)
- [Blink tables](../../../conceptual/table-types.md#specialization-3-blink)
- [Directed acyclic graph (DAG)](../../../conceptual/dag.md)
- [Input tables](../../../how-to-guides/input-tables.md)
- [`emptyTable`](./emptyTable.md)
- [Javadoc](/core/javadoc/io/deephaven/stream/TablePublisher.html)
