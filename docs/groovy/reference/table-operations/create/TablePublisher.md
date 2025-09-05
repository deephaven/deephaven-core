---
title: TablePublisher
---

The `TablePublisher` class creates a [blink table](../../../conceptual/table-types.md#specialization-3-blink) from tables that are added to it via the [`add`](#methods) method.

## Syntax

```groovy syntax
TablePublisher.of(name, definition, onFlushCallback, onShutdownCallback)
TablePublisher.of(name, definition, onFlushCallback, onShutdownCallback, updateGraph, chunkSize)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the table publisher.

</Param>
<Param name="definition" type="TableDefinition">

The table definition.

</Param>
<Param name="onFlushCallback" type="Consumer">

The on-flush callback. If not `null`, the consumer is called once at the beginning of each update cycle. It allows publishers to add any data that they may have been batching. It blocks the update cycle from proceeding, so implementations should take care not to do extraneous work.

</Param><Param name="onShutdownCallback" type="Runnable">

The on-shutdown callback. If not `null`, the runnable is called one time when [`publishFailure`](#methods) is called.

</Param>
<Param name="updateGraph" type="UpdateGraph">

The update graph for the blink table.

</Param>
<Param name="chunkSize" type="int">

The maximum chunk size. The default value is 2048.

</Param>
</ParamTable>

## Returns

A new `TablePublisher`.

## Methods

`TablePublisher` supports the following methods:

- [`add(table)`](https://deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html#add(io.deephaven.engine.table.Table)) - Adds a table to the blink table.
- [`definition()`](https://deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html#definition()) - Gets the table definition.
- [`isAlive()`](https://deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html#isAlive()) - Checks if the table publisher is alive.
- [`publishFailure(failure)`](https://deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html#publishFailure(java.lang.Throwable)) - Indicate that data publication has hailed. Listeners will be notified, the on-shutdown callback will be invoked (if it hasn't already been), and future calls to `add` will return without doing anything.
- [`table()`](https://deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html#table()) - Gets the blink table.

## Examples

In this example, `TablePublisher` is used to create a blink table with three columns (`X`, `Y`, and `Z`). The columns are of type `int`, `double`, and `double`, respectively.

```groovy test-set=1 order=source
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y"),
    ColumnDefinition.ofDouble("Z")
)

shutDown = {println "Finished using My Publisher."}

onShutdown = new MutableBoolean()

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

source = publisher.table()
```

To add data to blink table, call `add`.

```groovy test-set=1 order=source
publisher.add(emptyTable(10).update("X = randomInt(-100, 100)", "Y = randomDouble(-5.0, 5.0)", "Z = randomDouble(100.0, 1000.0)"))
```

To shut the publisher down, call `publishFailure`.

```groovy test-set=1 order=null
publisher.publishFailure(new RuntimeException("Publisher shut down by user."))
```

## Related documentation

- [How to use TablePublisher](../../../how-to-guides/dynamic-table-writer.md#table-publisher)
- [`emptyTable`](./emptyTable.md)
- [Javadoc](/core/javadoc/io/deephaven/stream/TablePublisher.html)
