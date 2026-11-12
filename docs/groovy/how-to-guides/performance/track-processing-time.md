---
title: Track processing time and measure latency
---

This guide explains how to track when Deephaven processes row modifications, which is useful for measuring end-to-end latency in real-time data pipelines.

## Problem: formulas skip re-evaluation

When a row is modified, [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md) recompute a [formula](../formulas.md) column only if one of the formula's input columns changed. They learn which columns changed from the update's [modified column set](../../conceptual/table-update-model.md#modified-columns), a bitset that records which columns were modified in each update cycle.

Consider this example where we want to record when Deephaven processes a row. The [time table](../time-table.md) ticks every second, adding a new row each time, and [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) keeps a single row that changes on each tick:

```groovy ticking-table order=null
// lastBy() collapses to a single row that gets modified on each tick
source = timeTable("PT1S").update("SourceTime = Timestamp", "Value = i").lastBy()

// ProcessTime doesn't update when Value changes
result = source.update("ProcessTime = now()")
```

Open `result` in the IDE: `SourceTime` and `Value` change every second, but `ProcessTime` keeps the value it got when the row was created.

A formula like `ProcessTime = now()`, which calls the built-in [`now`](../../reference/time/datetime/now.md) function, has no column dependencies. It evaluates once when the row is created. It doesn't re-evaluate when `Value` changes on later ticks, because none of its input columns changed.

In real-world pipelines, this matters when you receive data from external sources, like [Kafka](../overview-kafka.md), that include a source timestamp. You want to compare that timestamp against the time Deephaven processed the update. A plain `ProcessTime = now()` formula can't give you that, because it doesn't re-evaluate when the row is modified.

## Solution: force re-evaluation with `withRecomputeOnModifiedRow`

To force a formula to re-evaluate every time a row is modified, use [`SelectColumnFactory.getExpression`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)) with [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()):

```groovy ticking-table order=null test-set=1
import io.deephaven.engine.table.impl.select.SelectColumnFactory

// lastBy() collapses to a single row that gets modified on each tick
source = timeTable("PT1S").update("SourceTime = Timestamp", "Value = i").lastBy()

// Create a SelectColumn that always re-evaluates
processTimeCol = SelectColumnFactory.getExpression("ProcessTime = now()")
    .withRecomputeOnModifiedRow()

// Apply it using update() with a List
result = source.update(Arrays.asList(processTimeCol))
```

With [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) applied, `ProcessTime` updates on every tick that modifies the row, capturing when Deephaven processes each tick.

This approach requires three steps:

1. **Create a [`SelectColumn`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html)** using [`SelectColumnFactory.getExpression`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)). A `SelectColumn` is the object form of a column formula. Unlike a [string formula](../formulas.md) passed to [`update`](../../reference/table-operations/select/update.md), it can be marked to re-evaluate on every row modification.
2. **Wrap it** with [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) to bypass the [modified column set](../../conceptual/table-update-model.md#modified-columns) optimization.
3. **Pass it as a `Collection`** using `Arrays.asList`. The [`update`](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.util.Collection)) method that accepts [`SelectColumn`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html) objects requires a `Collection`, not individual arguments.

A column marked with [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) works only with [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md). [`view`](../../reference/table-operations/select/view.md), [`updateView`](../../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) reject such a column with an `IllegalArgumentException`.

## Example: measure end-to-end latency

Because `ProcessTime` updates on every tick, you can compare it against a source timestamp, such as an exchange's trade timestamp, to measure end-to-end latency. This helps identify bottlenecks in your data pipeline. In this example, the time table's `Timestamp` column stands in for a source timestamp. The result shows how long after each scheduled tick Deephaven processed it. With real data, use the timestamp your source provides, such as the `KafkaTimestamp` column that Kafka ingestion adds.

Using `result` from the [solution above](#solution-force-re-evaluation-with-withrecomputeonmodifiedrow), subtract `SourceTime` from `ProcessTime` to calculate the latency:

```groovy ticking-table order=null test-set=1
// Calculate latency
result = result.update(
    "LatencyNanos = ProcessTime - SourceTime",
    "LatencyMs = nanosToMillis(LatencyNanos)"
)
```

`LatencyNanos` is the time, in nanoseconds, between the source timestamp and the moment Deephaven processed the row. `LatencyMs` converts it to whole milliseconds with the built-in [`nanosToMillis`](../../reference/time/datetime/nanosToMillis.md) function. The conversion truncates toward zero, so a latency under one millisecond shows as `0`.

## Performance considerations

Using [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) bypasses an important optimization. The [modified column set](../../conceptual/table-update-model.md#modified-columns) allows the engine to skip formula evaluation when inputs haven't changed, which can significantly reduce CPU usage for complex formulas.

Use this feature only when a formula must re-evaluate on every row modification even though its input columns didn't change, such as recording processing time. For most use cases, the default behavior of evaluating formulas only when their inputs change is correct and more efficient.

## Related documentation

- [Incremental update model](../../conceptual/table-update-model.md)
- [Create a time table](../time-table.md)
- [Formulas](../formulas.md)
- [Kafka overview](../overview-kafka.md)
- [Formulas and threads](formula-threads.md)
- [Parallelization](../../conceptual/query-engine/parallelization.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`nanosToMillis`](../../reference/time/datetime/nanosToMillis.md)
- [`now`](../../reference/time/datetime/now.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md)
- [`SelectColumn` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html)
- [`SelectColumnFactory` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html)
- [`TableOperations` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html)
