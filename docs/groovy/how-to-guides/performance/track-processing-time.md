---
title: Track processing time and measure latency
sidebar_label: Track processing time
---

This guide explains how to track when Deephaven processes row modifications, which is useful for measuring end-to-end latency in real-time data pipelines.

## Problem: formulas skip re-evaluation

By default, Deephaven optimizes formula evaluation by only recomputing values when their input columns change. This is controlled by the [Modified Column Set](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ModifiedColumnSet.html) (MCS) — a bitset that tracks which columns were modified in each update cycle.

Consider this example where we want to record when a row is processed:

```groovy ticking-table order=null
// lastBy() collapses to a single row that gets modified on each tick
source = timeTable("PT1S").update("SourceTime = Timestamp", "Value = i").lastBy()

// This ProcessTime column will NOT update when Value changes
result = source.update("ProcessTime = now()")
```

The `ProcessTime` column evaluates `now()` once when the row is created, but it does not re-evaluate when `Value` changes on subsequent ticks. This is because `now()` has no column dependencies, so the engine skips re-evaluation as an optimization.

In real-world pipelines, this becomes important when you receive data from external sources (like Kafka) that include a source timestamp. You want to compare that source timestamp against when Deephaven actually processed the update — but a simple `now()` formula won't re-evaluate when the row is modified.

## Solution: force re-evaluation with `withRecomputeOnModifiedRow`

To force a formula to re-evaluate every time a row is modified (regardless of which columns changed), use [`SelectColumnFactory.getExpression`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)) combined with [`withRecomputeOnModifiedRow`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()):

```groovy syntax
import io.deephaven.engine.table.impl.select.SelectColumnFactory

selectColumn = SelectColumnFactory.getExpression("ColumnName = formula")
    .withRecomputeOnModifiedRow()

result = source.update(Arrays.asList(selectColumn))
```

This approach requires three steps:

1. **Create a [`SelectColumn`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html)** using `SelectColumnFactory.getExpression`. The standard string-based `update("formula")` API doesn't expose re-evaluation control, so we need to work with the underlying `SelectColumn` object.
2. **Wrap it** with `withRecomputeOnModifiedRow` to bypass the Modified Column Set optimization.
3. **Pass it as a Collection** using `Arrays.asList()`. The [`update`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#update(java.util.Collection)) method that accepts `SelectColumn` objects requires a Collection, not individual arguments.

## Example: track processing time

This example creates a `ProcessTime` column that updates every time the row is modified:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.select.SelectColumnFactory

// lastBy() collapses to a single row that gets modified on each tick
source = timeTable("PT1S").update("SourceTime = Timestamp", "Value = i").lastBy()

// Create a SelectColumn that always re-evaluates
processTimeCol = SelectColumnFactory.getExpression("ProcessTime = now()")
    .withRecomputeOnModifiedRow()

// Apply it using update() with a List
result = source.update(Arrays.asList(processTimeCol))
```

Now `ProcessTime` updates every time the row is modified, capturing when Deephaven processes each tick.

## Example: measure end-to-end latency

A common use case is measuring the latency between when data originates (e.g., a trade timestamp from an exchange) and when Deephaven processes it. This helps identify bottlenecks in your data pipeline:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.select.SelectColumnFactory

// Simulate incoming data with a source timestamp; lastBy() creates a single row that updates
source = timeTable("PT1S").update("SourceTime = Timestamp").lastBy()

// Track when Deephaven processes each modification
processTimeCol = SelectColumnFactory.getExpression("ProcessTime = now()")
    .withRecomputeOnModifiedRow()

result = source.update(Arrays.asList(processTimeCol))

// Calculate latency
result = result.update(
    "LatencyNanos = ProcessTime - SourceTime",
    "LatencyMs = nanosToMillis(LatencyNanos)"
)
```

The `LatencyNanos` column shows the time difference between the source timestamp and when Deephaven processed the row.

## Performance considerations

Using `withRecomputeOnModifiedRow` bypasses an important optimization. The Modified Column Set allows the engine to skip formula evaluation when inputs haven't changed, which can significantly reduce CPU usage for complex formulas.

Only use this feature when you specifically need to track processing time or implement similar functionality. For most use cases, the default behavior (evaluating formulas only when inputs change) is correct and more efficient.

## Related documentation

- [Formulas and threads](formula-threads.md)
- [Parallelization](../../conceptual/query-engine/parallelization.md)
- [Incremental update model](../../conceptual/table-update-model.md)
