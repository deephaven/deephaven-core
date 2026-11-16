---
title: Track processing time and measure latency
---

This guide explains how to track when Deephaven processes row modifications, which is useful for measuring end-to-end latency in real-time data pipelines.

## Problem: formulas skip re-evaluation

When a row is modified, [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md) recompute a [formula](../formulas.md) column only if one of the formula's input columns changed. They learn which columns changed from the update's [modified column set](../../conceptual/table-update-model.md#modified-columns), a bitset that records which columns were modified in each update cycle.

Consider this example where we want to record when Deephaven processes a row. The [time table](../time-table.md) ticks every second, adding a new row each time, and [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) keeps a single row that changes on each tick:

```python ticking-table order=null
from deephaven import time_table

# last_by() collapses to a single row that gets modified on each tick
source = time_table("PT1S").update(["SourceTime = Timestamp", "Value = i"]).last_by()

# ProcessTime doesn't update when Value changes
result = source.update("ProcessTime = now()")
```

Open `result` in the IDE: `SourceTime` and `Value` change every second, but `ProcessTime` keeps the value it got when the row was created.

A formula like `ProcessTime = now()`, which calls the built-in [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) function, has no column dependencies. It evaluates once when the row is created. It doesn't re-evaluate when `Value` changes on later ticks, because none of its input columns changed.

In real-world pipelines, this matters when you receive data from external sources, like [Kafka](../overview-kafka.md), that include a source timestamp. You want to compare that timestamp against the time Deephaven processed the update. A plain `ProcessTime = now()` formula can't give you that, because it doesn't re-evaluate when the row is modified.

## Solution: force re-evaluation with `withRecomputeOnModifiedRow`

To force a formula to re-evaluate every time a row is modified, create the column with [`SelectColumnFactory.getExpression`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)) and mark it with [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()). The Python API has no wrapper for these Java methods, so use [jpy](../use-jpy.md) to call them directly:

```python ticking-table order=null test-set=1
import jpy
from deephaven import time_table
from deephaven.jcompat import j_array_list
from deephaven.table import Table

SelectColumnFactory = jpy.get_type(
    "io.deephaven.engine.table.impl.select.SelectColumnFactory"
)

# last_by() collapses to a single row that gets modified on each tick
source = time_table("PT1S").update(["SourceTime = Timestamp", "Value = i"]).last_by()

# Force now() to re-evaluate on every modification
process_time_col = SelectColumnFactory.getExpression(
    "ProcessTime = now()"
).withRecomputeOnModifiedRow()

# Call j_table.update directly since the Python wrapper doesn't recognize jpy types
result = Table(j_table=source.j_table.update(j_array_list([process_time_col])))
```

With [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) applied, `ProcessTime` updates on every tick that modifies the row, capturing when Deephaven processes each tick.

This approach requires three steps:

1. **Get the Java class.** Use [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) to load [`SelectColumnFactory`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html).
2. **Create and mark the column.** [`getExpression`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)) returns a Java [`SelectColumn`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html), the object form of a column formula. [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) marks it to re-evaluate on every row modification.
3. **Call the Java `update` method on `source.j_table`.** The Python [`update`](../../reference/table-operations/select/update.md) method doesn't recognize a `SelectColumn` created through jpy. The Java [`update`](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.util.Collection)) takes a `Collection` of [`Selectable`](/core/javadoc/io/deephaven/api/Selectable.html) objects, and a `SelectColumn` is a `Selectable`. Two helpers handle the conversion:
   - [`j_array_list`](/core/pydoc/code/deephaven.jcompat.html#deephaven.jcompat.j_array_list) wraps the column in a Java list.
   - [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) wraps the Java result in a Python table.

A column marked with [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) works only with [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md). [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), and [`lazy_update`](../../reference/table-operations/select/lazy-update.md) accept only [formula strings](../formulas.md). The engine also rejects such a column in those operations, even when you pass it through the Java API.

## Example: measure end-to-end latency

Because `ProcessTime` updates on every tick, you can compare it against a source timestamp, such as an exchange's trade timestamp, to measure end-to-end latency. This helps identify bottlenecks in your data pipeline. In this example, the time table's `Timestamp` column stands in for a source timestamp. The result shows how long after each scheduled tick Deephaven processed it. With real data, use the timestamp your source provides, such as the `KafkaTimestamp` column that Kafka ingestion adds.

Using `result` from the [solution above](#solution-force-re-evaluation-with-withrecomputeonmodifiedrow), subtract `SourceTime` from `ProcessTime` to calculate the latency:

```python ticking-table order=null test-set=1
# Calculate latency
result = result.update(
    [
        "LatencyNanos = ProcessTime - SourceTime",
        "LatencyMs = nanosToMillis(LatencyNanos)",
    ]
)
```

`LatencyNanos` is the time, in nanoseconds, between the source timestamp and the moment Deephaven processed the row. `LatencyMs` converts it to whole milliseconds with the built-in [`nanosToMillis`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosToMillis(long)) function. The conversion truncates toward zero, so a latency under one millisecond shows as `0`.

## Performance considerations

Using [`withRecomputeOnModifiedRow`](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) bypasses an important optimization. The [modified column set](../../conceptual/table-update-model.md#modified-columns) allows the engine to skip formula evaluation when inputs haven't changed, which can significantly reduce CPU usage for complex formulas.

Use this feature only when a formula must re-evaluate on every row modification even though its input columns didn't change, such as recording processing time. For most use cases, the default behavior of evaluating formulas only when their inputs change is correct and more efficient.

## Related documentation

- [Incremental update model](../../conceptual/table-update-model.md)
- [Create a time table](../time-table.md)
- [Formulas](../formulas.md)
- [Kafka overview](../overview-kafka.md)
- [Use jpy](../use-jpy.md)
- [Formulas and threads](formula-threads.md)
- [Parallelization](../../conceptual/query-engine/parallelization.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`lazy_update`](../../reference/table-operations/select/lazy-update.md)
- [`jpy.get_type` Pydoc](/core/pydoc/code/jpy.html#jpy.get_type)
- [`j_array_list` Pydoc](/core/pydoc/code/deephaven.jcompat.html#deephaven.jcompat.j_array_list)
- [`Table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`Selectable` Javadoc](/core/javadoc/io/deephaven/api/Selectable.html)
- [`SelectColumn` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html)
- [`SelectColumnFactory` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
- [`TableOperations` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html)
