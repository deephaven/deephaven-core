---
title: Track processing time and measure latency
sidebar_label: Track processing time
---

This guide explains how to track when Deephaven processes row modifications, which is useful for measuring end-to-end latency in real-time data pipelines.

> [!NOTE]
> This feature requires using jpy to access the Java API directly. See the [Groovy documentation](/core/groovy/docs/how-to-guides/performance/track-processing-time) for a full explanation of the concepts.

## Problem

By default, Deephaven optimizes formula evaluation by only recomputing values when their input columns change. A formula like `ProcessTime = now()` evaluates once when the row is created but does not re-evaluate when other columns change — because `now()` has no column dependencies.

This matters when you receive data from external sources (like Kafka) that include a source timestamp. You want to compare that timestamp against when Deephaven processed the update — but a simple `now()` formula won't re-evaluate when the row is modified.

## Solution

Use [`SelectColumnFactory.getExpression`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumnFactory.html#getExpression(java.lang.String)) with [`withRecomputeOnModifiedRow`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/select/SelectColumn.html#withRecomputeOnModifiedRow()) to force the formula to re-evaluate every time a row is modified.

Since there's no native Python wrapper for this feature, use `jpy.get_type` to access the Java class. The resulting `SelectColumn` can be passed directly to `update` — Python's table wrapper accepts Java `Selectable` instances and handles conversion automatically.

```python ticking-table order=null
import jpy
from deephaven import time_table

SelectColumnFactory = jpy.get_type(
    "io.deephaven.engine.table.impl.select.SelectColumnFactory"
)

# last_by() collapses to a single row that gets modified on each tick
source = time_table("PT1S").update(["SourceTime = Timestamp"]).last_by()

# Force now() to re-evaluate on every modification
process_time_col = SelectColumnFactory.getExpression(
    "ProcessTime = now()"
).withRecomputeOnModifiedRow()

# Pass the Java SelectColumn directly to update
result = source.update(process_time_col)

# Calculate latency
result = result.update(
    [
        "LatencyNanos = ProcessTime - SourceTime",
        "LatencyMs = nanosToMillis(LatencyNanos)",
    ]
)
```

## Related documentation

- [Track processing time (Groovy)](/core/groovy/docs/how-to-guides/performance/track-processing-time) - Full explanation of concepts
- [Use jpy](../use-jpy.md)
- [Formulas and threads](formula-threads.md)
- [Parallelization](../../conceptual/query-engine/parallelization.md)
