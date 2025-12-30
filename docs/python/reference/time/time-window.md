---
title: time_window
---

The `time_window` method creates a new table by applying a time window to the source table and adding a new Boolean column.

- The Boolean column's value will be `false` when the row's timestamp is older than the specified number of nanoseconds.
- If the timestamp is within _N_ nanoseconds (`windowNanos`) of the current time, the result column is `true`.
- If the timestamp is null, the value is null.
  The resulting table adds a new row whenever the source table ticks, and modifies a row's value in the `result` column (from `true` to `false`) when it passes out of the window.

## Syntax

```python syntax
time_window(table: Table, ts_col: str, window: int, bool_col: str) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The source table.

</Param>
<Param name="ts_col" type="str">

The timestamp column in the source table to monitor.

</Param>
<Param name="window" type="int">

How much time, in nanoseconds, to include in the window.
Rows with a `"Timestamp"` value greater than or equal to the current time minus `windowNanos` will be marked `true` in the new output column.
Results are refreshed on each [`UpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html) cycle.

</Param>
<Param name="bool_col" type="str">

The name of the new Boolean column.

</Param>
</ParamTable>

## Returns

A new table that contains an in-window Boolean column.

## Examples

The following example creates a time table, and then applies `time_window` to show whether a given row is within the last 10 seconds.

```python order=null ticking-table
from deephaven.experimental import time_window
from deephaven import time_table

source = time_table("PT00:00:01")

result = time_window(source, "Timestamp", 5000000000, "WithinLastFiveSeconds")
```

![The above `result` table, which displays only rows that ticked within the last five seconds](../../assets/reference/windowcheckpy.gif)

## Related documentation

- [Create a time table](../table-operations/create/timeTable.md)
- [How to use filters](../../how-to-guides/use-filters.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/WindowCheck.html)
