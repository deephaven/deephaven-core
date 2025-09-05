---
title: addTimeWindow
---

The `addTimeWindow` method adds a Boolean column to the source table.

- The Boolean column's value will be `false` when the row's timestamp is older than the specified number of nanoseconds.
- If the timestamp is within _N_ nanoseconds (`windowNanos`) of the current time, the result column is `true`.
- If the timestamp is null, the value is null.
  The resulting table adds a new row whenever the source table ticks, and modifies a row's value in the `result` column (from `true` to `false`) when it passes out of the window.

## Syntax

```
result = WindowCheck.addTimeWindow(table, timestampColumn, windowNanos, inWindowColumn)
```

## Parameters

<ParamTable>
<Param name="table" type="QueryTable">

The source table.

</Param>
<Param name="timestampColumn" type="String">

The timestamp column in the source table to monitor.

</Param>
<Param name="windowNanos" type="long">

How much time, in nanoseconds, to include in the window.
Rows with a `"Timestamp"` value greater than or equal to the current time minus `windowNanos` will be marked `true` in the new output column.
Results are refreshed on each [`UpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html) cycle.

</Param>
<Param name="inWindowColumn" type="String">

The name of the new Boolean column.

</Param>
</ParamTable>

## Returns

A new table that contains an in-window Boolean column.

## Examples

The following example creates a time table, and then applies `addTimeWindow` to show whether a given row is within the last 10 seconds.

```groovy order=null ticking-table
import io.deephaven.engine.util.WindowCheck

source = timeTable("PT00:00:01")

result = WindowCheck.addTimeWindow(source, "Timestamp", SECOND * 10, "WithinLast10Seconds")
```

![The above `result` table, which displays only rows that ticked within the last ten seconds](../../assets/reference/WindowCheck.gif)

## Related documentation

- [Create a time table](../table-operations/create/timeTable.md)
- [How to use filters](../../how-to-guides/use-filters.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/WindowCheck.html)
