---
title: TimeSeriesFilter
---

`TimeSeriesFilter` filters a table for the most recent _N_ nanoseconds.

> [!CAUTION]
> `TimeSeriesFilter` must be used on a [`DateTime`](../query-language/types/date-time.md) column.

## Syntax

```
result = source.where(new TimeSeriesFilter(columnName, nanos))
result = source.where(new TimeSeriesFilter(columnName, period))
```

## Parameters

<ParamTable>
<Param name="columnName" type="String">

The name of the `DateTime` column to filter by.

</Param>

<Param name="nanos" type="long">

How much time, in nanoseconds, to include in the window.

</Param>
<Param name="period" type="String">

How much time to include in the window.

</Param>
</ParamTable>

## Returns

A new table that contains only the rows within the specified time window.

## Examples

The following example creates a time table, and then applies `TimeSeriesFilter` so that it only contains rows from the last 10 seconds.

```groovy order=null ticking-table
import io.deephaven.engine.table.impl.select.TimeSeriesFilter

source = timeTable("PT00:00:01")

result = source.where(new TimeSeriesFilter("Timestamp", "PT00:00:10"))
```

<LoopedVideo src='../../assets/reference/TimeSeriesFilterVideo.mp4' />

## Related documentation

- [Create a time table](../table-operations/create/timeTable.md)
- [How to use filters](../../how-to-guides/use-filters.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/TimeSeriesFilter.html)
