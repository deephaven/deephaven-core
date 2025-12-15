---
title: TailInitializationFilter
---

`TailInitializationFilter` optimizes the initialization of filtered tables when you only need the most recent rows from each partition. This is particularly useful when working with large historical datasets where you're primarily interested in the tail of the data.

The filter is designed to work with add-only source tables where each contiguous range of row keys represents a partition. Each partition must be sorted by timestamp, with the most recent timestamp at the end.

Once initialized, the filter passes through all new rows. Rows that have already been filtered are not removed or modified.

## Syntax

```syntax
result = TailInitializationFilter.mostRecent(table, timestampName, period)
result = TailInitializationFilter.mostRecent(table, timestampName, nanos)
result = TailInitializationFilter.mostRecentRows(table, rowCount)
```

## Parameters

### `mostRecent` (period)

<ParamTable>
<Param name="table" type="Table">

The source table to filter. Must be add-only with partitions sorted by timestamp.

</Param>
<Param name="timestampName" type="String">

The name of the timestamp column used to determine recency.

</Param>
<Param name="period" type="String">

The time period string specifying how far back from the last row to include rows. The period is parsed using `DateTimeUtils.parseDurationNanos()`.

Examples: `"PT1H"` (1 hour), `"PT30M"` (30 minutes), `"PT10S"` (10 seconds)

</Param>
</ParamTable>

### `mostRecent` (nanos)

<ParamTable>
<Param name="table" type="Table">

The source table to filter. Must be add-only with partitions sorted by timestamp.

</Param>
<Param name="timestampName" type="String">

The name of the timestamp column used to determine recency.

</Param>
<Param name="nanos" type="long">

The interval in nanoseconds between the last row in a partition and rows that match the filter.

</Param>
</ParamTable>

### `mostRecentRows`

<ParamTable>
<Param name="table" type="Table">

The source table to filter. Must be add-only.

</Param>
<Param name="rowCount" type="long">

The number of rows to include per partition.

</Param>
</ParamTable>

## Returns

A table containing only the most recent values from each partition in the source table.

## How it works

For each partition, the filter uses the last row's timestamp as the reference point. It subtracts the specified period from this timestamp and performs a binary search to identify rows within that time window.

The filter makes these assumptions:

- The source table is add-only (no modifications, shifts, or removals).
- Each contiguous range of row keys is a partition.
- Each partition is sorted by timestamp.
- Null timestamps are not permitted.

## Examples

### Filter by time period

This example uses a time table and filters to show only rows from the last 10 seconds:

```groovy order=null
import io.deephaven.engine.table.impl.util.TailInitializationFilter

source = timeTable("PT00:00:01").update("Value = ii")

result = TailInitializationFilter.mostRecent(source, "Timestamp", "PT00:00:10")
```

This filters to show only rows where the timestamp is within 10 seconds of the most recent row in the table.

### Filter by time in nanoseconds

This example filters to show rows from the last 5 seconds (5 billion nanoseconds):

```groovy order=null
import io.deephaven.engine.table.impl.util.TailInitializationFilter
import static io.deephaven.time.DateTimeUtils.SECOND

source = timeTable("PT00:00:01").update("Value = ii")

result = TailInitializationFilter.mostRecent(source, "Timestamp", 5 * SECOND)
```

### Filter by row count

The `mostRecentRows()` method filters to show a specified number of rows from the end of each partition:

```groovy syntax
result = TailInitializationFilter.mostRecentRows(table, rowCount)
```

## Related documentation

- [Filters](../../../how-to-guides/filters.md)
- [Create a time table](../create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html)
