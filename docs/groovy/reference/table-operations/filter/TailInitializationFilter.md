---
title: TailInitializationFilter
---

`TailInitializationFilter` reduces the input size for downstream operations by limiting initialization to only the most recent rows from each partition. This is particularly useful when working with large datasets that periodically publish new snapshots, and you intend to run a `lastBy` on the data to retrieve the most recent snapshot.

The filter is designed to work with add-only source tables with one or more partitions. `mostRecent` detects partitions from the timestamp column. When that column's source is regioned (for example, Parquet-backed tables), one partition is assumed per region. Otherwise, each contiguous range of row keys is assumed to represent a single partition. Each partition must be sorted by timestamp, with the most recent timestamp at the end.

`mostRecentRows` never reads the timestamp column and does not require sorted timestamps. Since it has no timestamp argument, it detects partitions the same way `mostRecent` does, but checks whether any column in the table is regioned rather than the timestamp column specifically. It keeps the trailing rows of each partition by row position.

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

For each partition, `mostRecent` uses the last row's timestamp as the reference point. It subtracts the specified period from this timestamp and performs a binary search to identify rows within that time window.

`mostRecent` makes these assumptions:

- The source table is add-only (no modifications, shifts, or removals).
- Each partition is sorted by timestamp.
- Null timestamps are not permitted.

Violating the add-only requirement raises an `IllegalArgumentException`. The binary search reads only the first, last, and midpoint timestamps of each partition, not every row. It raises an `IllegalArgumentException` if one of those is null, but a null elsewhere in the partition may go undetected. If a partition is not correctly sorted by timestamp, the result table is undefined.

`mostRecentRows` never reads timestamps, so the sorting and null-timestamp assumptions do not apply to it. Only the add-only requirement does.

## Examples

### Filter by time period

This example filters a table of historical snapshots to show only rows from the last 10 seconds of the partition's timeline:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.TailInitializationFilter

source = emptyTable(20).update("Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii")

result = TailInitializationFilter.mostRecent(source, "Timestamp", "PT00:00:10")
```

`source` spans 20 seconds of history; `result` keeps only the rows within 10 seconds of the newest timestamp in the partition.

### Filter by time in nanoseconds

This example filters to show rows from the last 5 seconds (5 billion nanoseconds):

```groovy order=result,source
import io.deephaven.engine.table.impl.util.TailInitializationFilter
import static io.deephaven.time.DateTimeUtils.SECOND

source = emptyTable(20).update("Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii")

result = TailInitializationFilter.mostRecent(source, "Timestamp", 5 * SECOND)
```

### Filter by row count

The `mostRecentRows` method filters to show a specified number of rows from the end of each partition:

```groovy order=result,source
source = emptyTable(20).update("Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii")
rowCount = 10
result = TailInitializationFilter.mostRecentRows(source, rowCount)
```

## Related documentation

- [Filters](../../../how-to-guides/filters.md)
- [Create a time table](../create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html)
