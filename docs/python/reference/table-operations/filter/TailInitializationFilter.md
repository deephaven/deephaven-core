---
title: TailInitializationFilter
---

`TailInitializationFilter` filters an [add-only](../../../conceptual/table-types.md#specialization-2-add-only) source table down to its most recent rows, using either a timestamp window or a row count. This is particularly useful when working with large datasets that periodically publish new snapshots, and you intend to run a [`last_by`](../group-and-aggregate/lastBy.md) on the data to retrieve the most recent snapshot.

The filter treats the timestamp column's source as regioned (for example, Parquet-backed tables) or not. When the source is regioned, one partition is assumed per region. Otherwise, each contiguous range of row keys in the table is assumed to be a single partition. Each partition must be sorted by timestamp, with the most recent timestamp at the end.

Once initialized, the filter passes through all new rows appended to the source. Rows that have already been filtered are not removed or modified.

## Syntax

```python syntax
from deephaven.table import TailInitializationFilter

result = TailInitializationFilter.most_recent(table, ts_col, period)
result = TailInitializationFilter.most_recent_rows(table, row_count)
```

## Parameters

### `most_recent`

<ParamTable>
<Param name="table" type="Table">

The add-only source table to filter. Must be add-only with partitions sorted ascending by `ts_col`.

</Param>
<Param name="ts_col" type="str">

The name of the timestamp column used to determine recency.

</Param>
<Param name="period" type="DurationLike">

The look-behind window, measured from the newest timestamp in each partition. Accepts a duration string (for example, `"PT10S"`), an integer number of nanoseconds, a `datetime.timedelta`, or another [duration-like](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration) value. A period of `0` keeps only the rows that share the newest timestamp in each partition.

</Param>
</ParamTable>

### `most_recent_rows`

<ParamTable>
<Param name="table" type="Table">

The add-only source table to filter.

</Param>
<Param name="row_count" type="int">

The number of most-recent rows to keep per partition.

</Param>
</ParamTable>

## Returns

A new [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) containing only the most recent values from each partition in the source table. The result is refreshing if the source table is refreshing.

## Errors

`most_recent` and `most_recent_rows` raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) if the source table is not add-only, or if `ts_col` contains a null value. If a partition is not correctly sorted by timestamp, the result is undefined.

## Examples

### Filter by time period

This example filters a table of historical snapshots to show only rows from the last 10 seconds of each partition's timeline:

```python order=result,source
from deephaven import empty_table
from deephaven.table import TailInitializationFilter

source = empty_table(20).update(
    ["Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii"]
)

result = TailInitializationFilter.most_recent(source, "Timestamp", "PT00:00:10")
```

`source` spans 20 seconds of history; `result` keeps only the rows within 10 seconds of the newest timestamp in the partition.

### Filter by a `timedelta`

`period` also accepts a `datetime.timedelta`, which is convenient when the window is computed in Python:

```python order=result,source
from datetime import timedelta
from deephaven import empty_table
from deephaven.table import TailInitializationFilter

source = empty_table(20).update(
    ["Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii"]
)

result = TailInitializationFilter.most_recent(source, "Timestamp", timedelta(seconds=5))
```

### Filter by row count

`most_recent_rows` keeps a fixed number of rows from the end of each partition instead of using a time window:

```python order=result,source
from deephaven import empty_table
from deephaven.table import TailInitializationFilter

source = empty_table(20).update(
    ["Timestamp = '2026-01-01T00:00:00 UTC' + ii * SECOND", "Value = ii"]
)
row_count = 10

result = TailInitializationFilter.most_recent_rows(source, row_count)
```

## Related documentation

- [Filters](../../../how-to-guides/filters.md)
- [Create a time table](../create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.TailInitializationFilter)
