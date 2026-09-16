---
title: TailInitializationFilter
---

`TailInitializationFilter` reduces the input size for downstream operations by limiting initialization to only the most recent rows from each partition. This is particularly useful when working with large datasets that periodically publish new snapshots, and you intend to run a [`last_by`](../group-and-aggregate/lastBy.md) on the data to retrieve the most recent snapshot.

The filter is designed to work with [add-only](../../../conceptual/table-types.md#specialization-2-add-only) source tables with one or more partitions. If the input table is in Parquet or Enterprise format, partitions are detected automatically. Otherwise, each contiguous range of row keys is assumed to represent a partition. Each partition must be sorted by timestamp, with the most recent timestamp at the end.

Once initialized, the filter passes through all new rows. Rows that have already been filtered are not removed or modified.

`TailInitializationFilter` is a Java utility class. It is not exposed as a first-class Python API, but it can be accessed from Python through [`jpy`](../../../how-to-guides/use-jpy.md).

> [!NOTE]
> Python API methods on [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) automatically acquire the update graph lock when needed. Because `TailInitializationFilter` is called directly through `jpy`, that automatic locking does not apply. If the source table is refreshing (for example, created with [`time_table`](../create/timeTable.md)), wrap the call in [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx) to avoid an `IllegalStateException`.

## Syntax

```python syntax
import jpy
from deephaven.update_graph import auto_locking_ctx

TailInitializationFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.util.TailInitializationFilter"
)

with auto_locking_ctx(table):
    result = TailInitializationFilter.mostRecent(table.j_table, timestamp_name, period)
    result = TailInitializationFilter.mostRecent(table.j_table, timestamp_name, nanos)
    result = TailInitializationFilter.mostRecentRows(table.j_table, row_count)
```

## Parameters

### `mostRecent` (period)

<ParamTable>
<Param name="table" type="Table">

The `j_table` of the source table to filter. Must be add-only with partitions sorted by timestamp.

</Param>
<Param name="timestamp_name" type="str">

The name of the timestamp column used to determine recency.

</Param>
<Param name="period" type="str">

The time period string specifying how far back from the last row to include rows. The period is parsed using `DateTimeUtils.parseDurationNanos()`.

Examples: `"PT1H"` (1 hour), `"PT30M"` (30 minutes), `"PT10S"` (10 seconds)

</Param>
</ParamTable>

### `mostRecent` (nanos)

<ParamTable>
<Param name="table" type="Table">

The `j_table` of the source table to filter. Must be add-only with partitions sorted by timestamp.

</Param>
<Param name="timestamp_name" type="str">

The name of the timestamp column used to determine recency.

</Param>
<Param name="nanos" type="int">

The interval in nanoseconds between the last row in a partition and rows that match the filter.

</Param>
</ParamTable>

### `mostRecentRows`

<ParamTable>
<Param name="table" type="Table">

The `j_table` of the source table to filter. Must be add-only.

</Param>
<Param name="row_count" type="int">

The number of rows to include per partition.

</Param>
</ParamTable>

## Returns

A Java table object containing only the most recent values from each partition in the source table. Wrap the result with [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) to use it as a Deephaven Python table.

## How it works

For each partition, the filter uses the last row's timestamp as the reference point. It subtracts the specified period from this timestamp and performs a binary search to identify rows within that time window.

The filter makes these assumptions:

- The source table is add-only (no modifications, shifts, or removals).
- Each partition is sorted by timestamp.
- Null timestamps are not permitted.

If any of these assumptions are violated, the result table is undefined.

## Examples

### Filter by time period

This example uses a time table and filters to show only rows from the last 10 seconds:

```python order=result,source
import jpy
from deephaven import time_table
from deephaven.table import Table
from deephaven.update_graph import auto_locking_ctx

TailInitializationFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.util.TailInitializationFilter"
)

source = time_table("PT00:00:01").update("Value = ii")

with auto_locking_ctx(source):
    result = Table(
        TailInitializationFilter.mostRecent(source.j_table, "Timestamp", "PT00:00:10")
    )
```

This filters to show only rows where the timestamp is within 10 seconds of the most recent row in the table.

### Filter by time in nanoseconds

This example filters to show rows from the last 5 seconds (5 billion nanoseconds):

```python order=result,source
import jpy
from deephaven import time_table
from deephaven.table import Table
from deephaven.update_graph import auto_locking_ctx

TailInitializationFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.util.TailInitializationFilter"
)

source = time_table("PT00:00:01").update("Value = ii")

with auto_locking_ctx(source):
    result = Table(
        TailInitializationFilter.mostRecent(source.j_table, "Timestamp", 5_000_000_000)
    )
```

### Filter by row count

The `mostRecentRows` method filters to show a specified number of rows from the end of each partition:

```python order=result,source
import jpy
from deephaven import time_table
from deephaven.table import Table
from deephaven.update_graph import auto_locking_ctx

TailInitializationFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.util.TailInitializationFilter"
)

source = time_table("PT00:00:01").update("Value = ii")
row_count = 10

with auto_locking_ctx(source):
    result = Table(TailInitializationFilter.mostRecentRows(source.j_table, row_count))
```

## Related documentation

- [Filters](../../../how-to-guides/filters.md)
- [How to use jpy](../../../how-to-guides/use-jpy.md)
- [Create a time table](../create/timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TailInitializationFilter.html)
