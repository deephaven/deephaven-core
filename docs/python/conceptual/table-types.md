---
title: Deephaven's table types
---

Deephaven tables are the core data structures supporting Deephaven's static and streaming capabilities. Deephaven implements several specialized table types. They differ in how the engine stores and processes streaming data, and in how some downstream operations behave. This document covers static tables, standard streaming tables, and the four specialized streaming table types: [append-only](#specialization-1-append-only), [add-only](#specialization-2-add-only), [blink](#specialization-3-blink), and [ring](#specialization-4-ring).

## Table type summary

This guide discusses the unique properties of each of Deephaven's table types. The following table summarizes the main points.

<table className="text--center">
  <thead>
    <tr>
      <th>Table type</th>
      <th>Index-based ops (<code>i</code>, <code>ii</code>)</th>
      <th>Row key variable <code>k</code></th>
      <th>Consistent inter-cycle row ordering</th>
      <th>Bounded memory usage</th>
      <th>Inter-cycle data persistence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row" ><a href="#static-tables">Static</a></td>
      <td>✅</td>
      <td>✅</td>
      <td>🚫</td>
      <td>🚫</td>
      <td>🚫</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#standard-streaming-tables">Standard</a></td>
      <td>❌</td>
      <td>❌</td>
      <td>❌</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-1-append-only">Append-only</a></td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-2-add-only">Add-only</a></td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-3-blink">Blink</a></td>
      <td>✅</td>
      <td>✅</td>
      <td>🚫</td>
      <td>✅</td>
      <td>❌</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-4-ring">Ring</a></td>
      <td>❌</td>
      <td>❌</td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
    </tr>
  </tbody>
</table>

✅ means yes, ❌ means no, and 🚫 means the column doesn't apply to that table type. The columns mean the following:

- **Index-based ops (`i`, `ii`)**: Formulas can use the row-position [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii` and get stable results.
- **Row key variable `k`**: Formulas can use the special variable `k`, which holds a row's [row key](../reference/community-questions/shifts.md#key-terms), without raising an error.
- **Consistent inter-cycle row ordering**: Rows keep their order of arrival from one [update cycle](./table-update-model.md) to the next.
- **Bounded memory usage**: The table can't grow without bound as new data arrives.
- **Inter-cycle data persistence**: Rows stay in the table after the update cycle that added them.

The rest of this guide explores each of these table types, their properties, and the consequences of those properties.

## Static tables

Static tables are the simplest types of Deephaven tables and are analogous to [pandas DataFrames](https://pandas.pydata.org/docs/reference/frame.html), [PyArrow Tables](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html), and many other tabular representations of data. They represent data sources that do not update, and therefore do not support any of Deephaven's streaming capabilities.

Because static tables do not update, they have the following characteristics:

1. Index-based operations are fully supported. These are formulas that read a row's position through the [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii`, which hold the position as an `int` and a `long`. Row positions in a static table always run from `0` to `N-1` and never change.
2. Row keys never change either. The special variable `k` gives a row's [row key](../reference/community-questions/shifts.md#key-terms), a stable internal address for the row rather than its position.
3. A static table never reprocesses a row, so [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) evaluate each formula once per row. [`view`](../reference/table-operations/select/view.md) and [`update_view`](../reference/table-operations/select/update-view.md) instead evaluate a formula each time a cell is read.

Even on a static table, the engine assumes by default that formulas are stateless, which means they don't depend on mutable outside state or on the order in which rows are evaluated. It can therefore evaluate them in parallel. Mark a formula that changes shared state, or that depends on row order, with [`with_serial`](../reference/query-language/types/ConcurrencyControl.md#with_serial). When formulas in more than one column share state, they also need barriers. See [Parallelization](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where).

Reading [CSV](../how-to-guides/data-import-export/csv-import.md), [Iceberg](../how-to-guides/data-import-export/iceberg.md), [Parquet](../how-to-guides/data-import-export/parquet-import.md), or [SQL](../how-to-guides/data-import-export/execute-sql-queries.md) data produces a static table by default. You can also create one with a table creation function such as [`new_table`](../how-to-guides/new-and-empty-table.md#new_table) or [`empty_table`](../how-to-guides/new-and-empty-table.md#empty_table). This example uses `empty_table` to construct a static table:

```python test-set=1 order=t
from deephaven import empty_table

# create a static table with 10 rows and 2 columns
t = empty_table(10).update(["IntIdx = i", "LongIdx = ii"])
```

Check whether a table is a static table with the [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md) property. This property is `False` for static tables:

```python test-set=1 order=:log
print(t.is_refreshing)
```

Convert any streaming table to a static table by taking a [`snapshot`](../reference/table-operations/snapshot/snapshot.md). The snapshot is a static copy of the streaming table at the moment you take it:

```python test-set=2 ticking-table order=null
from deephaven import time_table

# create a streaming table with time_table
t = time_table("PT1s")

# take a static snapshot of the streaming table as it is right now
t_snapshot = t.snapshot()
```

![Two Deephaven tables side by side — 't' ticks, while 't_snapshot' is static](../assets/conceptual/table-types/table-types-1.gif)

Verify that the snapshot is static with [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md):

```python test-set=2 order=:log
print(t.is_refreshing)
print(t_snapshot.is_refreshing)
```

## Standard streaming tables

Most streaming Deephaven tables are "standard" tables. Deephaven applies changes to streaming tables in batches called [update cycles](./table-update-model.md). Each cycle delivers the rows added, removed, modified, and moved since the previous cycle. Standard tables are the most flexible and least constrained types of tables, with the following key properties:

- Rows can be added, modified, deleted, or shifted (moved to a different [row key](../reference/community-questions/shifts.md#key-terms)) at any time, at any position in the table.
- The table's size can grow without bound.

These properties have some important consequences:

1. Formulas that use the [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii` would return values that change between update cycles, so by default Deephaven raises an error when a formula on a standard table uses `i`, `ii`, `k`, or most [column array](../how-to-guides/work-with-arrays.md) expressions. A formula that uses `k` raises an error because the engine allows `k` only on add-only and blink tables, and a standard table can remove rows or shift them to new row keys.
2. Stateful operations change state outside the row, such as a counter, or depend on the order in which rows are evaluated. On a streaming table, the engine recomputes a formula whenever it reprocesses a row, such as a modified row in a later update cycle, so a stateful formula can return a different value for the same row or change that state more than once. Deephaven can't detect stateful operations, so it doesn't raise an error for them the way it does for `i` and `ii`. Avoid them on standard tables.
3. The rows in standard tables are not guaranteed to maintain their original order of arrival. Operations should not assume anything about the order of data in a standard table.
4. Standard tables may eventually result in out-of-memory errors in data-intensive applications.

These properties are not ideal for every use case. Deephaven's specialized table types provide alternatives.

## Specialization 1: Append-only

Append-only tables are highly constrained types of tables. They have the following key properties:

- Rows can only be added to the _end_ of the table.
- Once a row is in an append-only table, it cannot be modified, deleted, or shifted to a new [row key](../reference/community-questions/shifts.md#key-terms).
- The table's size can grow without bound.

These properties yield the following consequences:

1. Append-only tables guarantee that old rows do not change, move, or disappear, so operations using the [special variables](../reference/query-language/variables/special-variables.md) `i`, `ii`, and `k` are allowed and yield stable values for existing rows.
2. By default, most [column array](../how-to-guides/work-with-arrays.md) expressions still raise an error on append-only tables. Simple constant offsets such as `X_[i-1]` are the exception.
3. Deephaven can't detect [stateful operations](#standard-streaming-tables), so avoid them on append-only tables.
4. The rows in append-only tables are guaranteed to maintain their original order of arrival.
5. Append-only tables may eventually result in out-of-memory errors in data-intensive applications.

Append-only tables are useful when the use case needs a complete and ordered history of every record ingested from a stream. They are predictable for index-based operations and are guaranteed to retain all the data they've seen.

A [time table](../how-to-guides/time-table.md) is append-only unless you create it as a [blink table](#specialization-3-blink), which keeps only the current update cycle's rows. To get an append-only table from a blink table, see [Create an append-only table from a blink table](#create-an-append-only-table-from-a-blink-table).

## Specialization 2: Add-only

Add-only tables are relaxed versions of append-only tables. They have the following key properties:

- Rows can only be added to the table. They may be added at _any position_, as long as each new row gets a new [row key](../reference/community-questions/shifts.md#key-terms).
- Existing rows cannot be deleted, modified, or shifted to new row keys. Their positions, and so their `i` and `ii` values, can change when new rows are inserted before them.
- The table's size can grow without bound.

These properties yield the following consequences:

1. Formulas that use the [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii` would return values that change between update cycles, so by default Deephaven raises an error when a formula on an add-only table uses `i`, `ii`, or most [column array](../how-to-guides/work-with-arrays.md) expressions. The row key variable `k` is still allowed, because existing row keys never change.
2. Deephaven can't detect [stateful operations](#standard-streaming-tables), so avoid them on add-only tables.
3. The rows in add-only tables are not guaranteed to maintain their original order of arrival. Operations should not assume anything about the order of data in an add-only table.
4. Add-only tables may eventually result in out-of-memory errors in data-intensive applications.

Every append-only table, such as a non-blink [time table](../how-to-guides/time-table.md), is also add-only. You can also convert any refreshing add-only table to a blink table. See [Create a blink table from an add-only table](#create-a-blink-table-from-an-add-only-table).

An add that requires a shift is _not_ an add-only operation. For example, the result of [`merge`](../reference/table-operations/merge/merge.md) is not always add-only. `merge` reserves a range of row keys for each source table. When a streaming source other than the last one outgrows its range, the rows from the tables after it shift to new row keys.

## Specialization 3: Blink

Blink tables keep only the set of rows received during the current update cycle. You can create blink tables when ingesting [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md), creating [time tables](../how-to-guides/time-table.md), or using a [`TablePublisher`](../how-to-guides/table-publisher.md#table-publisher). They have the following key properties:

- The table only consists of rows added in the current update cycle.
- No rows persist for more than one update cycle.
- The table's size is bounded by the size of the largest update it receives.

These properties have the following consequences:

1. Blink tables see a brand new world at every update cycle. Because a blink table contains only the current cycle's rows, formulas that use the [special variables](../reference/query-language/variables/special-variables.md) `i`, `ii`, and `k`, or [column arrays](../how-to-guides/work-with-arrays.md), are allowed. The engine evaluates them over only those rows.
2. Deephaven can't detect [stateful operations](#standard-streaming-tables), so avoid them on blink tables.
3. The entire table changes every update cycle, so preserving row order from cycle to cycle is irrelevant.
4. Blink tables can only cause memory problems if a single update cycle delivers more data than fits in available RAM. This is unusual, but not impossible.

Blink tables are the default table type for [Kafka ingestion](../how-to-guides/data-import-export/kafka-stream.md) within Deephaven because they use little memory. They are most useful for computing low-memory [aggregations](../how-to-guides/dedicated-aggregations.md) over the whole stream, deriving downstream tables, or running code in response to new data with [table listeners](../how-to-guides/table-listeners-python.md). For how those aggregations work, see [Specialized semantics for blink tables](#specialized-semantics-for-blink-tables).

Check whether a table is a blink table with the [`is_blink`](../reference/table-operations/metadata/is_blink.md) property:

```python test-set=3 ticking-table order=null
from deephaven import time_table

# time_table can be used to create a blink table with blink_table=True
t = time_table("PT0.2s", blink_table=True).update("X = ii")
```

![A ticking blink table](../assets/conceptual/table-types/table-types-2.gif)

```python test-set=3 order=:log
print(t.is_blink)
```

Most operations on blink tables behave exactly as they do on other tables and process added rows as usual. For example, the result of [`select`](../reference/table-operations/select/select.md) on a blink table contains only the newly added rows from the current update cycle. The exceptions are:

- [Aggregations](../how-to-guides/dedicated-aggregations.md) and [`select_distinct`](../reference/table-operations/select/select-distinct.md), which cover all rows seen since the operation started. See [Specialized semantics for blink tables](#specialized-semantics-for-blink-tables).
- [`head`](../reference/table-operations/filter/head.md) and [`tail`](../reference/table-operations/filter/tail.md), which keep the first or last `N` rows seen across all update cycles rather than only the current cycle's rows.
- [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) with a blink filter table, which keep every key the blink table has ever held rather than only the current cycle's keys.
- The [unsupported operations](#unsupported-operations), which raise an error.

### Specialized semantics for blink tables

[Aggregation operations](../how-to-guides/dedicated-aggregations.md) such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) use specialized semantics on blink tables. The result aggregates every row the blink table has delivered since you created the aggregation, not only the current cycle's rows. For example, a [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) on a blink table holds each group's sum over all rows seen since the `sum_by` started. For aggregations that keep a fixed amount of state per group, such as sums and counts, you can therefore aggregate over the full history of a stream while using far less memory than holding the whole stream in an in-memory table.

Here is an example that demonstrates a blink table's specialized aggregation semantics:

```python test-set=4 ticking-table order=null
from deephaven import time_table

# create blink table with two groups of data to sum
t = time_table("PT0.1s", blink_table=True).update(
    ["X = ii", "Group = ii % 2 == 0 ? `A` : `B`"]
)

# note that the sums continue to grow by including all previous data
t_sum = t.view(["X", "Group"]).sum_by("Group")
```

![Two Deephaven tables — table 't' is a blink table and 't_sum' is aggregated with `sum_by`](../assets/conceptual/table-types/table-types-3.gif)

These specialized aggregation semantics are not always what you want. The blink attribute is the table marker that turns on blink table semantics. To disable the semantics, call [`remove_blink`](../reference/table-operations/create/remove-blink.md) on the blink table. It returns a child table with the same rows as the parent blink table but without the blink attribute. The result still follows the blink table update pattern, where each cycle removes all previous rows and contains only the new ones:

```python test-set=4 ticking-table order=null
t_no_blink = t.remove_blink()

# sum is no longer over all data, but only over data in this cycle
t_sum_no_blink = t_no_blink.view(["X", "Group"]).sum_by("Group")
```

![Two Deephaven tables. Without the blink attribute, 't_sum_no_blink' only aggregates over data in this cycle](../assets/conceptual/table-types/table-types-4.gif)

### Unsupported operations

Attempting to use the following operations on a blink table raises an error:

- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) (see [Partition a blink table](#partition-a-blink-table) for a workaround)
- [`partitioned_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`head_pct`](../reference/table-operations/filter/head-pct.md)
- [`tail_pct`](../reference/table-operations/filter/tail-pct.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`slice_pct`](../reference/table-operations/filter/slice-pct.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) if [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md), [`agg.partition`](../reference/table-operations/group-and-aggregate/AggPartition.md), or [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) is used
- [`agg_all_by`](../reference/table-operations/group-and-aggregate/AggAllBy.md) if [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) or the deprecated `formula_param` form of [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) is used
- [`rollup`](../reference/table-operations/create/rollup.md) if `include_constituents=True`, or if [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md), [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md), [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md), [`agg.sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md), or [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) is used
- [`tree`](../reference/table-operations/create/tree.md)

### Partition a blink table

[`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) raises an error on a blink table, so partitioning one takes three steps:

1. Call [`remove_blink`](../reference/table-operations/create/remove-blink.md) to drop the blink attribute.
2. Partition the result with [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md).
3. Call [`transform`](../reference/table-operations/partitioned-tables/transform.md) with [`with_attributes`](../reference/table-operations/create/withAttributes.md) to set the blink attribute again on each constituent table of the [partitioned table](../how-to-guides/partitioned-tables.md).

The following example chains these three calls:

```python ticking-table order=null
from deephaven import time_table
import jpy

_JTable = jpy.get_type("io.deephaven.engine.table.Table")

table = time_table("PT0.1s", blink_table=True).update(
    ["X = ii", "Group = ii % 2 == 0 ? `A` : `B`"]
)

partitioned_blink_table = (
    table.remove_blink()
    .partition_by("Group")
    .transform(lambda t: t.with_attributes({_JTable.BLINK_TABLE_ATTRIBUTE: True}))
)
```

### Create an append-only table from a blink table

It is common to create an append-only table from a blink table to preserve the entire data history. Use [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md) to do this:

```python ticking-table order=null
from deephaven import time_table
from deephaven.stream import blink_to_append_only

t = time_table("PT1s", blink_table=True)

# get an append-only table from t
t_append_only = blink_to_append_only(t)
```

![An append-only table that preserves the data from table 't' as it ticks](../assets/conceptual/table-types/table-types-5.gif)

### Create a blink table from an add-only table

Use [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md) to convert an add-only table to a blink table. The conversion helps when the upstream add-only table is not fully in memory, or when you want [blink table aggregation semantics](#specialized-semantics-for-blink-tables). If the upstream table already holds all its rows in memory, the conversion doesn't reduce memory use, because the upstream table still keeps every row.

The following example converts an append-only time table to a blink table:

```python ticking-table order=null
from deephaven import time_table
from deephaven.stream import add_only_to_blink

# t is append-only, which is a subset of add-only
t = time_table("PT0.5s")

# get a blink table from t
t_blink = add_only_to_blink(t)
```

![Two Deephaven tables. Table 't_blink' creates a blink table from the data in table 't'](../assets/conceptual/table-types/table-types-5.5.gif)

## Specialization 4: Ring

[Ring tables](../how-to-guides/ring-table.md) keep only the latest `N` rows they receive, where `N` is a capacity you set. They have the following key properties:

- New rows are appended at the end. Once the table holds `N` rows, the oldest rows are removed from the front as new rows arrive.
- Rows are never modified or shifted, so arrival order is preserved.
- The positions of retained rows, and so their `i` and `ii` values, change as old rows are dropped.
- The table's size is strictly limited to the latest `N` rows.

These properties have the following consequences:

1. Formulas that use the [special variables](../reference/query-language/variables/special-variables.md) `i` and `ii` would return values that change between update cycles, so by default Deephaven raises an error when a formula on a ring table uses `i`, `ii`, or most [column array](../how-to-guides/work-with-arrays.md) expressions. A formula that uses `k` also raises an error, because the engine allows `k` only on add-only and blink tables, and a ring table removes rows.
2. Deephaven can't detect [stateful operations](#standard-streaming-tables), so avoid them on ring tables.
3. The rows in ring tables maintain their original order of arrival.
4. Ring tables _do not_ grow without bound and are strictly limited to a maximum number of rows. Once the table reaches that limit, it removes the oldest rows from memory.

Ring tables do not get the [specialized aggregation semantics](#specialized-semantics-for-blink-tables) that blink tables do. However, because a ring table discards old data, operations downstream of it can use less memory than the same operations on an append-only or standard table that keeps growing.

### Create a ring table from a blink table

It is common to create a ring table from a blink table to preserve _some_ data history, _but not all_. Use [`ring_table`](../reference/table-operations/create/ringTable.md) to do this.

The following example creates a ring table that holds the five most recent rows from a blink table:

```python ticking-table order=null
from deephaven import time_table, ring_table

t = time_table("PT0.5s", blink_table=True)

# get ring table from t that holds last five rows
t_ring = ring_table(parent=t, capacity=5)
```

![Two Deephaven tables — a time table 't', and a 5-row ring table, 't_ring'](../assets/conceptual/table-types/table-types-6.gif)

### Create a ring table from an append-only table

> [!CAUTION]
> Creating a ring table from an append-only table does not give the memory savings that ring tables are useful for.

You can also create ring tables from append-only tables with [`ring_table`](../reference/table-operations/create/ringTable.md). The append-only table upstream keeps every row, so it can grow until it eats up all available memory. The downstream ring table only _appears_ to save memory, and is effectively a [`tail`](../reference/table-operations/filter/tail.md) of that table.

This example creates a ring table with a 5-row capacity from an append-only time table:

```python ticking-table order=null
from deephaven import time_table, ring_table

# t is an append-only table
t = time_table("PT0.5s")

# get ring table from t that holds last five rows
t_ring = ring_table(parent=t, capacity=5)
```

![An append-only time table and a 5-row ring table](../assets/conceptual/table-types/table-types-7.gif)

### Include rows already in the source table

If the source table _already_ has rows in it when you call [`ring_table`](../reference/table-operations/create/ringTable.md), the resulting ring table includes those rows by default:

```python test-set=5 ticking-table order=null
from deephaven import empty_table, time_table, ring_table, merge

# create a ticking table that starts with five rows
t_static = empty_table(5).update("X = ii")
t_dynamic = time_table("PT1s").update("X = ii + 5").drop_columns("Timestamp")
t = merge([t_static, t_dynamic])

# get ring table from t that holds last ten rows
t_ring_with_initial = ring_table(parent=t, capacity=10)
```

![A ring table initializes with the rows that are already present in the source table](../assets/conceptual/table-types/table-types-8.gif)

To disable this behavior, set `initialize=False`:

```python test-set=5 ticking-table order=null
t_ring_without_initial = ring_table(parent=t, capacity=10, initialize=False)
```

![When `initialize=False`, a ring table does not initialize with data that existed in the source table before the ring table was created](../assets/conceptual/table-types/table-types-9.gif)

## Related documentation

- [How to use a `TablePublisher`](../how-to-guides/table-publisher.md#table-publisher)
- [Create a time table](../how-to-guides/time-table.md)
- [Create a ring table](../how-to-guides/ring-table.md)
- [Kafka basic terminology](./kafka-basic-terms.md)
