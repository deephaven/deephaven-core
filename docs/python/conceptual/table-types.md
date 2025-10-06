---
title: Deephaven's table types
sidebar_label: Table types
---

Deephaven tables are the core data structures supporting Deephaven's static and streaming capabilities. Deephaven implements several specialized table types that differ in how streaming data is stored and processed in the engine, how the UI displays data to the user, and how some downstream operations behave. This document covers static tables, standard streaming tables, and the four specialized streaming table types: [append-only](#specialization-1-append-only), [add-only](#specialization-2-add-only), [blink](#specialization-3-blink), and [ring](#specialization-4-ring).

## Table type summary

This guide discusses the unique properties of each of Deephaven's table types. The main points are summarized in the following table.

<table className="text--center">
  <thead>
    <tr>
      <th>Table type</th>
      <th>Index-based ops, special variables</th>
      <th>Consistent inter-cycle row ordering</th>
      <th>Bounded memory usage</th>
      <th>Inter-cycle data persistence</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row" ><a href="#static-tables">Static</a></td>
      <td>✅</td>
      <td>🚫</td>
      <td>❌</td>
      <td>🚫</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#standard-streaming-tables">Standard</a></td>
      <td>❌</td>
      <td>❌</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-1-append-only">Append-only</a></td>
      <td>✅</td>
      <td>✅</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-2-add-only">Add-only</a></td>
      <td>❌</td>
      <td>❌</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a href="#specialization-3-blink">Blink</a></td>
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
    </tr>
  </tbody>
</table>

The rest of this guide explores each of these table types, their properties, and the consequences of those properties.

## Static tables

Static tables are the simplest types of Deephaven tables and are analogous to [Pandas Dataframes](https://pandas.pydata.org/docs/reference/frame.html), [PyArrow Tables](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html), and many other tabular representations of data. They represent data sources that do not update, and therefore do not support any of Deephaven's streaming capabilities.

Because static tables do not update, they have the following characteristics:

1. Index-based operations are fully supported. The row indices of a static table always range from `0` to `N-1`, so operations can depend on these values to be stable.
2. Operations that depend on or modify external state can be used with static tables. Stateful operations can present problems for some types of streaming tables.
3. The use of [special variables](../reference/query-language/variables/special-variables.md) is fully supported. Deephaven's special variables `i` and `ii` represent row indices of a table as `int` or `long` types, respectively. These variables are guaranteed to have consistent values in static tables.

Static tables can be created by reading from a static data source, such as [CSV](../how-to-guides/data-import-export/csv-import.md), [Iceberg](../how-to-guides/data-import-export/iceberg.md), [Parquet](../how-to-guides/data-import-export/parquet-import.md), [SQL](../how-to-guides/data-import-export/execute-sql-queries.md). Or, they can be created with Deephaven's table creation functions, like [`new_table`](../how-to-guides/new-and-empty-table.md#new_table) or [`empty_table`](../how-to-guides/new-and-empty-table.md#empty_table). This example uses [`empty_table`](../how-to-guides/new-and-empty-table.md#new_table) to construct a static table:

```python test-set=1 order=t
from deephaven import empty_table

# create a static table with 10 rows and 2 columns
t = empty_table(10).update(["IntIdx = i", "LongIdx = ii"])
```

Check whether a table is a static table with the [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md) property. This property will be `False` for static tables:

```python test-set=1 order=:log
print(t.is_refreshing)
```

Any streaming table can be converted to a static table by taking a [`snapshot`](../reference/table-operations/snapshot/snapshot.md). This will produce a static copy of the streaming table at the moment in time the snapshot is taken:

```python test-set=2 ticking-table order=null
from deephaven import time_table

# create a streaming table with time_table
t = time_table("PT1s")

# at some point in the future, take a snapshot of the streaming table
t_snapshot = t.snapshot()
```

![Two Deephaven tables side by side - 't' ticks, while 't_snapshot' is static](../assets/conceptual/table-types/table-types-1.gif)

Verify that the snapshot is static with [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md):

```python test-set=2 order=:log
print(t.is_refreshing)
print(t_snapshot.is_refreshing)
```

## Standard streaming tables

Most streaming Deephaven tables are "standard" tables. These are the most flexible and least constrained types of tables, with the following key properties:

- Rows can be added, modified, deleted, or reindexed at any time, at any position in the table.
- The table's size can grow without bound.

These properties have some important consequences:

1. Index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) may yield results that change unexpectedly between update cycles. By default, Deephaven throws an error in these cases.
2. The rows in standard tables are not guaranteed to maintain their original order of arrival. Operations should not assume anything about the order of data in a standard table.
3. Standard tables may eventually result in out-of-memory errors in data-intensive applications.

These properties are not ideal for every use case. Deephaven's specialized table types provide alternatives.

## Specialization 1: Append-only

Append-only tables are highly-constrained types of tables. They have the following key properties:

- Rows can only be added to the _end_ of the table.
- Once a row is in an append-only table, it cannot be modified, deleted, or reindexed.
- The table's size can grow without bound.

These properties yield the following consequences:

1. Append-only tables guarantee that old rows will not change, move, or disappear, so index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) are guaranteed to yield results that do not change unexpectedly between update cycles.
2. The rows in append-only tables are guaranteed to maintain their original order of arrival.
3. Append-only tables may eventually result in out-of-memory errors in data-intensive applications.

Append-only tables are useful when the use case needs a complete and ordered history of every record ingested from a stream. They are safe and predictable under any Deephaven query and are guaranteed to retain all the data they've seen.

## Specialization 2: Add-only

Add-only tables are relaxed versions of append-only tables. They have the following key properties:

- Rows can only be added to the table, but they may be added at _any position_ in the table.
- Existing rows cannot be deleted or modified, but may be reindexed.
- The table's size can grow without bound.

These properties yield the following consequences:

1. Index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) may yield results that change unexpectedly between update cycles. By default, Deephaven throws an error in these cases.
2. The rows in add-only tables are not guaranteed to maintain their original order of arrival. Operations should not assume anything about the order of data in an add-only table.
3. Add-only tables may eventually result in out-of-memory errors in data-intensive applications.

## Specialization 3: Blink

Blink tables keep only the set of rows received during the current update cycle. Users can create blink tables when ingesting [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md), creating [time tables](../how-to-guides/time-table.md), or using [Table Publishers](../how-to-guides/table-publisher.md). They have the following key properties:

- The table only consists of rows added in the previous update cycle.
- No rows persist for more than one update cycle.
- The table's size is bounded by the size of the largest update it receives.

These properties have the following consequences:

1. Since blink tables see a brand new world at every update cycle, index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) are guaranteed to yield results that do not change unexpectedly between update cycles.
2. The entire table changes every update cycle, so preserving row order from cycle to cycle is irrelevant.
3. Blink tables can only cause memory problems if a single update receives more data than fits in available RAM. This is unusual, but not impossible.

Blink tables are the default table type for Kafka ingestion within Deephaven because they use little memory. They are most useful for low-memory aggregations, deriving downstream tables, or using programmatic listeners to react to data.

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

### Specialized semantics for blink tables

Aggregation operations such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) operate with special semantics on blink tables, allowing the result to aggregate over the entire observed stream of rows from the time the operation is initiated. That means, for example, that a [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) on a blink table will contain the resulting sums for each aggregation group over all observed rows since the [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) was applied, rather than just the sums for the current update cycle. This allows for aggregations over the full history of a stream to be performed with greatly reduced memory costs when compared to the alternative strategy of holding the entirety of the stream as an in-memory table.

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

![Two Deephaven tables - table 't' is a blink table and 't_sum' is aggregated with `sum_by`](../assets/conceptual/table-types/table-types-3.gif)

These special aggregation semantics may not always be desirable. Disable them by calling `remove_blink` on the blink table:

<!---TODO: Link remove_blink pydoc when it exists-->

```python test-set=4 ticking-table order=null
t_no_blink = t.remove_blink()

# sum is no longer over all data, but only over data in this cycle
t_sum_no_blink = t_no_blink.view(["X", "Group"]).sum_by("Group")
```

![Two Deephaven tables. Without the blink attribute, 't_sum_no_blink' only aggregates over data in this cycle](../assets/conceptual/table-types/table-types-4.gif)

Most operations on blink tables behave exactly as they do on other tables (see the [exclusions below](#unsupported-operations)); that is, added rows are processed as usual. For example, [`select`](../reference/table-operations/select/select.md) on a blink table will contain only the newly added rows from the current update cycle.

Because Deephaven does not need to keep all the history of rows read from the input stream in memory, table operations on blink tables may require less memory.

> [!TIP]
> To disable blink table semantics, use [`remove_blink`](../reference/table-operations/create/remove-blink.md), which returns a child table that is identical to the parent blink table in every way, but is no longer marked for special blink table semantics. The resulting table will still exhibit the “blink” table update pattern, removing all previous rows on each cycle, and thus only containing “new” rows.

### Unsupported operations

Attempting to use the following operations on a blink table will raise an error:

- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) (see [below](#partition-a-blink-table) for a workaround)
- [`partition_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`head_pct`](../reference/table-operations/filter/head-pct.md)
- [`tail_pct`](../reference/table-operations/filter/tail-pct.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`slice_pct`](../reference/table-operations/filter/slice-pct.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) if either `group` or `partition` is used.
- [`rollup`](../reference/table-operations/create/rollup.md) if `includeConstituents=true`.
- [`tree`](../reference/table-operations/create/tree.md)

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

It may be useful to create a blink table from an add-only table. This will only provide real benefit if the upstream add-only table is not fully in-memory. In this case, the operation will not fail, but there will be no memory savings. Use [`add_only_to_blink`](../reference/table-operations/create/add-only-to-blink.md) to accomplish this:

```python ticking-table order=null
from deephaven import time_table
from deephaven.stream import add_only_to_blink

# t is append-only, which is a subset of add-only
t = time_table("PT0.5s")

# get a blink table from t
t_blink = add_only_to_blink(t)
```

![Two Deephaven tables. Table 't_blink' creates a blink table from the data in table 't'](../assets/conceptual/table-types/table-types-5.5.gif)

### Partition a blink table

To partition a blink table, drop the blink attribute, create the partition, and then use a transform to add back the blink table attribute:

```python
from deephaven import time_table
import jpy

tOb = jpy.get_type("io.deephaven.engine.table.Table")

table = time_table("PT0.1s", blink_table=True).update(
    ["X = ii", "Group = ii % 2 == 0 ? `A` : `B`"]
)

partitioned_blink_table = (
    table.remove_blink()
    .partition_by("Group")
    .transform(lambda t: t.with_attributes({tOb.BLINK_TABLE_ATTRIBUTE: True}))
)
```

## Specialization 4: Ring

Ring tables are like standard tables, but are limited in how large they can grow. They have the following key properties:

- Rows can be added, modified, deleted, or reindexed at any time, at any position in the table.
- The table's size is strictly limited to the latest `N` rows, set by the user. As new rows are added, old rows are discarded so as to not exceed the maximum limit.

These properties have the following consequences:

1. Index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) may yield results that change unexpectedly between update cycles. By default, Deephaven throws an error in these cases.
2. The rows in ring tables are not guaranteed to maintain their original order of arrival. Operations should not assume anything about the order of data in a ring table.
3. Ring tables _will not_ grow without bound and are strictly limited to a maximum number of rows. Once that limit is reached, the oldest rows are discarded and deleted from memory.

Ring tables are semantically the same as standard tables, and they do not get specialized aggregation semantics like blink tables do. However, operations use less memory because ring tables dispose of old data.

### Create a ring table from a blink table

It is common to create a ring table from a blink table to preserve _some_ data history, _but not all_. Use [`ring_table`](../reference/table-operations/create/ringTable.md) to do this.

The following example creates a ring table that holds the five most recent observations from a blink table:

```python ticking-table order=null
from deephaven import time_table, ring_table

t = time_table("PT0.5s", blink_table=True)

# get ring table from t that holds last five rows
t_ring = ring_table(parent=t, capacity=5)
```

![Two Deephaven tables - a time table 't', and a 5-row ring table, 't_ring'](../assets/conceptual/table-types/table-types-6.gif)

### Create a ring table from an append-only table

> [!CAUTION]
> Creating a ring table from an append-only table does not give the memory savings that ring tables are useful for.

Ring tables can also be created from append-only tables using [`ring_table`](../reference/table-operations/create/ringTable.md). This is a less common use case because the typical memory savings that ring tables afford is lost. If there is an append-only table anywhere in the equation, it can grow until it eats up all available memory. A downstream ring table will only _appear_ to save on memory, and is effectively equivalent to applying a [`tail`](../reference/table-operations/filter/tail.md) operation to an append-only table.

This example creates a ring table with a 5-row capacity from a simple append-only time table:

```python ticking-table order=null
from deephaven import time_table, ring_table

# t is an append-only table
t = time_table("PT0.5s")

# get ring table from t that holds last five rows
t_ring = ring_table(parent=t, capacity=5)
```

![An append-only time table and a 5-row ring table](../assets/conceptual/table-types/table-types-7.gif)

If the source append-only table _already_ has rows in it when [`ring_table`](../reference/table-operations/create/ringTable.md) is called, the resulting ring table will include those rows by default:

```python test-set=5 ticking-table order=null
from deephaven import empty_table, time_table, ring_table, merge

# create append-only table that starts with five rows
t_static = empty_table(5).update("X = ii")
t_dynamic = time_table("PT1s").update("X = ii + 5").drop_columns("Timestamp")
t = merge([t_static, t_dynamic])

# get ring table from t that holds last ten rows
t_ring_with_initial = ring_table(parent=t, capacity=10)
```

![A ring table initializes with the rows that are already present in the source append-only table](../assets/conceptual/table-types/table-types-8.gif)

To disable this behavior, set `initialize = False`:

```python test-set=5 ticking-table order=null
t_ring_without_initial = ring_table(parent=t, capacity=10, initialize=False)
```

![When `initialize=False`, a ring table does not initialize with data that existed in the source table before the ring table was created](../assets/conceptual/table-types/table-types-9.gif)

## Related documentation

- [How to use a TablePublisher](../how-to-guides/table-publisher.md)
- [Create a time table](../how-to-guides/time-table.md)
- [Kafka basic terminology](./kafka-in-deephaven.md)
