---
title: Data indexes
---

> [!IMPORTANT]
> This feature is experimental. The API and performance characteristics are subject to change.

This guide covers what data indexes are, how to create and use them, and how they can improve query performance.

A data index maps key values in one or more key columns to the [row sets](../conceptual/table-update-model.md#describing-table-updates) that contain them. Every data index is backed by a table that stores the row sets in a column called `dh_row_set`. Many table operations use a data index if one is present. Data indexes help in two ways:

- They let [filters](./use-filters.md), [exact and relational joins](./joins-exact-relational.md), [as-of and range joins](./joins-timeseries-range.md), [sorts](./sort.md), and [aggregations](../conceptual/aggregations.md) look up each key's rows in the index instead of reading the key columns of every row.
- They let several operations share the work of grouping a table's rows by key. Without an index, each [join](#join) or [aggregation](../conceptual/aggregations.md) on a table repeats that grouping. With a data index, those operations can start from the index instead, which can make them faster and use less memory.

Data indexes work on ticking data too. On a [refreshing](../conceptual/table-types.md) table, the index updates as rows tick, so operations that use it stay current.

An index can also make a query slower. See [Performance](#performance) for when an index helps.

## Create a data index

Create a data index from a source table and one or more key columns with [`data_index`](../reference/engine/data-index.md):

```python test-set=1 order=source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = i")
source_index = data_index(source, "X")
```

> [!WARNING]
> A data index you create with [`data_index`](../reference/engine/data-index.md) is [weakly reachable](https://en.wikipedia.org/wiki/Weak_reference). Assign the index to a variable to ensure it does not get garbage collected.

> [!IMPORTANT]
> Creating a data index doesn't compute it. Deephaven computes the index the first time one of these happens:
>
> - A table operation that can use the index looks at it. [Joins](#join) and [sorts](#sort) compute the index to check its size, even when they then don't use it.
> - You access the index's [`table`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex.table) property.
> - You [write the table to Parquet](#persist-data-indexes-with-parquet) with that index.
>
> [`where`](../reference/table-operations/filter/where.md) never computes an index you create with [`data_index`](../reference/engine/data-index.md). See [Filter](#filter).
>
> The first operation that computes an index pays for it, even if it then reads the table directly. This is an important detail when you measure performance.

### View an index's contents

To see an index's contents, retrieve its backing table with the [`table`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex.table) property:

```python test-set=1 order=result
result = source_index.table
```

The `source` table in the above example doesn't create a very interesting data index. Each key value in `X` only appears once. The following example better illustrates a data index:

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = 100 + i % 5")
index = data_index(source, "X")
source_index = index.table
```

When looking at `source`, we can see that the value `100` in column `X` appears in rows 0 and 5. The value `101` appears in rows 1 and 6, `102` in rows 2 and 7, and so on. Each value in `dh_row_set` holds the rows where that row's key value appears. Let's look at an example for more than one key column.

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(20).update(["X = i % 4", "Y = i % 2"])
index = data_index(source, ["X", "Y"])
source_index = index.table
```

There are only four unique keys in `X` and `Y` together. The resultant column `dh_row_set` shows the row sets in which each unique key appears.

In the previous examples, the key values repeat in a fixed pattern, so the row sets do too: key `100` appears in every fifth row. Key values in real data rarely follow a pattern like this, so don't assume that the rows for a key are evenly spaced or next to each other. In the following example, each row gets a random key:

```python order=result,source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(25).update(
    ["Key = 100 + randomInt(0, 4)", "Value = randomDouble(0, 100)"]
)

index = data_index(source, "Key")
source_index = index.table

result = source_index.where("Key in 100, 102")
```

Each row set in `source_index` now holds an irregular set of row keys, which change every time the example runs. Because the index is backed by an ordinary table, you can query it like any other table. In this example, `result` [filters](./use-filters.md) it to show only the row sets for keys `100` and `102`.

## Retrieve and verify a data index

You can check for or retrieve a data index with two functions:

- [`has_data_index`](../reference/engine/has-data-index.md): Returns a boolean indicating whether a data index exists for the table and the given key column(s).
- [`data_index`](../reference/engine/data-index.md): When you pass `create_if_absent=False`, returns the existing data index, or `None` if no usable index exists. It doesn't create an index.

The following example creates a data index on `Key1` and `Key3`, then uses [`has_data_index`](../reference/engine/has-data-index.md) to check which key column combinations have an index. Finally, it tries to retrieve an index on `Key2` without creating one. No such index exists, so the result is `None`.

```python order=:log,source
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii % 8",
        "Key2 = ii % 11",
        "Key3 = ii % 19",
        "Value = -ii",
    ]
)

index_1_3 = data_index(source, ["Key1", "Key3"])

has_index_1 = has_data_index(source, ["Key1"])
has_index_2_3 = has_data_index(source, ["Key2", "Key3"])
has_index_1_3 = has_data_index(source, ["Key1", "Key3"])

print(f"There is a data index for source on Key 1: {has_index_1}")
print(f"There is a data index for source on Keys 2 and 3: {has_index_2_3}")
print(f"There is a data index for source on Keys 1 and 3: {has_index_1_3}")

index_2 = data_index(source, ["Key2"], create_if_absent=False)

# Retrieving without creating returns None, and no index is created
print(f"Retrieved index for Key 2: {index_2}")

has_index_2 = has_data_index(source, ["Key2"])

print(f"There is a data index for source on Key 2: {has_index_2}")
```

## Usage in queries

You don't pass a data index to a table operation. If a table has a data index that an operation can use, the engine finds it and uses it, except in the cases listed under [Performance](#performance), where reading the table directly is faster. The following table operations can use data indexes:

- Filter:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`where_in`](../reference/table-operations/filter/where-in.md)
  - [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- Join:
  - [`exact_join`](../reference/table-operations/join/exact-join.md)
  - [`natural_join`](../reference/table-operations/join/natural-join.md)
  - [`aj`](../reference/table-operations/join/aj.md)
  - [`raj`](../reference/table-operations/join/raj.md)
  - [`range_join`](../reference/table-operations/join/range-join.md)
  - [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- Sort:
  - [`sort`](../reference/table-operations/sort/sort.md)
  - [`sort_descending`](../reference/table-operations/sort/sort-descending.md)
- Group and aggregate:
  - [Dedicated aggregations](./dedicated-aggregations.md) such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) and [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
  - [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
  - [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
  - [`partitioned_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- Update by:
  - [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- Select:
  - [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- Hierarchical tables:
  - [`rollup`](../reference/table-operations/create/rollup.md)
  - [`tree`](../reference/table-operations/create/tree.md)

> [!NOTE]
> Whether an operation can use a [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) depends on the index's key columns:
>
> - [Joins that can use an index](#join), [aggregations](../conceptual/aggregations.md), and [`select_distinct`](../reference/table-operations/select/select-distinct.md) use an index only when its key columns exactly match the operation's key columns. For as-of joins and [`range_join`](../reference/table-operations/join/range-join.md), the index must match the join's exact-match columns. For example, an [aggregation](../conceptual/aggregations.md) by column `X` does not use an index on columns `X` and `Y`.
> - [`where`](../reference/table-operations/filter/where.md) can use an index whose key columns include the filtered column.
> - [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) can use an index on all or some of their key columns.
> - [Sorts](./sort.md) can use an index on all of the sort columns, or a single-column index on the first sort column.
>
> The sections below give details for [filters](./use-filters.md), [joins](#join), [sorts](#sort), and [aggregations](#aggregate).

The code blocks in the following subsections use these tables:

```python test-set=2 order=source,source_right,source_right_distinct
from deephaven.experimental.data_index import data_index
from deephaven import empty_table
from deephaven import agg

source = empty_table(100_000).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii % 11",
        "Key2 = ii % 47",
        "Key3 = ii % 499",
        "Value = ii",
    ]
)

source_right = empty_table(400).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii % 11",
        "Key2 = ii % 43",
        "Value = ii/2",
    ]
)

source_right_distinct = source_right.select_distinct(["Key1", "Key2"])
```

### Filter

The engine uses single- and multi-column indexes to speed up most [filter](./use-filters.md) types, not only exact matches, through the [predicate pushdown](./predicate-pushdown.md) framework. Two rules decide whether [`where`](../reference/table-operations/filter/where.md) uses an index:

- The index's key columns must include the filtered column. For example, an index on `Key1` and `Key2` can speed up a filter on `Key1`.
- On average, each key value must appear in many of the rows being filtered. [Predicate pushdown](./predicate-pushdown.md) gives the exact threshold.

How [`where`](../reference/table-operations/filter/where.md) uses a data index depends on the kind of index:

- A table-level data index covers the whole table. You create one with [`data_index`](../reference/engine/data-index.md), and Deephaven registers one when it [reads saved indexes from Parquet](#persist-data-indexes-with-parquet). [`where`](../reference/table-operations/filter/where.md) never computes a table-level index. It uses one only after one of these computes it:
  - Another operation that can use the index, such as a [join](#join), [sort](#sort), [aggregation](#aggregate), or [`where_in`](../reference/table-operations/filter/where-in.md).
  - Reading the index's [`table`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex.table) property.
- A file-level data index is one stored on disk with the data, such as an index [written with Parquet](#persist-data-indexes-with-parquet). Nothing has to compute it first. The engine uses it to filter that file without loading the entire index into memory.

> [!NOTE]
> Support for using data indexes with most filter types, beyond exact matches, came with the [predicate pushdown](./predicate-pushdown.md) framework. File-level data indexes support this starting in Deephaven 0.40.0, and table-level data indexes starting in Deephaven 41.0.

The following filters can use the index created at the top of the code block. The example accesses the index's [`table`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex.table) property before filtering:

```python test-set=2 order=result_k1,result_k1_in
source_k1_index = data_index(source, ["Key1"])
# where only uses an index whose table is already computed, so compute it now
source_k1_index.table

result_k1 = source.where("Key1 = 7")
result_k1_in = source.where("Key1 in 2,4,6,8")
```

The results are the same with or without the index. The index changes only how the engine finds the matching rows.

The following example can only use the index for the first filter:

```python test-set=2 order=result_k1_k2
source_k1_index = data_index(source, ["Key1"])
source_k1_index.table

result_k1_k2 = source.where(["Key1 = 7", "Key2 = 11"])
```

When you filter one table by another with [`where_in`](../reference/table-operations/filter/where-in.md) or [`where_not_in`](../reference/table-operations/filter/where-not-in.md), the engine looks for a data index on the table being filtered. It prefers an index on all of the key columns, but it also uses an index on some of them. Unlike [`where`](../reference/table-operations/filter/where.md), these methods compute the index if it hasn't been computed yet. Like [`where`](../reference/table-operations/filter/where.md), they use the index only when, on average, each key value appears in many of the rows being filtered. Both filters in the following example can use `source_k1_k2_index`:

```python test-set=2 order=result_where_in,result_where_not_in
source_k1_k2_index = data_index(source, ["Key1", "Key2"])

result_where_in = source.where_in(source_right_distinct, cols=["Key1", "Key2"])
result_where_not_in = source.where_not_in(source_right_distinct, cols=["Key1", "Key2"])
```

### Join

Each method for [exact and relational joins](./joins-exact-relational.md) and [as-of and range joins](./joins-timeseries-range.md) can use a left table index, a right table index, both, or neither:

| Method                                                                     | Can use indexes?   | Can use left table index? | Can use right table index? |
| -------------------------------------------------------------------------- | ------------------ | ------------------------- | -------------------------- |
| [`exact_join`](../reference/table-operations/join/exact-join.md)           | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`natural_join`](../reference/table-operations/join/natural-join.md)       | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`aj`](../reference/table-operations/join/aj.md)                           | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`raj`](../reference/table-operations/join/raj.md)                         | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`join`](../reference/table-operations/join/join.md)                       | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`multi_join`](../reference/table-operations/join/multi-join.md)           | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`range_join`](../reference/table-operations/join/range-join.md)           | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |

[`full_outer_join`](../reference/table-operations/join/full-outer-join.md) starts with a [`left_outer_join`](../reference/table-operations/join/left-outer-join.md), which can't use indexes. It uses indexes only through the two other operations it is built from:

- A [`select_distinct`](../reference/table-operations/select/select-distinct.md) on the left table.
- A [`natural_join`](../reference/table-operations/join/natural-join.md) with the right table as its left table and those distinct keys as its right table.

The comparison table marks [`natural_join`](../reference/table-operations/join/natural-join.md) as unable to use a right table index because it never reads the right table's rows through one. [Natural join](#natural-join) describes the one case where it looks at a right table index.

#### Natural join

Without data indexes, a [`natural_join`](../reference/table-operations/join/natural-join.md) reads the key values of every row in the left table and hashes them to match each row with the right table. If the left table has a data index on the join keys, the engine can read the index instead. The index has one row per unique key, so reading it can be faster than scanning every row of the left table.

Whether the engine uses each index depends on whether the tables are [refreshing](../conceptual/table-types.md), on the sizes of the tables and the index, and on the join's `type`, a [`NaturalJoinType`](/core/pydoc/code/deephaven.table.html#deephaven.table.NaturalJoinType):

- **Left table index:** If either table is refreshing, the engine uses it. If both tables are [static](../conceptual/table-types.md) and `type` is `ERROR_ON_DUPLICATE` (the default) or `EXACTLY_ONE_MATCH`, the engine uses it only if the index has no more rows than the right table.
- **Right table index:** With the default `type` or `EXACTLY_ONE_MATCH`, the engine never uses it. With `FIRST_MATCH` or `LAST_MATCH`, the engine reads it only to size its hash table and to decide whether to build from the left table's index. It still reads the right table's rows directly.

The following example creates an index on `Key1` and `Key2` for both `source` and `source_right`, then joins the tables with [`natural_join`](../reference/table-operations/join/natural-join.md). The engine decides whether to use each index as follows:

- The join uses the default `type`, so the engine ignores the right table's index, `source_right_k1_k2_index`.
- The left table's index, `source_k1_k2_index`, matches the join keys, so the engine checks its size. Checking the size computes the index if nothing has computed it yet.
- `source_k1_k2_index` has 517 rows and `source_right` has 400. Both tables are static, so the join reads `source` directly instead of using the index.

```python test-set=2 order=result_natural_joined
source_k1_k2_index = data_index(source, ["Key1", "Key2"])
source_right_k1_k2_index = data_index(source_right, ["Key1", "Key2"])

result_natural_joined = source.natural_join(
    source_right, on=["Key1", "Key2"], joins=["RhsValue=Value"]
)
```

#### As-of join

Data indexes can speed up the exact match in an [`aj`](../reference/table-operations/join/aj.md) or an [`raj`](../reference/table-operations/join/raj.md), because the engine no longer has to group each table's rows by key. The following example can use both indexes, because their key column matches the join's exact-match column:

```python test-set=2 order=result_as_of_joined
source_k1_index = data_index(source, ["Key1"])
source_right_k1_index = data_index(source_right, ["Key1"])

result_as_of_joined = source.aj(
    source_right,
    on=["Key1", "Timestamp >= Timestamp"],
    joins=["RhsValue=Value", "RhsTimestamp=Timestamp"],
)
```

### Sort

[Sort](../reference/table-operations/sort/sort.md) operations can use both full and partial indexes.

- A full index matches all sort columns, in any order.
- A partial index is a single-column index on the first sort column.

When a full index is present, the engine only needs to sort the index rows. Two limits apply:

- The engine skips a full index that has at least half as many rows as the table being sorted, because sorting the table directly is faster. It then sorts without any index, even if a single-column index on the first sort column exists.
- On a refreshing table, the index speeds up only the initial sort. The engine sorts later updates without it.

When a [sort](../reference/table-operations/sort/sort.md) operation uses a partial index, the engine sorts the first column using the index and sorts the remaining columns without one. The engine doesn't use a multi-column index that covers only some of the sort columns.

The following [`sort_descending`](../reference/table-operations/sort/sort-descending.md) operation uses the index because the index's key columns match the sort columns, even though the operation lists them in a different order:

```python test-set=2 order=result_sorted_k2_k1
source_k1_k2_index = data_index(source, ["Key1", "Key2"])

result_sorted_k2_k1 = source.sort_descending(order_by=["Key2", "Key1"])
```

The following [sort](../reference/table-operations/sort/sort.md) operation uses the index when sorting the first key column only:

```python test-set=2 order=result_sorted_k1_k3
source_k1_index = data_index(source, ["Key1"])

result_sorted_k1_k3 = source.sort(order_by=["Key1", "Key3"])
```

The following [sort](../reference/table-operations/sort/sort.md) operation does not use any index, because no index on `source` covers `Key3`, the first sort column:

```python test-set=2 order=result_sorted_k3_k1
source_k1_index = data_index(source, ["Key1"])

result_sorted_k3_k1 = source.sort(order_by=["Key3", "Key1"])
```

### Aggregate

Without a data index, an [aggregation](../conceptual/aggregations.md) must first group rows by their key values and then calculate on each group. The index lets the engine skip the grouping step and start calculating right away. The following [aggregation](../conceptual/aggregations.md) uses the index because the index's key columns match the aggregation's key columns:

```python test-set=2 order=result_aggregated
source_k1_k2_index = data_index(source, ["Key1", "Key2"])

result_aggregated = source.agg_by(
    [
        agg.sum_(cols=["Sum=Value"]),
        agg.avg(cols=["Avg=Value"]),
        agg.std(cols=["Std=Value"]),
    ],
    by=["Key1", "Key2"],
)
```

## Data index inheritance

A table derived from an indexed table inherits its data indexes unless the operation changes the row set or any indexed data.

A table you [subscribe to from another worker](./use-uris.md) does not inherit that worker's indexes. To still benefit from an index, [filter](./use-filters.md) the table on the worker that publishes it, then subscribe to the filtered result.

## Persist data indexes with Parquet

Deephaven can save data indexes along with your data when you [write a table to Parquet files](./data-import-export/parquet-export.md). By default, [`write`](../reference/data-import-export/Parquet/writeTable.md) saves every index on the table. [Export to Parquet](./data-import-export/parquet-export.md) lists the defaults of the other write functions. To save only some indexes, pass their key columns in the `index_columns` argument. If you name an index that doesn't exist yet, Deephaven creates a temporary one just for the write.

When you [load static data from a Parquet file](./data-import-export/parquet-import.md), Deephaven also finds any saved indexes and registers them on the table. It reads an index from disk only when an operation uses it.

The following example writes a table to Parquet twice and reads it back after each write:

- The first write names the index key columns. `source` has no indexes yet, so Deephaven creates them temporarily for the write.
- The second write names no index columns, so Deephaven saves every index that exists on `source`.

```python order=:log
from deephaven import empty_table
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven.parquet import read, write

source = empty_table(100_000).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii % 11",
        "Key2 = ii % 47",
        "Key3 = ii % 499",
        "Value = ii",
    ]
)

# Write to disk and specify two sets of index key columns
write(source, path="/data/indexed.parquet", index_columns=[["Key1"], ["Key1", "Key2"]])

# Read the table; its saved indexes are registered but not loaded yet
disk_table = read("/data/indexed.parquet")

# Show that the table loaded from disk has indexes
print(has_data_index(disk_table, ["Key1"]))
print(has_data_index(disk_table, ["Key1", "Key2"]))

# Create two indexes on source
index_key1 = data_index(source, ["Key1"])
index_key1_key2 = data_index(source, ["Key1", "Key2"])

# Write to disk without naming index columns, which saves every index on source
write(source, path="/data/indexed_all.parquet")

# Read the table; its saved indexes are registered but not loaded yet
disk_table_new = read("/data/indexed_all.parquet")

# Show that the table loaded from disk has indexes
print(has_data_index(disk_table_new, ["Key1"]))
print(has_data_index(disk_table_new, ["Key1", "Key2"]))
```

## Partitioning columns as data indexes

A partitioning column is a column whose values come from the directory layout of a partitioned dataset, such as the `Date=2024-01-01` directories in a [key-value partitioned Parquet directory](./data-import-export/parquet-formats.md#key-value-partitioned-directories). Deephaven automatically creates a data index for each partitioning column. The engine first uses partitioning column values to choose which files to read. After that, it uses the partitioning column indexes like any other data index in query operations.

> [!WARNING]
> Do not create data indexes on partitioning or grouping columns. Deephaven automatically manages indexes for these columns internally. Adding a manual data index can interfere with this process and cause unexpected behavior. For example, passing a partitioning column by itself as an index in `index_columns` when you write a key-value partitioned Parquet directory raises an error. Calling [`data_index`](../reference/engine/data-index.md) on a partitioning column returns the index Deephaven already created.

## Performance

Data indexes improve performance when used in the right context. If used in the wrong context, data indexes can make performance worse. Use the following guidelines to decide whether an index helps:

- A data index is more likely to help performance if the rows that share a key value are stored near each other in memory. This avoids index fragmentation and poor data access patterns.
- Reusing a data index multiple times is necessary to overcome the cost of initially computing the index.
- Generally speaking, the higher the key cardinality, the better the performance improvement. Key cardinality is the number of distinct key values.

Even when a usable index exists, the engine skips it in these cases:

- [`where`](../reference/table-operations/filter/where.md), [`where_in`](../reference/table-operations/filter/where-in.md), and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) use an index only when, on average, each key value appears in many of the rows being filtered. See [Predicate pushdown](./predicate-pushdown.md) for details.
- A [sort](#sort) skips an index on all of the sort columns when the index has at least half as many rows as the table.
- A [`natural_join`](../reference/table-operations/join/natural-join.md) of two static tables with the default `type` or `EXACTLY_ONE_MATCH` uses the left table's index only if the index has no more rows than the right table.

### Benchmark scripts

If you're interested in how data indexes affect query performance, the scripts below measure each operation with and without indexes. You'll notice that performance improves in some cases and gets worse in others.

This first script sets up the tests:

```python skip-test
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven import empty_table
from deephaven.table import Table
from typing import Any, Callable
from time import time
import jpy

# Disable memoization for perf testing
jpy.get_type("io.deephaven.engine.table.impl.QueryTable").setMemoizeResults(False)


def time_it(name: str, f: Callable[[], Any]) -> Any:
    """
    Time a function execution.
    """
    start = time()
    rst = f()
    end = time()
    exec_time = end - start
    print(f"{name} took {(exec_time):.4f} seconds.")
    return rst, exec_time


def add_index(t: Table, by: list[str]):
    """
    Add a data index.
    Accessing .table computes the index now instead of when an operation first uses it.

    Args:
    - t: Table to add the index to.
    - by: Columns to index on.

    Returns:
    - The index.
    """
    print(f"Adding data index: {by}")

    idx = data_index(t, by)
    # Access .table to compute the index now, so the benchmark times index creation
    # separately from the operations that use it. Not for production code.
    idx.table
    return idx


def run_test(n_i_keys: int, n_j_keys: int, create_idxs: list[bool]) -> list[float]:
    """
    Run a series of performance benchmark tests.

    Args:
    - n_i_keys: Number of unique keys for column I.
    - n_j_keys: Number of unique keys for column J.
    - create_idxs: List of booleans to determine which indexes to create (I, J, and [I, J])
    """
    t = empty_table(100_000_000).update(
        ["I = ii % n_i_keys", "J = ii % n_j_keys", "V = random()"]
    )
    t_lastby = t.last_by(["I", "J"])

    if create_idxs[0]:
        idx1_i = add_index(t, ["I"])
    if create_idxs[1]:
        idx1_j = add_index(t, ["J"])
    if create_idxs[2]:
        idx1_ij = add_index(t, ["I", "J"])

    idx_ij = has_data_index(t, ["I", "J"])
    idx_i = has_data_index(t, ["I"])
    idx_j = has_data_index(t, ["J"])
    print(f"Has index: ['I', 'J']={idx_ij} ['I']={idx_i} ['J']={idx_j}")
    ret, t_where = time_it("where", lambda: t.where(["I = 3", "J = 6"]))
    ret, t_countby = time_it("count_by", lambda: t.count_by("Count", ["I", "J"]))
    ret, t_sumby = time_it("sum_by", lambda: t.sum_by(["I", "J"]))
    ret, t_nj = time_it(
        "natural_join", lambda: t.natural_join(t_lastby, ["I", "J"], "VJ = V")
    )

    return [t_where, t_countby, t_sumby, t_nj]
```

Here's a follow-up script that runs each test on low- and high-cardinality data, with and without data indexes. The low-cardinality case has 700 unique `I` and `J` key pairs (100 × 7), and the high-cardinality case has 99,700 (100 × 997):

```python skip-test
from deephaven.column import double_col, string_col
from deephaven import new_table

print("Low cardinality, no data indexes")
times_lc_ni = run_test(n_i_keys=100, n_j_keys=7, create_idxs=[False, False, False])

print("Low cardinality, with data indexes")
times_lc_wi = run_test(n_i_keys=100, n_j_keys=7, create_idxs=[True, True, True])

print("High cardinality, no data indexes")
times_hc_ni = run_test(n_i_keys=100, n_j_keys=997, create_idxs=[False, False, False])

print("High cardinality, with data indexes")
times_hc_wi = run_test(n_i_keys=100, n_j_keys=997, create_idxs=[True, True, True])

results = new_table(
    [
        string_col("Operation", ["where", "count_by", "sum_by", "natural_join"]),
        double_col("TimesLowCardinalityNoIndex", times_lc_ni),
        double_col("TimesLowCardinalityWithIndex", times_lc_wi),
        double_col("TimesHighCardinalityNoIndex", times_hc_ni),
        double_col("TimesHighCardinalityWithIndex", times_hc_wi),
    ]
)
```

The benchmark turns off memoization for the whole server. To turn it back on when you finish, run `jpy.get_type("io.deephaven.engine.table.impl.QueryTable").setMemoizeResults(True)`.

It can also be helpful to plot the results. The following [Deephaven Express](/core/plotly/docs/) bar chart groups the four timings for each operation:

```python skip-test
import deephaven.plot.express as dx

fig = dx.bar(
    results,
    x="Operation",
    y=[
        "TimesLowCardinalityNoIndex",
        "TimesLowCardinalityWithIndex",
        "TimesHighCardinalityNoIndex",
        "TimesHighCardinalityWithIndex",
    ],
    barmode="group",
)
```

For each operation, compare its time with and without indexes at the same cardinality, then check the differences against the guidelines at the start of [Performance](#performance).

## Related documentation

- [Table update model](../conceptual/table-update-model.md)
- [Table types](../conceptual/table-types.md)
- [Aggregations](../conceptual/aggregations.md)
- [Single aggregation](./dedicated-aggregations.md)
- [Multi-aggregation](./combined-aggregations.md)
- [Filter table data](./use-filters.md)
- [Predicate pushdown](./predicate-pushdown.md)
- [Export to Parquet](./data-import-export/parquet-export.md)
- [Import from Parquet](./data-import-export/parquet-import.md)
- [Supported Parquet formats](./data-import-export/parquet-formats.md)
- [Joins: exact and relational](./joins-exact-relational.md)
- [Joins: inexact and range](./joins-timeseries-range.md)
- [Sort table data](./sort.md)
- [Use URIs to share tables](./use-uris.md)
- [Deephaven Express](/core/plotly/docs/)
- [`data_index`](../reference/engine/data-index.md)
- [`has_data_index`](../reference/engine/has-data-index.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`exact_join`](../reference/table-operations/join/exact-join.md)
- [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`partitioned_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`range_join`](../reference/table-operations/join/range-join.md)
- [`read`](../reference/data-import-export/Parquet/readTable.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`sort_descending`](../reference/table-operations/sort/sort-descending.md)
- [`std`](../reference/table-operations/group-and-aggregate/AggStd.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- [`write`](../reference/data-import-export/Parquet/writeTable.md)
- [`DataIndex` Pydoc](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex)
