---
title: Data indexes
---

This guide covers what data indexes are, how to create and use them, and how they can improve query performance.

A data index maps key values in one or more key columns to the [row sets](../conceptual/table-update-model.md#describing-table-updates) that contain them. Every data index is backed by a table that stores the row sets in a column called `dh_row_set`. Many table operations use a data index if one is present. Data indexes help in two ways:

- They let [filters](./use-filters.md), [exact and relational joins](./joins-exact-relational.md), [as-of and range joins](./joins-timeseries-range.md), [sorts](./sort.md), and [aggregations](../conceptual/aggregations.md) look up each key's rows in the index instead of reading the key columns of every row.
- They let several operations share the work of grouping a table's rows by key. Without an index, each [join](#join) or [aggregation](../conceptual/aggregations.md) on a table repeats that grouping. With a data index, those operations can start from the index instead, which can make them faster and use less memory.

Data indexes work on ticking data too. On a [refreshing](../conceptual/table-types.md) table, the index updates as rows tick, so operations that use it stay current.

An index can also make a query slower. See [Performance](#performance) for when an index helps.

## Create a data index

Create a data index from a source table and one or more key columns with [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md):

```groovy test-set=1 order=source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(10).update("X = i")
sourceIndex = getOrCreateDataIndex(source, "X")
```

> [!WARNING]
> A data index you create with [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md) is [weakly reachable](https://en.wikipedia.org/wiki/Weak_reference). Assign the index to a variable to ensure it does not get garbage collected.

> [!IMPORTANT]
> Creating a data index doesn't compute it. Deephaven computes the index the first time one of these happens:
>
> - A table operation that can use the index looks at it. [Joins](#join) and [sorts](#sort) compute the index to check its size, even when they then don't use it.
> - You call the index's [`table`](/core/javadoc/io/deephaven/engine/table/BasicDataIndex.html#table()) method.
> - You [write the table to Parquet](#persist-data-indexes-with-parquet) with that index.
>
> [`where`](../reference/table-operations/filter/where.md) never computes an index you create with [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md). See [Filter](#filter).
>
> The first operation that computes an index pays for it, even if it then reads the table directly. This is an important detail when you measure performance.

### View an index's contents

To see an index's contents, retrieve its backing table with the [`table`](/core/javadoc/io/deephaven/engine/table/BasicDataIndex.html#table()) method:

```groovy test-set=1 order=result
result = sourceIndex.table()
```

The `source` table in the above example doesn't create a very interesting data index. Each key value in `X` only appears once. The following example better illustrates a data index:

```groovy order=sourceIndex,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(10).update("X = 100 + i % 5")
index = getOrCreateDataIndex(source, "X")
sourceIndex = index.table()
```

When looking at `source`, we can see that the value `100` in column `X` appears in rows 0 and 5. The value `101` appears in rows 1 and 6, `102` in rows 2 and 7, and so on. Each value in `dh_row_set` holds the rows where that row's key value appears. Let's look at an example for more than one key column.

```groovy order=sourceIndex,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(20).update("X = i % 4", "Y = i % 2")
index = getOrCreateDataIndex(source, "X", "Y")
sourceIndex = index.table()
```

There are only four unique keys in `X` and `Y` together. The resultant column `dh_row_set` shows the row sets in which each unique key appears.

In the previous examples, the key values repeat in a fixed pattern, so the row sets do too: key `100` appears in every fifth row. Key values in real data rarely follow a pattern like this, so don't assume that the rows for a key are evenly spaced or next to each other. In the following example, each row gets a random key:

```groovy order=result,sourceIndex,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(25).update("Key = 100 + randomInt(0, 4)", "Value = randomDouble(0, 100)")

index = getOrCreateDataIndex(source, "Key")
sourceIndex = index.table()

result = sourceIndex.where("Key in 100, 102")
```

Each row set in `sourceIndex` now holds an irregular set of row keys, which change every time the example runs. Because the index is backed by an ordinary table, you can query it like any other table. In this example, `result` [filters](./use-filters.md) it to show only the row sets for keys `100` and `102`.

## Retrieve and verify a data index

You can check for or retrieve a data index with two methods:

- [`hasDataIndex`](../reference/engine/hasDataIndex.md): Returns a boolean indicating whether a data index exists for the table and the given key column(s).
- [`getDataIndex`](../reference/engine/getDataIndex.md): Returns the existing data index for the table and key column(s). Returns `null` if no index exists, if the cached index is invalid, or if a refreshing cached index is no longer live. It never creates an index.

The following example creates a data index on `Key1` and `Key3`, then uses [`hasDataIndex`](../reference/engine/hasDataIndex.md) to check which key column combinations have an index. Finally, it tries to retrieve an index on `Key2` without creating one. No such index exists, so the result is `null`.

```groovy order=:log,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(100).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 8",
    "Key2 = ii % 11",
    "Key3 = ii % 19",
    "Value = -ii"
)

index13 = getOrCreateDataIndex(source, "Key1", "Key3")

hasIndex1 = hasDataIndex(source, "Key1")
hasIndex23 = hasDataIndex(source, "Key2", "Key3")
hasIndex13 = hasDataIndex(source, "Key1", "Key3")

println "There is a data index for source on Key 1: ${hasIndex1}"
println "There is a data index for source on Keys 2 and 3: ${hasIndex23}"
println "There is a data index for source on Keys 1 and 3: ${hasIndex13}"

index2 = getDataIndex(source, "Key2")

// getDataIndex returns null and does not create an index
println "Retrieved index for Key 2: ${index2}"

hasIndex2 = hasDataIndex(source, "Key2")

println "There is a data index for source on Key 2: ${hasIndex2}"
```

## Usage in queries

You don't pass a data index to a table operation. If a table has a data index that an operation can use, the engine finds it and uses it, except in the cases listed under [Performance](#performance), where reading the table directly is faster. The following table operations can use data indexes:

- Filter:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- Join:
  - [`exactJoin`](../reference/table-operations/join/exact-join.md)
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md)
  - [`aj`](../reference/table-operations/join/aj.md)
  - [`raj`](../reference/table-operations/join/raj.md)
  - [`rangeJoin`](../reference/table-operations/join/range-join.md)
  - [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)
- Sort:
  - [`sort`](../reference/table-operations/sort/sort.md)
  - [`sortDescending`](../reference/table-operations/sort/sort-descending.md)
- Group and aggregate:
  - [Dedicated aggregations](./dedicated-aggregations.md) such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) and [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
  - [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
  - [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
  - [`partitionedAggBy`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- Update by:
  - [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- Select:
  - [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- Hierarchical tables:
  - [`rollup`](../reference/table-operations/create/rollup.md)
  - [`tree`](../reference/table-operations/create/tree.md)

> [!NOTE]
> Whether an operation can use a [`DataIndex`](/core/javadoc/io/deephaven/engine/table/DataIndex.html) depends on the index's key columns:
>
> - [Joins that can use an index](#join), [aggregations](../conceptual/aggregations.md), and [`selectDistinct`](../reference/table-operations/select/select-distinct.md) use an index only when its key columns exactly match the operation's key columns. For as-of joins and [`rangeJoin`](../reference/table-operations/join/range-join.md), the index must match the join's exact-match columns. For example, an [aggregation](../conceptual/aggregations.md) by column `X` does not use an index on columns `X` and `Y`.
> - [`where`](../reference/table-operations/filter/where.md) can use an index whose key columns include the filtered column.
> - [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) can use an index on all or some of their key columns.
> - [Sorts](./sort.md) can use an index on all of the sort columns, or a single-column index on the first sort column.
>
> The sections below give details for [filters](./use-filters.md), [joins](#join), [sorts](#sort), and [aggregations](#aggregate).

The code blocks in the following subsections use these tables:

```groovy test-set=2 order=source,sourceRight,sourceRightDistinct
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(100_000).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 11",
    "Key2 = ii % 47",
    "Key3 = ii % 499",
    "Value = ii"
)

sourceRight = emptyTable(400).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 11",
    "Key2 = ii % 43",
    "Value = ii / 2.0"
)

sourceRightDistinct = sourceRight.selectDistinct("Key1", "Key2")
```

### Filter

The engine uses single- and multi-column indexes to speed up most [filter](./use-filters.md) types, not only exact matches, through the [predicate pushdown](./predicate-pushdown.md) framework. Two rules decide whether [`where`](../reference/table-operations/filter/where.md) uses an index:

- The index's key columns must include the filtered column. For example, an index on `Key1` and `Key2` can speed up a filter on `Key1`.
- On average, each key value must appear in many of the rows being filtered. [Predicate pushdown](./predicate-pushdown.md) gives the exact threshold.

How [`where`](../reference/table-operations/filter/where.md) uses a data index depends on the kind of index:

- A table-level data index covers the whole table. You create one with [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md), and Deephaven registers one when it [reads saved indexes from Parquet](#persist-data-indexes-with-parquet). [`where`](../reference/table-operations/filter/where.md) never computes a table-level index. It uses one only after one of these computes it:
  - Another operation that can use the index, such as a [join](#join), [sort](#sort), [aggregation](#aggregate), or [`whereIn`](../reference/table-operations/filter/where-in.md).
  - Calling the index's [`table`](/core/javadoc/io/deephaven/engine/table/BasicDataIndex.html#table()) method.
- A file-level data index is one stored on disk with the data, such as an index [written with Parquet](#persist-data-indexes-with-parquet). Nothing has to compute it first. The engine uses it to filter that file without loading the entire index into memory.

> [!NOTE]
> Support for using data indexes with most filter types, beyond exact matches, came with the [predicate pushdown](./predicate-pushdown.md) framework. File-level data indexes support this starting in Deephaven 0.40.0, and table-level data indexes starting in Deephaven 41.0.

The following filters can use the index created at the top of the code block. The example calls the index's [`table`](/core/javadoc/io/deephaven/engine/table/BasicDataIndex.html#table()) method before filtering:

```groovy test-set=2 order=resultK1,resultK1In
sourceK1Index = getOrCreateDataIndex(source, "Key1")
// where only uses an index whose table is already computed, so compute it now
sourceK1Index.table()

resultK1 = source.where("Key1 = 7")
resultK1In = source.where("Key1 in 2,4,6,8")
```

The results are the same with or without the index. The index changes only how the engine finds the matching rows.

The following example can only use the index for the first filter:

```groovy test-set=2 order=resultK1K2
sourceK1Index = getOrCreateDataIndex(source, "Key1")
sourceK1Index.table()

resultK1K2 = source.where("Key1 = 7", "Key2 = 11")
```

When you filter one table by another with [`whereIn`](../reference/table-operations/filter/where-in.md) or [`whereNotIn`](../reference/table-operations/filter/where-not-in.md), the engine looks for a data index on the table being filtered. It prefers an index on all of the key columns, but it also uses an index on some of them. Unlike [`where`](../reference/table-operations/filter/where.md), these methods compute the index if it hasn't been computed yet. Like [`where`](../reference/table-operations/filter/where.md), they use the index only when, on average, each key value appears in many of the rows being filtered. Both filters in the following example can use `sourceK1K2Index`:

```groovy test-set=2 order=resultWhereIn,resultWhereNotIn
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")

resultWhereIn = source.whereIn(sourceRightDistinct, "Key1", "Key2")
resultWhereNotIn = source.whereNotIn(sourceRightDistinct, "Key1", "Key2")
```

### Join

Each method for [exact and relational joins](./joins-exact-relational.md) and [as-of and range joins](./joins-timeseries-range.md) can use a left table index, a right table index, both, or neither:

| Method                                                                   | Can use indexes?   | Can use left table index? | Can use right table index? |
| ------------------------------------------------------------------------ | ------------------ | ------------------------- | -------------------------- |
| [`exactJoin`](../reference/table-operations/join/exact-join.md)          | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`naturalJoin`](../reference/table-operations/join/natural-join.md)      | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`aj`](../reference/table-operations/join/aj.md)                         | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`raj`](../reference/table-operations/join/raj.md)                       | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`join`](../reference/table-operations/join/join.md)                     | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`multiJoin`](../reference/table-operations/join/multi-join.md)          | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`rangeJoin`](../reference/table-operations/join/range-join.md)          | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |

[`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) starts with a [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md), which can't use indexes. It uses indexes only through the two other operations it is built from:

- A [`selectDistinct`](../reference/table-operations/select/select-distinct.md) on the left table.
- A [`naturalJoin`](../reference/table-operations/join/natural-join.md) with the right table as its left table and those distinct keys as its right table.

The comparison table marks [`naturalJoin`](../reference/table-operations/join/natural-join.md) as unable to use a right table index because it never reads the right table's rows through one. [Natural join](#natural-join) describes the one case where it looks at a right table index.

#### Natural join

Without data indexes, a [`naturalJoin`](../reference/table-operations/join/natural-join.md) reads the key values of every row in the left table and hashes them to match each row with the right table. If the left table has a data index on the join keys, the engine can read the index instead. The index has one row per unique key, so reading it can be faster than scanning every row of the left table.

Whether the engine uses each index depends on whether the tables are [refreshing](../conceptual/table-types.md), on the sizes of the tables and the index, and on the join type, a [`NaturalJoinType`](/core/javadoc/io/deephaven/api/NaturalJoinType.html) argument:

- **Left table index:** If either table is refreshing, the engine uses it. If both tables are [static](../conceptual/table-types.md) and the join type is `ERROR_ON_DUPLICATE` (the default) or `EXACTLY_ONE_MATCH`, the engine uses it only if the index has no more rows than the right table.
- **Right table index:** With the default join type or `EXACTLY_ONE_MATCH`, the engine never uses it. With `FIRST_MATCH` or `LAST_MATCH`, the engine reads it only to size its hash table and to decide whether to build from the left table's index. It still reads the right table's rows directly.

The following example creates an index on `Key1` and `Key2` for both `source` and `sourceRight`, then joins the tables with [`naturalJoin`](../reference/table-operations/join/natural-join.md). The engine decides whether to use each index as follows:

- The join uses the default join type, so the engine ignores the right table's index, `sourceRightK1K2Index`.
- The left table's index, `sourceK1K2Index`, matches the join keys, so the engine checks its size. Checking the size computes the index if nothing has computed it yet.
- `sourceK1K2Index` has 517 rows and `sourceRight` has 400. Both tables are static, so the join reads `source` directly instead of using the index.

```groovy test-set=2 order=resultNaturalJoined
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")
sourceRightK1K2Index = getOrCreateDataIndex(sourceRight, "Key1", "Key2")

resultNaturalJoined = source.naturalJoin(
    sourceRight,
    "Key1, Key2",
    "RhsValue=Value"
)
```

#### As-of join

Data indexes can speed up the exact match in an [`aj`](../reference/table-operations/join/aj.md) or an [`raj`](../reference/table-operations/join/raj.md), because the engine no longer has to group each table's rows by key. The following example can use both indexes, because their key column matches the join's exact-match column:

```groovy test-set=2 order=resultAsOfJoined
sourceK1Index = getOrCreateDataIndex(source, "Key1")
sourceRightK1Index = getOrCreateDataIndex(sourceRight, "Key1")

resultAsOfJoined = source.aj(
    sourceRight,
    "Key1, Timestamp >= Timestamp",
    "RhsValue=Value, RhsTimestamp=Timestamp"
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

The following [`sortDescending`](../reference/table-operations/sort/sort-descending.md) operation uses the index because the index's key columns match the sort columns, even though the operation lists them in a different order:

```groovy test-set=2 order=resultSortedK2K1
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")

resultSortedK2K1 = source.sortDescending("Key2", "Key1")
```

The following [sort](../reference/table-operations/sort/sort.md) operation uses the index when sorting the first key column only:

```groovy test-set=2 order=resultSortedK1K3
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultSortedK1K3 = source.sort("Key1", "Key3")
```

The following [sort](../reference/table-operations/sort/sort.md) operation does not use any index, because no index on `source` covers `Key3`, the first sort column:

```groovy test-set=2 order=resultSortedK3K1
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultSortedK3K1 = source.sort("Key3", "Key1")
```

### Aggregate

Without a data index, an [aggregation](../conceptual/aggregations.md) must first group rows by their key values and then calculate on each group. The index lets the engine skip the grouping step and start calculating right away. The following [aggregation](../conceptual/aggregations.md) uses the index because the index's key columns match the aggregation's key columns:

```groovy test-set=2 order=resultAggregated
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")

aggregations = [
    AggSum("Sum=Value"),
    AggAvg("Avg=Value"),
    AggStd("Std=Value")
]

resultAggregated = source.aggBy(aggregations, "Key1", "Key2")
```

## Data index inheritance

A table derived from an indexed table inherits its data indexes unless the operation changes the row set or any indexed data.

A table you [subscribe to from another worker](./use-uris.md) does not inherit that worker's indexes. To still benefit from an index, [filter](./use-filters.md) the table on the worker that publishes it, then subscribe to the filtered result.

## Persist data indexes with Parquet

Deephaven can save data indexes along with your data when you [write a table to Parquet files](./data-import-export/parquet-export.md). Which indexes it saves by default depends on the method:

- [`writeTable`](../reference/data-import-export/Parquet/writeTable.md) saves all of the table's available indexes.
- [`writeKeyValuePartitionedTable`](../reference/data-import-export/Parquet/writeKeyValuePartitionedTable.md) with a table saves its indexes, except single-column indexes on a [partitioning column](#partitioning-columns-as-data-indexes).
- `writeKeyValuePartitionedTable` with a [partitioned table](./partitioned-tables.md) saves none.

To save only some, pass their key columns with [`ParquetInstructions.Builder.addIndexColumns`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#addIndexColumns(java.lang.String...)). If you name an index that doesn't exist yet, Deephaven creates a temporary index just for the write.

When you [load static data from a Parquet file](./data-import-export/parquet-import.md), Deephaven also finds any saved indexes and registers them on the table. It reads an index from disk only when an operation uses it.

The following example writes a table to Parquet twice and reads it back after each write:

- The first write names the index key columns. `source` has no indexes yet, so Deephaven creates them temporarily for the write.
- The second write names no index columns, so Deephaven saves every index that exists on `source`.

```groovy order=:log
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions

source = emptyTable(100_000).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 11",
    "Key2 = ii % 47",
    "Key3 = ii % 499",
    "Value = ii"
)


// Write to disk and specify two sets of index key columns
writeInstructions = ParquetInstructions.builder().addIndexColumns("Key1").addIndexColumns("Key1", "Key2").build()
ParquetTools.writeTable(source, "/data/indexed.parquet", writeInstructions)

// Read the table; its saved indexes are registered but not loaded yet
diskTable = ParquetTools.readTable("/data/indexed.parquet")

// Show that the table loaded from disk has indexes
println hasDataIndex(diskTable, "Key1")
println hasDataIndex(diskTable, "Key1", "Key2")

// Create two indexes on source
indexKey1 = getOrCreateDataIndex(source, "Key1")
indexKey1Key2 = getOrCreateDataIndex(source, "Key1", "Key2")

// Write to disk without naming index columns, which saves every index on source
ParquetTools.writeTable(source, "/data/indexed_all.parquet")

// Read the table; its saved indexes are registered but not loaded yet
diskTableNew = ParquetTools.readTable("/data/indexed_all.parquet")

// Show that the table loaded from disk has indexes
println hasDataIndex(diskTableNew, "Key1")
println hasDataIndex(diskTableNew, "Key1", "Key2")
```

## Partitioning columns as data indexes

A partitioning column is a column whose values come from the directory layout of a partitioned dataset, such as the `Date=2024-01-01` directories in a [key-value partitioned Parquet directory](./data-import-export/parquet-formats.md#key-value-partitioned-directories). Deephaven automatically creates a data index for each partitioning column. The engine first uses partitioning column values to choose which files to read. After that, it uses the partitioning column indexes like any other data index in query operations.

> [!WARNING]
> Do not create data indexes on partitioning or grouping columns. Deephaven automatically manages indexes for these columns internally. Adding a manual data index can interfere with this process and cause unexpected behavior. For example, passing a partitioning column by itself to [`addIndexColumns`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#addIndexColumns(java.lang.String...)) when you write a key-value partitioned Parquet directory raises an error. Calling [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md) on a partitioning column returns the index Deephaven already created.

## Performance

Data indexes improve performance when used in the right context. If used in the wrong context, data indexes can make performance worse. Use the following guidelines to decide whether an index helps:

- A data index is more likely to help performance if the rows that share a key value are stored near each other in memory. This avoids index fragmentation and poor data access patterns.
- Reusing a data index multiple times is necessary to overcome the cost of initially computing the index.
- Generally speaking, the higher the key cardinality, the better the performance improvement. Key cardinality is the number of distinct key values.

Even when a usable index exists, the engine skips it in these cases:

- [`where`](../reference/table-operations/filter/where.md), [`whereIn`](../reference/table-operations/filter/where-in.md), and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) use an index only when, on average, each key value appears in many of the rows being filtered. See [Predicate pushdown](./predicate-pushdown.md) for details.
- A [sort](#sort) skips an index on all of the sort columns when the index has at least half as many rows as the table.
- A [`naturalJoin`](../reference/table-operations/join/natural-join.md) of two static tables with the default join type or `EXACTLY_ONE_MATCH` uses the left table's index only if the index has no more rows than the right table.

### Benchmark scripts

If you're interested in how data indexes affect query performance, the scripts below measure each operation with and without indexes. You'll notice that performance improves in some cases and gets worse in others.

This first script sets up the tests:

```groovy skip-test
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

import io.deephaven.engine.table.Table
import io.deephaven.engine.table.impl.QueryTable

// Disable memoization for perf testing
QueryTable.setMemoizeResults(false)

timeIt = { String name, Closure<Table> f ->
    final long start = System.nanoTime()
    final Table rst = f.call()
    final long end = System.nanoTime()
    final double execTimeSeconds = (end - start) / 1_000_000_000.0
    println "${name} took ${String.format('%.4f', execTimeSeconds)} seconds."
    return [rst, execTimeSeconds]
}

addIndex = { Table t, List<String> by ->
    println "Adding data index: ${by}"

    final def idx = getOrCreateDataIndex(t, by as String[])
    // Call .table() to compute the index now, so the benchmark times index creation separately from the operations that use it. Not for production code.
    idx.table()
    return idx
}

runTest = { int nIKeys, int nJKeys, List<Boolean> createIdxs ->
    final Table t = emptyTable(100_000_000).update(
        "I = (int)(ii % ${nIKeys})",
        "J = (int)(ii % ${nJKeys})",
        "V = random()"
    )

    final Table tLastBy = t.lastBy("I", "J")

    // Keep a reference to each index so that it is not garbage collected during the tests
    final def indexI = createIdxs[0] ? addIndex(t, ["I"]) : null
    final def indexJ = createIdxs[1] ? addIndex(t, ["J"]) : null
    final def indexIJ = createIdxs[2] ? addIndex(t, ["I", "J"]) : null

    final boolean idxIJ = hasDataIndex(t, "I", "J")
    final boolean idxI = hasDataIndex(t, "I")
    final boolean idxJ = hasDataIndex(t, "J")
    println "Has index: ['I', 'J']=${idxIJ} ['I']=${idxI} ['J']=${idxJ}"

    def (ret, tWhere) = timeIt("where", { t.where("I = 3", "J = 6") })
    def (ret2, tCountBy) = timeIt("countBy", { t.countBy("Count", "I", "J") })
    def (ret3, tSumBy) = timeIt("sumBy", { t.sumBy("I", "J") })
    def (ret4, tNj) = timeIt("naturalJoin", { t.naturalJoin(tLastBy, "I, J", "VJ = V") })

    return [tWhere, tCountBy, tSumBy, tNj]
}
```

Here's a follow-up script that runs each test on low- and high-cardinality data, with and without data indexes. The low-cardinality case has 700 unique `I` and `J` key pairs (100 × 7), and the high-cardinality case has 99,700 (100 × 997):

```groovy skip-test
println "Low cardinality, no data indexes"
timesLcNi = runTest(100, 7, [false, false, false])

println "Low cardinality, with data indexes"
timesLcWi = runTest(100, 7, [true, true, true])

println "High cardinality, no data indexes"
timesHcNi = runTest(100, 997, [false, false, false])

println "High cardinality, with data indexes"
timesHcWi = runTest(100, 997, [true, true, true])

println "Low cardinality, no data indexes: ${timesLcNi}"
println "Low cardinality, with data indexes: ${timesLcWi}"
println "High cardinality, no data indexes: ${timesHcNi}"
println "High cardinality, with data indexes: ${timesHcWi}"
```

The benchmark turns off memoization for the whole server. To turn it back on when you finish, run `QueryTable.setMemoizeResults(true)`.

It can also be helpful to plot the results. The following [`catPlotBy`](../reference/plot/catPlotBy.md) chart groups the four timings for each operation:

```groovy skip-test
operations = ["where", "countBy", "sumBy", "naturalJoin"]

scenarioToTimes = [
    "Low cardinality (700 keys), no indexes": timesLcNi,
    "Low cardinality (700 keys), with indexes": timesLcWi,
    "High cardinality (99,700 keys), no indexes": timesHcNi,
    "High cardinality (99,700 keys), with indexes": timesHcWi,
]

operationCol = []
scenarioCol = []
timeSecondsCol = []

scenarioToTimes.each { scenario, times ->
    operations.eachWithIndex { op, i ->
        operationCol << op
        scenarioCol << scenario
        timeSecondsCol << times[i]
    }
}

resultsLong = newTable(
    stringCol("Operation", operationCol as String[]),
    stringCol("Scenario", scenarioCol as String[]),
    doubleCol("TimeSeconds", timeSecondsCol as double[])
)

fig = catPlotBy("Index benchmark times", resultsLong, "Operation", "TimeSeconds", "Scenario").show()
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
- [`getDataIndex`](../reference/engine/getDataIndex.md)
- [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md)
- [`hasDataIndex`](../reference/engine/hasDataIndex.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`catPlotBy`](../reference/plot/catPlotBy.md)
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`doubleCol`](../reference/table-operations/create/doubleCol.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`exactJoin`](../reference/table-operations/join/exact-join.md)
- [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`partitionedAggBy`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)
- [`readTable`](../reference/data-import-export/Parquet/readTable.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`sortDescending`](../reference/table-operations/sort/sort-descending.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- [`writeKeyValuePartitionedTable`](../reference/data-import-export/Parquet/writeKeyValuePartitionedTable.md)
- [`writeTable`](../reference/data-import-export/Parquet/writeTable.md)
- [`DataIndex` Javadoc](/core/javadoc/io/deephaven/engine/table/DataIndex.html)
- [`DataIndexer` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
- [`ParquetInstructions` Javadoc](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html)
