---
title: Data Indexes
---

This guide covers what data indexes are, how to create and use them, and how they can improve query performance. A data index maps key values in one or more key columns to the [row sets](../conceptual/table-update-model.md#describing-table-updates) containing the relevant data. Data indexes are used to improve the speed of data access operations in tables. Many table operations will use a data index if it is present. Every data index is backed by a table, with the index stored in a column called `dh_row_set`.

Data indexes can improve the speed of filtering operations. Additionally, data indexes can be useful if multiple query operations need to compute the same data index on a table. This is common when a table is used in multiple joins or aggregations. If the table does not have a data index, each operation will internally create the same index. If the table does have a data index, the individual operations will not need to create their own indexes and can execute faster and use less RAM.

## Data index inheritance and behavior

Data indexes are inherited by derived tables unless one of the following conditions is true:

1. The row set has changed.
2. Any of the indexed data changed.
3. It's a shared table from another worker (though you can still take advantage of the index if you apply pre-filtering before subscription).

### Partitioning columns as data indexes

Partitioning columns are a specialized form of data index. The main addition is that after location selection, the engine also leverages them like other data indexes for query operations.

## Create a data index

A data index can be created from a source table and one or more key columns using `getOrCreateDataIndex`:

```groovy test-set=1 order=source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(10).update("X = i")
sourceIndex = getOrCreateDataIndex(source, "X")
```

> [!IMPORTANT]
> When a new data index is created, it is not immediately computed. The index is computed when a table operation first uses it or the `table` method is called on the index. This is an important detail when trying to understand performance data.

Every data index is backed by a table. The backing table can be retrieved with the `table` method:

```groovy test-set=1 order=result
result = sourceIndex.table()
```

The `source` table in the above example doesn't create a very interesting data index. Each key value in `X` only appears once. The following example better illustrates a data index:

```groovy order=sourceIndex,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(10).update("X = 100 + i % 5")
sourceIndex = getOrCreateDataIndex(source, "X").table()
```

When looking at `source`, we can see that the value `100` in column `X` appears in rows 0 and 5. The value `101` appears in rows 1, 6, and so on. Each row in `dh_row_set` contains the rows where each unique key value appears. Let's look at an example for more than one key column.

```groovy order=sourceIndex,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex

source = emptyTable(20).update("X = i % 4", "Y = i % 2")
sourceIndex = getOrCreateDataIndex(source, "X", "Y").table()
```

There are only four unique keys in `X` and `Y` together. The resultant column `dh_row_set` shows the row sets in which each unique key appears.

All of the previous examples create tables with flat data indexes. A flat data index is one in which the row sets are sequential. However, it is not safe to assume that all data indexes are flat, as this is uncommon in practice. Consider the following example where the data index is not flat:

```groovy test-set=3 order=sourceIndexTable,result skip-test
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(25).update("Key = 100 + randomInt(0, 4)", "Value = randomDouble(0, 100)")

sourceIndex = getOrCreateDataIndex(source, "Key").table()

result = sourceIndex.where("Key in 100, 102")
```

## Retrieve and verify a data index

You can verify whether a data index exists for a particular table using one of two methods:

- `hasDataIndex`: Returns a boolean indicating whether a data index exists for the table and the given key column(s).
- `getDataIndex`: Returns a data index if it exists. Returns `null` it does not, if the cached index is invalid, or if the refreshing cached index is no longer live.

The following example creates data indexes for a table and checks if they exist based on different key columns. [`hasDataIndex`](../reference/engine/hasDataIndex.md) is used to check if the index exists.

```groovy order=:log,source
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(100).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 8",
    "Key2 = ii % 11",
    "Key3 = ii % 19",
    "value = -ii"
)

index13 = getOrCreateDataIndex(source, "Key1", "Key3")

hasIndex1 = hasDataIndex(source, "Key1")
hasIndex23 = hasDataIndex(source, "Key2", "Key3")
hasIndex13 = hasDataIndex(source, "Key1", "Key3")

println "There is a data index for source on Key 1: ${hasIndex1}"
println "There is a data index for source on Keys 2 and 3: ${hasIndex23}"
println "There is a data index for source on Keys 1 and 3: ${hasIndex13}"


def index2 = getDataIndex(source, "Key2")
def hasIndex2AfterGet = hasDataIndex(source, "Key2")

println "There is a data index for source on Key 2: ${hasIndex2AfterGet}"
```

## Usage in queries

> [!WARNING]
> Data indexes are [weakly reachable](https://en.wikipedia.org/wiki/Weak_reference). Assign the index to a variable to ensure it does not get garbage collected.

If a table has a data index and an engine operation can leverage it, it will be automatically used. The following table operations can use data indexes:

- Join:
  - [`exactJoin`](../reference/table-operations/join/exact-join.md)
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md)
  - [`aj`](../reference/table-operations/join/aj.md)
  - [`raj`](../reference/table-operations/join/raj.md)
- Group and aggregate:
  - [Single aggregators](./dedicated-aggregations.md)
  - [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
  - [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- Filter:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- Select:
  - [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- Sort:
  - [`sort`](../reference/table-operations/sort/sort.md)
  - [`sortDescending`](../reference/table-operations/sort/sort-descending.md)

The following subsections explore different table operations. The code blocks below use the following tables:

```groovy test-set=2 order=source,sourceRight,sourceRightDistinct
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*

source = emptyTable(100_000).update(
    "Timestamp = '2024-04-25T12:00:00Z'",
    "Key1 = (int)i % 11",
    "Key2 = (int)i % 47",
    "Key3 = (int)i % 499",
    "Value = (int)i"
)

sourceRight = emptyTable(400).update(
    "Timestamp = '2024-04-25T12:00:00Z'",
    "Key1 = (int)i % 11",
    "Key2 = (int)i % 43",
    "Value = (double)(int)i / 2.0"
)

sourceRightDistinct = sourceRight.selectDistinct("Key1", "Key2")
```

### Filter

The engine will use single and multi-column indexes to accelerate filtering operations through the [predicate pushdown](./predicate-pushdown.md) framework. When a materialized (in-memory) data index exists, the engine can use it to quickly identify matching rows for various filter types. Deferred (disk-based) data indexes will not be materialized by `where` operations.

> [!NOTE]
> The Deephaven engine only uses a [`DataIndex`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/DataIndex.html) when the keys exactly match what is needed for an operation. For example, if a data index is present for the columns `X` and `Y`, it will not be used if the engine only needs an index for column `X`.

Support for using data indexes with most filter types (beyond exact matches) was introduced in Deephaven v0.40.0 through the predicate pushdown framework.

The following filters can use the index created atop the code block:

```groovy test-set=2 order=resultK1,resultK1In
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultK1 = source.where("Key1 = 7")
resultK1In = source.where("Key1 in 2,4,6,8")
```

The following example can only use the index for the first filter:

```groovy test-set=2 order=resultK1K2
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultK1K2 = source.where("Key1 = 7", "Key2 = 11")
```

This is true for filtering one table based on another as well. The first filter in the following example can use `sourceK1K2Index`, whereas the second example cannot use `sourceRightK1Index` because the index does not match the filter:

```groovy test-set=2 order=resultWhereIn,resultWhereNotIn
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")
sourceRightK1Index = getOrCreateDataIndex(sourceRight, "Key1")

resultWhereIn = source.whereIn(sourceRightDistinct, "Key1", "Key2")
resultWhereNotIn = source.whereNotIn(sourceRightDistinct, "Key1")
```

### Join

Different joins will use indexes differently:

| Method                                                                   | Can use indexes?   | Can use left table index? | Can use right table index? |
| ------------------------------------------------------------------------ | ------------------ | ------------------------- | -------------------------- |
| [`exactJoin`](../reference/table-operations/join/exact-join.md)          | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`naturalJoin`](../reference/table-operations/join/natural-join.md)      | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`aj`](../reference/table-operations/join/aj.md)                         | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`raj`](../reference/table-operations/join/raj.md)                       | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`join`](../reference/table-operations/join/join.md)                     | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`multiJoin`](../reference/table-operations/join/multijoin.md)           | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`rangeJoin`](../reference/table-operations/join/rangeJoin.md)           | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |

#### Natural join

When performing a [`naturalJoin`](../reference/table-operations/join/natural-join.md) without data indexes, the engine performs a linear scan of the `source` (left) table's rows. Each key is hashed and a map is created to the specified joining (right) table. If a data index exists for the `source` (left) table, the engine skips the scan and uses the index directly to create the mapping.

Consider a [`naturalJoin`](../reference/table-operations/join/natural-join.md) on `source` and `sourceRight`. The following example can use `sourceK1K2Index` to accelerate the join, but not `sourceRightK1K2Index` because the operation cannot use the right table index:

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

The exact match in an [`aj`](../reference/table-operations/join/aj.md) or an [`raj`](../reference/table-operations/join/raj.md) can be accelerated using data indexes, eliminating the task of grouping the source table rows by key. The following example can use the indexes created because they match the exact match column used in the join:

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

Sorting operations can be accelerated using both full and partial indexes.

- A full index matches all sorting columns.
- A partial index matches a subset of sorting columns.

When a full index is present, the engine only needs to sort the index rows.

A sort operation can only use a partial index if the partial index matches the _first_ sorting column. When a sort operation uses a partial index, the engine sorts the first column using the index and then sorts any remaining columns naturally.

The following sort operation uses the index because it matches the sort columns (ordering does not matter):

```groovy test-set=2 order=resultSortedK2K1
sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")

resultSortedK2K1 = source.sortDescending("Key2", "Key1")
```

The following sort operation uses the index when sorting the first key column only:

```groovy test-set=2 order=resultSortedK1K3
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultSortedK1K3 = source.sort("Key1", "Key3")
```

The following sort operation does not use any indexes, as the only partial index (on `Key1`) does not match the first sort column:

```groovy test-set=2 order=resultSortedK3K1
sourceK1Index = getOrCreateDataIndex(source, "Key1")

resultSortedK3K1 = source.sort("Key3", "Key1")
```

### Aggregate

Aggregations without data indexes require the engine to group all similar keys and perform the calculation on subsets of rows. The index allows the engine to skip the grouping step and immediately begin calculating the aggregation. The following aggregation uses the index because it matches the aggregation key columns:

```groovy test-set=2 order=resultAgged
import io.deephaven.api.agg.Aggregation

sourceK1K2Index = getOrCreateDataIndex(source, "Key1", "Key2")

aggregations = [
    AggSum("Sum=Value"),
    AggAvg("Avg=Value"),
    AggStd("Std=Value")
]

resultAgged = source.aggBy(aggregations, "Key1", "Key2")
```

## Persist data indexes with Parquet

Deephaven can save data indexes along with your data when you [write a table to Parquet files](./data-import-export/parquet-export.md). By default, it saves all available indexes. You can also choose to save only some indexes. If you try to save an index that doesn't exist yet, Deephaven will create a temporary index just for the saving process. When you [load data from a Parquet file](./data-import-export/parquet-import.md), it also loads any saved indexes.

The following example writes a table plus indexes to Parquet and then reads a table plus indexes from the Parquet files.

```groovy order=:log
import static io.deephaven.engine.table.impl.indexer.DataIndexer.*
import io.deephaven.engine.liveness.*
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.util.SafeCloseable

source = emptyTable(100_000).update(
    "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
    "Key1 = ii % 11",
    "Key2 = ii % 47",
    "Key3 = ii % 499",
    "value = ii"
)


// Write to disk and specify two sets of index key columns
writeInstructions = ParquetInstructions.builder().addIndexColumns("Key1").addIndexColumns("Key1", "Key2").build()
ParquetTools.writeTable(source, "/data/indexed.parquet", writeInstructions)

// Load the table and the indexes from disk
diskTable = ParquetTools.readTable("/data/indexed.parquet")

// Show that the table loaded from disk has indexes
println hasDataIndex(diskTable, "Key1")
println hasDataIndex(diskTable, "Key1", "Key2")

scope = new LivenessScope()

// Open a new liveness scope so the indexes are released after writing
try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
    indexKey1 = getOrCreateDataIndex(source, "Key1")
    indexKey1Key2 = getOrCreateDataIndex(source, "Key1", "Key2")

    // Write to disk - indexes are automatically persisted
    ParquetTools.writeTable(source, "/data/indexed.parquet")
}

// Load the table and the indexes from disk
diskTableNew = ParquetTools.readTable("/data/indexed.parquet")

// Show that the table loaded from disk has indexes
println hasDataIndex(diskTable, "Key1")
println hasDataIndex(diskTable, "Key1", "Key2")
```

## Performance

Data indexes improve performance when used in the right context. If used in the wrong context, data indexes can make performance worse. The following list provides guidelines to consider when determining if data indexes will improve performance:

- A data index is more likely to help performance if the data within an index group is located near (in memory) other data in the group. This avoids index fragmentation and poor data access patterns.
- Reusing a data index multiple times is necessary to overcome the cost of initially computing the index.
- Generally speaking, the higher the key cardinality, the better the performance improvement.

### Benchmark scripts

If you're interested in how data indexes impact query performance, the scripts below collect performance metrics for each operation. You will notice that performance improves in some instances and is worse in others.

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
    // call .table() to force index computation here -- to benchmark the index creation separately -- not for production
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

    if (createIdxs[0]) {
        addIndex(t, ["I"])
    }
    if (createIdxs[1]) {
        addIndex(t, ["J"])
    }
    if (createIdxs[2]) {
        addIndex(t, ["I", "J"])
    }

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

Here's a follow-up script that runs tests for both low- and high-cardinality cases, both with and without data indexes:

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

It can also be helpful to plot the results:

```groovy skip-test
operations = ["where", "countBy", "sumBy", "naturalJoin"]

scenarioToTimes = [
    "Low cardinality (700 keys), no indexes.": timesLcNi,
    "Low cardinality (700 keys), with indexes.": timesLcWi,
    "High cardinality (99,700 keys), no indexes.": timesHcNi,
    "High cardinality (99,700 keys), with indexes.": timesHcWi,
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

## Related documentation

- [Combined aggregations](./dedicated-aggregations.md)
- [Filter table data](./use-filters.md)
- [Export to Parquet](./data-import-export/parquet-export.md)
- [Import to Parquet](./data-import-export/parquet-import.md)
- [Joins: exact and relational](./joins-exact-relational.md)
- [Joins: inexact and range](./joins-timeseries-range.md)
- [Sort table data](./sort.md)
- [`getDataIndex`](../reference/engine/getDataIndex.md)
- [`hasDataIndex`](../reference/engine/hasDataIndex.md)
- [`DataIndex` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/DataIndex.html)
- [`DataIndexer` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
