---
title: Query table configuration
---

[`QueryTable`](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) is Deephaven's primary implementation of the [Table API](../getting-started/crash-course/table-ops.md). This guide describes the main configuration properties that control [`QueryTable`](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) features and the memory recyclers that its column sources use, all of which affect your Deephaven tables' latency, throughput, and memory use.

The defaults suit most workloads, whether your tables are static or ticking. Change these properties when you benchmark, when memory is tight, or when you need to understand why an operation does or doesn't run in parallel.

Set these properties as JVM system properties (for example, `-DQueryTable.memoizeResults=false`) or in a [configuration file](../how-to-guides/configuration/config-file.md). See [How to set properties](../how-to-guides/configuration/configuration-properties.md#how-to-set-properties) for the ways to do so in each kind of deployment. The engine reads each property once, at startup, so a change takes effect only after a server restart.

## Properties at a glance

The table below lists every property this page describes.

| Category                                                                                      | Property                                                 | Default    |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ---------- |
| [Memoization](#memoization)                                                                   | `QueryTable.memoizeResults`                              | true       |
| [Redirection](#redirection)                                                                   | `QueryTable.redirectUpdate`                              | false      |
| [Redirection](#redirection)                                                                   | `QueryTable.redirectSelect`                              | false      |
| [Redirection](#redirection)                                                                   | `QueryTable.maximumStaticSelectMemoryOverhead`           | 1.1        |
| [Data indexes](#data-indexes)                                                                 | `QueryTable.useDataIndexForWhere`                        | true       |
| [Data indexes](#data-indexes)                                                                 | `QueryTable.useDataIndexForAggregation`                  | true       |
| [Data indexes](#data-indexes)                                                                 | `QueryTable.useDataIndexForJoins`                        | true       |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownDataIndex`               | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownDictionary`              | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownSortedColumn`            | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownMergedTables`            | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.disableWherePushdownAggregationKey`          | false      |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.dataIndexForWhereThreshold`                  | 0.0625     |
| [Predicate pushdown with `where`](#predicate-pushdown-with-where)                             | `QueryTable.dictionaryForWhereThreshold`                 | 0.25       |
| [Stateless by default](#stateless-by-default)                                                 | `QueryTable.statelessFiltersByDefault`                   | true       |
| [Stateless by default](#stateless-by-default)                                                 | `QueryTable.statelessSelectByDefault`                    | true       |
| [Stateless by default](#stateless-by-default)                                                 | `QueryTable.serialSelectImplicitBarriers`                | false      |
| [Parallel processing with `where`](#parallel-processing-with-where)                           | `QueryTable.disableParallelWhere`                        | false      |
| [Parallel processing with `where`](#parallel-processing-with-where)                           | `QueryTable.parallelWhereRowsPerSegment`                 | `1 << 16`  |
| [Parallel processing with `where`](#parallel-processing-with-where)                           | `QueryTable.parallelWhereSegments`                       | -1         |
| [Parallel processing with `where`](#parallel-processing-with-where)                           | `QueryTable.forceParallelWhere`                          | false      |
| [Parallel processing with `select` and `update`](#parallel-processing-with-select-and-update) | `QueryTable.enableParallelSelectAndUpdate`               | true       |
| [Parallel processing with `select` and `update`](#parallel-processing-with-select-and-update) | `QueryTable.minimumParallelSelectRows`                   | `1L << 22` |
| [Parallel processing with `select` and `update`](#parallel-processing-with-select-and-update) | `QueryTable.forceParallelSelectAndUpdate`                | false      |
| [Sorting](#sorting)                                                                           | `QueryTable.parallelSort`                                | true       |
| [Sorting](#sorting)                                                                           | `QueryTable.minimumParallelSortRows`                     | `1L << 20` |
| [Sorting](#sorting)                                                                           | `QueryTable.parallelSortSegmentSize`                     | `1L << 18` |
| [Sorting](#sorting)                                                                           | `QueryTable.useIndirectSortKernels`                      | true       |
| [Sorting](#sorting)                                                                           | `QueryTable.sortBySymbolTable`                           | true       |
| [Parallel snapshotting](#parallel-snapshotting)                                               | `QueryTable.enableParallelSnapshot`                      | true       |
| [Parallel snapshotting](#parallel-snapshotting)                                               | `QueryTable.minimumParallelSnapshotRows`                 | `1L << 20` |
| [Ungroup operations](#ungroup-operations)                                                     | `QueryTable.minimumUngroupBase`                          | 10         |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `array.recycler.capacity.*`                              | 1024       |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `sparsearray.recycler.capacity.*`                        | 1024       |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `array.recycler.maximumCapacity.*`                       | unlimited  |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `sparsearray.recycler.maximumCapacity.*`                 | unlimited  |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `SoftRecycler.windowMillis`                              | 1000       |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `SoftRecycler.maximumWindowMillis`                       | 60000      |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `SoftRecycler.windowMargin`                              | 1.5        |
| [`SoftRecycler` configuration](#softrecycler-configuration)                                   | `SoftRecycler.shrinkFraction`                            | 0.1        |

The sections below describe each property, grouped by the feature it controls.

## Memoization

Deephaven uses [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations to improve performance by eliminating duplicate work. See [Why do some queries run faster the second time they are run?](../reference/community-questions/query-memoization.md) for more details.

Disable memoization when benchmarking or testing, because memoized results hide the true cost of operations and skew performance metrics.

| Property name               | Default value | Description                        |
| --------------------------- | ------------- | ---------------------------------- |
| `QueryTable.memoizeResults` | true          | Enables memoizing table operations |

## Redirection

Each row in a Deephaven table has a row key, a 63-bit number that identifies the row. Column sources return a row's values by its row key. Row keys do not have to be consecutive, so a table's row keys can be sparse. Sparse keys do not force large allocations, because many of Deephaven's [column sources](/core/javadoc/io/deephaven/engine/table/ColumnSource.html), the objects that store and return a column's values by row key, store data in multiple levels. A table is [flat](../reference/table-operations/select/flatten.md) when its row keys run consecutively from 0 through the table's size minus 1.

Redirection maps each row key of an operation's result to the row key where a column source holds that row's data. For example, a sorted result redirects each of its row keys to the original row that holds the value in that sort position. Redirection can also map a sparse keyspace onto a flat, dense one.

How [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) store their results depends on the parent table:

- **[Flat](../reference/table-operations/select/flatten.md) tables**: The parent's row keys are already consecutive, so the engine stores the result at those row keys. The redirection properties have no effect.
- **[Refreshing tables](./table-types.md#standard-streaming-tables) that are neither flat nor [append-only](./table-types.md#specialization-1-append-only)**: By default, the engine stores the result in sparse column sources at the parent's row keys. Redirection stores it densely instead. Dense storage can use less memory, but every read must first look up the row in the redirection.
- **Refreshing [append-only tables](./table-types.md#specialization-1-append-only)**: Rows never move, so the engine always stores the result densely by position, whatever the redirection properties are set to.
- **[Static tables](./table-types.md#static-tables) that are not flat**: The result depends on the operation and on `QueryTable.maximumStaticSelectMemoryOverhead`:
  - [`select`](../reference/table-operations/select/select.md) stores its result densely. It keeps the parent's row keys only when a sparse result stays within the overhead limit and it either copies a parent column held in memory (other than a vector column) or uses a formula that reads another row by a constant offset, such as `X_[i-1]`.
  - When a sparse result would exceed the limit, [`select`](../reference/table-operations/select/select.md) [flattens](../reference/table-operations/select/flatten.md) the parent first.
  - [`update`](../reference/table-operations/select/update.md) stores its result sparsely, at the parent's row keys, unless a sparse result would exceed the limit. Then it stores the result densely.

The properties below control these choices. See [How efficient are my table selection operations? (Groovy)](/core/groovy/docs/reference/community-questions/selection-method-properties/) for more details.

| Property name                                  | Default value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.redirectUpdate`                    | false         | Forces [`update`](../reference/table-operations/select/update.md) on a [refreshing table](./table-types.md#standard-streaming-tables) that is neither flat nor [append-only](./table-types.md#specialization-1-append-only) to redirect, even though every read must then look up the row in the redirection. The engine computes redirected columns serially                                                                                                                                                                                                                                                                                                 |
| `QueryTable.redirectSelect`                    | false         | Forces [`select`](../reference/table-operations/select/select.md) on a [refreshing table](./table-types.md#standard-streaming-tables) that is neither flat nor [append-only](./table-types.md#specialization-1-append-only) to redirect, even though every read must then look up the row in the redirection. The engine computes redirected columns serially                                                                                                                                                                                                                                                                                                 |
| `QueryTable.maximumStaticSelectMemoryOverhead` | 1.1           | The most memory a sparse result of [`select`](../reference/table-operations/select/select.md) or [`update`](../reference/table-operations/select/update.md) on a [static table](./table-types.md#static-tables) that is not flat may use, as a multiple of what a dense result needs. For example, 1.1 allows 10% more. Above this, the engine stores the result densely. A negative value turns the check off, so [`update`](../reference/table-operations/select/update.md) always stores its result sparsely and [`select`](../reference/table-operations/select/select.md) never flattens the parent first. A value of 0 means the result is never sparse |

## Data indexes

A Deephaven [data index](../how-to-guides/data-indexes.md) is an index that can improve the speed of operations such as [filters](../how-to-guides/use-filters.md), [aggregations](./aggregations.md), [exact and relational joins](../how-to-guides/joins-exact-relational.md), and [time-series joins](../how-to-guides/joins-timeseries-range.md).

| Property name                           | Default value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.useDataIndexForWhere`       | true          | Enables [data index](../how-to-guides/data-indexes.md) usage in [`where`](../reference/table-operations/filter/where.md) operations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `QueryTable.useDataIndexForAggregation` | true          | Enables [data index](../how-to-guides/data-indexes.md) usage in [aggregations](./aggregations.md), including [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), dedicated aggregations such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) or [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md), [`select_distinct`](../reference/table-operations/select/select-distinct.md), [rollup tables](../how-to-guides/rollup-table.md), and [tree tables](../how-to-guides/tree-table.md)                                                                                        |
| `QueryTable.useDataIndexForJoins`       | true          | Enables [data index](../how-to-guides/data-indexes.md) usage in [`exact_join`](../reference/table-operations/join/exact-join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), [`aj`](../reference/table-operations/join/aj.md), and [`raj`](../reference/table-operations/join/raj.md), including the [`natural_join`](../reference/table-operations/join/natural-join.md) step of [`full_outer_join`](../reference/table-operations/join/full-outer-join.md). [`join`](../reference/table-operations/join/join.md) and [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) never use data indexes |

Two more properties affect how [`where`](../reference/table-operations/filter/where.md) uses [data indexes](../how-to-guides/data-indexes.md). `QueryTable.disableWherePushdownDataIndex` turns off the per-file Parquet data indexes that [predicate pushdown](../how-to-guides/predicate-pushdown.md) uses, separately from the table-level index that `QueryTable.useDataIndexForWhere` controls. `QueryTable.dataIndexForWhereThreshold` sets how large an index can be for [`where`](../reference/table-operations/filter/where.md) to use it. [Predicate pushdown with `where`](#predicate-pushdown-with-where) describes both.

## Predicate pushdown with `where`

[Predicate pushdown](../how-to-guides/predicate-pushdown.md) applies filter conditions as early as possible. Ideally, the engine applies them at the data source, such as a Parquet file, before it loads any data. Passing the predicates to the data source lets the engine skip files, row groups, and rows that cannot satisfy them. Skipping that data reduces I/O and improves performance.

The engine pushes filters down through several techniques, including [data indexes](../how-to-guides/data-indexes.md), Parquet row group metadata, dictionary encoding, and sorted columns. It can also push a filter through a [merged](../reference/table-operations/merge/merge.md) table to its constituents, and answer a filter on an [aggregation](./aggregations.md)'s single key column from the aggregation's hash table. [Predicate pushdown filtering](../how-to-guides/predicate-pushdown.md) describes the first four techniques. Each property below turns one technique off or tunes it.

| Property name                                            | Default value | Description                                                                                                                                                                                                                                                                                          |
| -------------------------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.disableWherePushdownDataIndex`               | false         | Disables the use of Parquet files' own data indexes during [predicate pushdown](../how-to-guides/predicate-pushdown.md) in [`where`](../reference/table-operations/filter/where.md). `QueryTable.useDataIndexForWhere` controls a table-level [data index](../how-to-guides/data-indexes.md) instead |
| `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false         | Disables the use of Parquet row group metadata during [predicate pushdown](../how-to-guides/predicate-pushdown.md)                                                                                                                                                                                   |
| `QueryTable.disableWherePushdownDictionary`              | false         | Disables the use of dictionary encoding during [predicate pushdown](../how-to-guides/predicate-pushdown.md)                                                                                                                                                                                          |
| `QueryTable.disableWherePushdownSortedColumn`            | false         | Disables the use of sorted column binary search during [predicate pushdown](../how-to-guides/predicate-pushdown.md)                                                                                                                                                                                  |
| `QueryTable.disableWherePushdownMergedTables`            | false         | Disables [predicate pushdown](../how-to-guides/predicate-pushdown.md) when filtering [merged](../reference/table-operations/merge/merge.md) tables                                                                                                                                                   |
| `QueryTable.disableWherePushdownAggregationKey`          | false         | Disables probing an [aggregation](./aggregations.md)'s hash table when pushing down a filter on the single key column of an [aggregation](./aggregations.md) result                                                                                                                                  |
| `QueryTable.dataIndexForWhereThreshold`                  | 0.0625        | The size, as a fraction of the rows remaining to be filtered, that a [data index](../how-to-guides/data-indexes.md) table must stay below for [`where`](../reference/table-operations/filter/where.md) to use the index                                                                              |
| `QueryTable.dictionaryForWhereThreshold`                 | 0.25          | The size, as a fraction of the rows remaining to be filtered, that a dictionary must stay below for [predicate pushdown](../how-to-guides/predicate-pushdown.md) to use it                                                                                                                           |

The two `*ForWhereThreshold` properties are cost heuristics rather than on/off switches. Scanning a [data index](../how-to-guides/data-indexes.md) table or a dictionary costs time proportional to its size, so the engine only does so when that structure is small relative to the rows remaining to be filtered. When the index table or dictionary reaches its threshold, the engine skips it and evaluates the filter on each remaining row instead. Raise the value toward `1.0` to apply these optimizations to columns with more distinct values, or set it to `0` to effectively disable the technique.

## Stateless by default

A stateless filter or column does not depend on mutable state outside its row or on the order in which rows are evaluated, so the engine can evaluate it in parallel. Two properties set what the engine assumes when you don't say:

- `QueryTable.statelessFiltersByDefault` applies to filters compiled from formulas.
- `QueryTable.statelessSelectByDefault` applies to [`Selectable`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) columns.

[Controlling concurrency for `select`, `update`, and `where`](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) describes how to override the default for one filter or column:

- Mark it [serial](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) with [`with_serial`](../reference/query-language/types/ConcurrencyControl.md#with_serial) to make it stateful.
- Use [Barriers](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) to control the order in which filters or columns are evaluated relative to each other.

| Property name                             | Default value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueryTable.statelessFiltersByDefault`    | true          | Enables the engine to assume that filters compiled from formulas are stateless by default, so it can evaluate them in parallel and [push them down to the data source](../how-to-guides/predicate-pushdown.md)                                                                                                                                                                                                                                                                                                                                                                                         |
| `QueryTable.statelessSelectByDefault`     | true          | Enables the engine to assume that [`Selectable`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) columns are stateless by default, so it can compute their rows in parallel                                                                                                                                                                                                                                                                                                                                                                                                          |
| `QueryTable.serialSelectImplicitBarriers` | false         | Makes stateful [`Selectable`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) columns, such as [serial](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) ones, form implicit [Barriers](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), so the engine evaluates them one after another in column order instead of concurrently. When this property is not set, it defaults to the opposite of `QueryTable.statelessSelectByDefault`, so it is true when `QueryTable.statelessSelectByDefault` is false |

## Parallel processing with `where`

A [`where`](../reference/table-operations/filter/where.md) filter runs in parallel only when all of these conditions hold:

- `QueryTable.disableParallelWhere` is false.
- The filter is [stateless](#stateless-by-default).
- The filter does not use Python objects or call Python functions, unless the Python build is free-threaded (has no global interpreter lock). [The Python-Java boundary](./python-java-boundary.md#why-crossing-the-boundary-affects-performance) explains why.
- At least twice `QueryTable.parallelWhereRowsPerSegment` plus 2 rows reach the filter, unless `QueryTable.forceParallelWhere` is true. With the default, that is 131,074 rows. This threshold avoids the overhead of using threads for small filters.
- The engine has more than one thread to run the filter on, and the maximum number of segments (see below) is greater than 1. The number of threads available is the engine's _parallelism factor_. For the initial filter, it is the size of the [operation initialization thread pool](./periodic-update-graph-configuration.md#operation-initialization-thread-pool). For updates to a refreshing table, it is the number of [update graph threads](./periodic-update-graph-configuration.md#updatethreads). When filtering updates, `QueryTable.forceParallelWhere` skips the thread check. The initial filter never skips it.

The engine then divides the filter's rows evenly into segments, one task per segment. The number of segments is the smaller of these two values:

- `QueryTable.parallelWhereSegments`, or the parallelism factor when that property is 0 or less.
- The number of rows divided by `QueryTable.parallelWhereRowsPerSegment`, rounded up.

Tune these properties to avoid parallelism whose overhead exceeds its gains.

| Property name                            | Default value | Description                                                                                                                                                                                                  |
| ---------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueryTable.disableParallelWhere`        | false         | Disables parallel evaluation of [`where`](../reference/table-operations/filter/where.md) filters                                                                                                             |
| `QueryTable.parallelWhereRowsPerSegment` | `1 << 16`     | The target number of rows per segment. A filter runs in parallel only when at least twice this many rows plus 2 reach it                                                                                     |
| `QueryTable.parallelWhereSegments`       | -1            | The maximum number of segments. A value of 0 or less uses the parallelism factor described above                                                                                                             |
| `QueryTable.forceParallelWhere`          | false         | For testing only: forces [`where`](../reference/table-operations/filter/where.md) operations to parallelize even when fewer than twice `QueryTable.parallelWhereRowsPerSegment` plus 2 rows reach the filter |

## Parallel processing with `select` and `update`

[`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) can compute columns in parallel in two phases:

- **Initialization**: The operation computes every row of its result table.
- **Updates**: When the parent table changes, the operation computes the rows added or modified in that update.

The engine computes a column in parallel only when all of these conditions hold:

- `QueryTable.enableParallelSelectAndUpdate` (or `QueryTable.forceParallelSelectAndUpdate`) is true, and the engine can run work on more than one thread.
- The column is [stateless](#stateless-by-default).
- The formula does not use Python objects or call Python functions, unless the Python build is free-threaded (has no global interpreter lock). [The Python-Java boundary](./python-java-boundary.md#why-crossing-the-boundary-affects-performance) explains why.
- The result is not [redirected](#redirection) by `QueryTable.redirectSelect` or `QueryTable.redirectUpdate`. The engine can still compute a result in parallel when it stores that result densely by position, as it does for [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) on [append-only tables](./table-types.md#specialization-1-append-only).
- The update does not shift any rows.
- At least `QueryTable.minimumParallelSelectRows` rows need to be computed, or the column's values are tables or [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s. Small updates to a large table therefore run serially, unless the column holds tables or [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s.

Each parallel task computes at least `QueryTable.minimumParallelSelectRows` rows, except possibly the last. A column whose values are tables or [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s is split into tasks of one row each. Tune `QueryTable.minimumParallelSelectRows` to avoid parallelism whose overhead exceeds its gains.

| Property name                              | Default value | Description                                                                                                                                                                                                                                                                      |
| ------------------------------------------ | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSelectAndUpdate` | true          | Enables parallel computation of columns in [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md)                                                                                                             |
| `QueryTable.minimumParallelSelectRows`     | `1L << 22`    | The minimum number of rows a column must compute to run in parallel, and the minimum number of rows in each parallel task                                                                                                                                                        |
| `QueryTable.forceParallelSelectAndUpdate`  | false         | For testing only: enables parallel [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) even when `QueryTable.enableParallelSelectAndUpdate` is false. `QueryTable.minimumParallelSelectRows` still applies |

## Sorting

These properties control how [`sort`](../reference/table-operations/sort/sort.md) and [`sort_descending`](../reference/table-operations/sort/sort-descending.md) compute their results. A [sort](../reference/table-operations/sort/sort.md) of at least `QueryTable.minimumParallelSortRows` rows can run in parallel. The engine processes updates to the [sort](../reference/table-operations/sort/sort.md) of a [refreshing table](./table-types.md#standard-streaming-tables) serially.

| Property name                        | Default value | Description                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------ | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.parallelSort`            | true          | Enables parallel sorting. When false, every [sort](../reference/table-operations/sort/sort.md) runs entirely on the calling thread                                                                                                                                                                                                                 |
| `QueryTable.minimumParallelSortRows` | `1L << 20`    | The minimum number of rows for a [sort](../reference/table-operations/sort/sort.md) to run in parallel. A value of 0 or less disables parallel sorting                                                                                                                                                                                             |
| `QueryTable.parallelSortSegmentSize` | `1L << 18`    | The minimum number of rows in each segment of a parallel [sort](../reference/table-operations/sort/sort.md). When dividing the rows by the size of the [operation initialization thread pool](./periodic-update-graph-configuration.md#operation-initialization-thread-pool) would make segments smaller than this, the engine uses fewer segments |
| `QueryTable.useIndirectSortKernels`  | true          | Uses indirect sort kernels, which permute row positions instead of values, for [sorts](../reference/table-operations/sort/sort.md) of object columns and [sorts](../reference/table-operations/sort/sort.md) of multiple columns. When false, multiple-column [sorts](../reference/table-operations/sort/sort.md) sort one column at a time        |
| `QueryTable.sortBySymbolTable`       | true          | Allows a single-column [sort](../reference/table-operations/sort/sort.md) to sort the column's symbol table instead of every row. A symbol table holds a column's distinct values. Some sources, such as dictionary-encoded Parquet columns, provide one                                                                                           |

## Parallel snapshotting

The server fulfills [Barrage](./what-is-barrage.md) subscription requests by snapshotting the requested rows and columns. The JavaScript client that the web UI uses sends these requests too. When the table is refreshing, the server also listens for changes to those rows and columns.

Parallel snapshotting reads the columns of a snapshot in parallel. It reduces latency when the columns are slow to access, but it can open many file handles to the same data source.

Parallel snapshotting applies to a snapshot of more than one column when either of these is true:

- The snapshot has at least `QueryTable.minimumParallelSnapshotRows` rows.
- The snapshot includes a column that is not held in memory.

Tune this threshold to avoid parallelism whose overhead exceeds its gains.

| Property name                            | Default value | Description                                                                                                     |
| ---------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSnapshot`      | true          | Enables parallel snapshotting across columns, such as for [Barrage](./what-is-barrage.md) subscription requests |
| `QueryTable.minimumParallelSnapshotRows` | `1L << 20`    | The minimum number of rows for a snapshot to be parallelized, unless one of its columns is not held in memory   |

## Ungroup operations

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) table operation can expand one row into multiple rows. `QueryTable.minimumUngroupBase` sets the smallest range of [row keys](#redirection) that [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) reserves for each input row. The range is a power of two, and the property sets its exponent.

| Property name                   | Default value | Description                                                                                                                                                                                                                                         |
| ------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.minimumUngroupBase` | 10            | The minimum base for the row keys of [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md). Each input row reserves `2^base` output row keys, and the engine uses a larger base when an input row has more than `2^base` values |

## `SoftRecycler` configuration

Deephaven uses [`SoftRecycler`](/core/javadoc/io/deephaven/util/SoftRecycler.html) objects to manage memory for array and sparse array column sources. These column sources must keep previous values during each [update cycle](./table-update-model.md). When a column source needs memory to record previous values, it borrows the memory from a recycler instead of allocating new memory. It returns the memory to the recycler at the end of the update cycle. Recyclers improve performance and reduce garbage collection pressure.

A recycler's capacity is the most arrays it holds at once. Each recycler adapts its capacity to its traffic:

- It starts at a configured capacity.
- It grows when a cycle needs more arrays than it holds, up to a configured maximum.
- It shrinks back toward the configured capacity when arrays go unused, but never below it.

You can set both values per type. [How recyclers grow and shrink](#how-recyclers-grow-and-shrink) gives the exact rules.

### Array column source recyclers

Array-backed column sources (dense arrays) keep one [`SoftRecycler`](/core/javadoc/io/deephaven/util/SoftRecycler.html) per type for their blocks of data. The `capacity` properties set the configured capacity and the `maximumCapacity` properties set the configured maximum described above.

| Property name                            | Default value | Description                                                                                                                                                                                                |
| ---------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `array.recycler.capacity.default`        | 1024          | Recycler capacity for array types whose type-specific property is not set                                                                                                                                  |
| `array.recycler.capacity.<type>`         | 1024          | Recycler capacity for blocks of `<type>`, which is one of `boolean`, `byte`, `char`, `double`, `float`, `int`, `long`, `short`, or `object`. Falls back to `array.recycler.capacity.default` when not set  |
| `array.recycler.maximumCapacity.default` | unlimited     | Maximum recycler capacity for array types whose type-specific property is not set                                                                                                                          |
| `array.recycler.maximumCapacity.<type>`  | unlimited     | Maximum recycler capacity for blocks of `<type>`, which is one of `boolean`, `byte`, `char`, `double`, `float`, `int`, `long`, `short`, or `object`. Never less than that type's `array.recycler.capacity` |

The "in use" bitmaps, which record which entries of a block hold a previous value, have a recycler of their own that every type shares. Its capacity and maximum are the sums of the per-type capacities and maximums, and no configuration property sets them.

### Sparse array column source recyclers

Sparse array column sources store data in a multi-level structure and keep a separate recycler for each level of each type:

- The base recycler holds the blocks that store values.
- Level 2 holds arrays of those blocks.
- Level 1 holds arrays of level-2 arrays.
- Level 0, the top level, holds arrays of level-1 arrays.

You can configure each level independently to fit your access patterns. As with array column sources, each `capacity` property sets the capacity a recycler starts with and never shrinks below, and each `maximumCapacity` property sets the most it grows to.

| Property name                                   | Default value | Description                                                                                                                                                                                            |
| ----------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sparsearray.recycler.capacity.default`         | 1024          | Recycler capacity for sparse array types whose type-specific property is not set                                                                                                                       |
| `sparsearray.recycler.capacity.<type>`          | 1024          | Base recycler capacity for `<type>` sparse arrays, and the default capacity for its other levels. `<type>` is one of `boolean`, `byte`, `char`, `double`, `float`, `int`, `long`, `short`, or `object` |
| `sparsearray.recycler.capacity.<type>.2`        | 1024          | Level 2 recycler capacity for `<type>` sparse arrays                                                                                                                                                   |
| `sparsearray.recycler.capacity.<type>.1`        | 1024          | Level 1 recycler capacity for `<type>` sparse arrays                                                                                                                                                   |
| `sparsearray.recycler.capacity.<type>.0`        | 1024          | Level 0 (top) recycler capacity for `<type>` sparse arrays                                                                                                                                             |
| `sparsearray.recycler.maximumCapacity.default`  | unlimited     | Maximum recycler capacity for sparse array types whose type-specific property is not set                                                                                                               |
| `sparsearray.recycler.maximumCapacity.<type>`   | unlimited     | Maximum base recycler capacity for `<type>` sparse arrays, and the default maximum for its other levels. Never less than the type's base capacity                                                      |
| `sparsearray.recycler.maximumCapacity.<type>.2` | unlimited     | Maximum level 2 recycler capacity for `<type>` sparse arrays. Never less than its level 2 capacity                                                                                                     |
| `sparsearray.recycler.maximumCapacity.<type>.1` | unlimited     | Maximum level 1 recycler capacity for `<type>` sparse arrays. Never less than its level 1 capacity                                                                                                     |
| `sparsearray.recycler.maximumCapacity.<type>.0` | unlimited     | Maximum level 0 (top) recycler capacity for `<type>` sparse arrays. Never less than its level 0 capacity                                                                                               |

Sparse array column sources also keep "in use" bitmaps, which record which entries of a block hold a previous value. These bitmaps have a recycler of their own at each level, which every type shares. Each one's capacity and maximum are the sums of the per-type capacities and maximums at that level, and no configuration property sets them.

### How recyclers grow and shrink

A recycler grows only after a cycle has shown that it is too small:

- When a cycle returns arrays to a full recycler, the recycler rejects them and counts them.
- In a later cycle, each borrow that finds the recycler empty grows it by one, until it has grown by that count.
- Growth never exceeds what some cycle used, and never exceeds the configured maximum.

A recycler can shrink only at the end of a window. A window is a period long enough to span a whole cycle of every active [update graph](./dag.md#update-graph-cycles), the engine component that runs update cycles. Its length follows these rules:

- For each update graph, the engine measures the time between its two most recent cycle starts.
- A window lasts `SoftRecycler.windowMargin` times the longest of those times.
- `SoftRecycler.windowMillis` sets the shortest window.
- `SoftRecycler.maximumWindowMillis` sets the longest window.

At the end of a window:

- If the recycler emptied during the window, it keeps its capacity.
- Otherwise, it lowers its capacity by a fraction of the fewest arrays it held during the window, which are arrays it never needed, and discards any arrays it holds beyond the new capacity. `SoftRecycler.shrinkFraction` sets that fraction. The capacity never drops below the configured capacity.

Windows end only when an update graph starts a cycle, so recyclers in a process whose update graphs never cycle never shrink. The garbage collector still reclaims their arrays under memory pressure, because recyclers hold them through soft references.

| Property name                      | Default value | Description                                                                                                                                                                                                     |
| ---------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SoftRecycler.windowMillis`        | 1000          | The shortest window, in milliseconds. A window lasts this long whenever `SoftRecycler.windowMargin` times the longest time between an update graph's two most recent cycle starts is shorter than this          |
| `SoftRecycler.maximumWindowMillis` | 60000         | The longest window, in milliseconds. Once a window exceeds this length, the next cycle start of any update graph ends it, even if one update graph has a longer interval between cycles                         |
| `SoftRecycler.windowMargin`        | 1.5           | The length of a window, as a multiple of the longest time between an update graph's two most recent cycle starts                                                                                                |
| `SoftRecycler.shrinkFraction`      | 0.1           | The fraction, from 0 to 1, of the arrays the recycler did not lend during the window that it discards when the window ends. A value of 0 keeps the capacity once grown, and 1 shrinks to what the window needed |

### Tuning recyclers

Because recyclers adapt, most workloads need no tuning. The configured capacity sets how many arrays a recycler keeps through quiet periods, and the maximum bounds how many it keeps through busy ones.

- **Workloads that alternate busy and quiet cycles**: Raise `SoftRecycler.windowMillis`, or set `SoftRecycler.shrinkFraction` to 0, so that a recycler keeps what a busy cycle needed through the quiet cycles in between.
- **Memory-constrained environments**: Set a `maximumCapacity` to bound what a recycler holds, or raise `SoftRecycler.shrinkFraction` toward 1 so that a recycler discards unused arrays sooner.
- **Workloads dominated by a few types**: Raise those types' capacity or maximum while reducing others.

## Related documentation

- [Incremental update model](./table-update-model.md)
- [Deephaven's live directed acyclic graph (DAG)](./dag.md)
- [Parallelizing queries](./query-engine/parallelization.md)
- [Periodic Update Graph](./periodic-update-graph-configuration.md)
- [The Python-Java boundary and how it affects query efficiency](./python-java-boundary.md)
- [Aggregations](./aggregations.md)
- [What is Barrage?](./what-is-barrage.md)
- [Table types](./table-types.md)
- [Basic table operations](../getting-started/crash-course/table-ops.md)
- [Configuration properties reference](../how-to-guides/configuration/configuration-properties.md)
- [Create a Deephaven configuration file](../how-to-guides/configuration/config-file.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Predicate pushdown filtering](../how-to-guides/predicate-pushdown.md)
- [Filters](../how-to-guides/use-filters.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Inexact, time-series, and range joins](../how-to-guides/joins-timeseries-range.md)
- [Rollup tables](../how-to-guides/rollup-table.md)
- [Tree tables](../how-to-guides/tree-table.md)
- [Why do some queries run faster the second time they are run?](../reference/community-questions/query-memoization.md)
- [How efficient are my table selection operations? (Groovy)](/core/groovy/docs/reference/community-questions/selection-method-properties/)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`exact_join`](../reference/table-operations/join/exact-join.md)
- [`flatten`](../reference/table-operations/select/flatten.md)
- [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- [`join`](../reference/table-operations/join/join.md)
- [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`select`](../reference/table-operations/select/select.md)
- [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`sort_descending`](../reference/table-operations/sort/sort-descending.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`with_serial`](../reference/query-language/types/ConcurrencyControl.md#with_serial)
- [`Filter` Pydoc](/core/pydoc/code/deephaven.filters.html)
- [`Selectable` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
- [`ColumnSource` Javadoc](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
- [`QueryTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html)
- [`RowSet` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
- [`SoftRecycler` Javadoc](/core/javadoc/io/deephaven/util/SoftRecycler.html)
