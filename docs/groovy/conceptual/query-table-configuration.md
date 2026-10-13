---
title: Query table configuration
---

This guide discusses how to control various [`QueryTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) features that affect your Deephaven tables' latency and throughput.

## `QueryTable`

[`QueryTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) is Deephaven's primary implementation of our [Table API](../getting-started/crash-course/table-ops.md).

The following table lists the user-configurable properties. Properties that start with `QueryTable.` control table operations, and the `array.recycler.capacity.*` and `sparsearray.recycler.capacity.*` properties control memory pooling. To set a property, see [Configuration properties](../how-to-guides/configuration/configuration-properties.md).

| Category                                                            | Property                                                 | Default                                         |
| ------------------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------- |
| [Memoization](#memoization)                                         | `QueryTable.memoizeResults`                              | true                                            |
| [Redirection](#redirection)                                         | `QueryTable.redirectUpdate`                              | false                                           |
| [Redirection](#redirection)                                         | `QueryTable.redirectSelect`                              | false                                           |
| [Redirection](#redirection)                                         | `QueryTable.maximumStaticSelectMemoryOverhead`           | 1.1                                             |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForWhere`                        | true                                            |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForAggregation`                  | true                                            |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForJoins`                        | true                                            |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownDataIndex`               | false                                           |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false                                           |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownMergedTables`            | false                                           |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownDictionary`              | false                                           |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownSortedColumn`            | false                                           |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.dataIndexForWhereThreshold`                  | 0.25                                            |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.dictionaryForWhereThreshold`                 | 0.25                                            |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.disableParallelWhere`                        | false                                           |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.parallelWhereRowsPerSegment`                 | `1 << 16`                                       |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.parallelWhereSegments`                       | -1                                              |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.forceParallelWhere` (test-focused)           | false                                           |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.enableParallelSelectAndUpdate`               | true                                            |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.minimumParallelSelectRows`                   | `1L << 22`                                      |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.forceParallelSelectAndUpdate` (test-focused) | false                                           |
| [Parallel sorting](#parallel-sorting)                               | `QueryTable.parallelSort`                                | true                                            |
| [Parallel sorting](#parallel-sorting)                               | `QueryTable.minimumParallelSortRows`                     | `1L << 20`                                      |
| [Parallel sorting](#parallel-sorting)                               | `QueryTable.parallelSortSegmentSize`                     | `1L << 18`                                      |
| [Parallel snapshotting](#parallel-snapshotting)                     | `QueryTable.enableParallelSnapshot`                      | true                                            |
| [Parallel snapshotting](#parallel-snapshotting)                     | `QueryTable.minimumParallelSnapshotRows`                 | `1L << 20`                                      |
| [Ungroup operations](#ungroup-operations)                           | `QueryTable.minimumUngroupBase`                          | 10                                              |
| [SoftRecycler configuration](#softrecycler-configuration)           | `array.recycler.capacity.*`                              | 1024                                            |
| [SoftRecycler configuration](#softrecycler-configuration)           | `sparsearray.recycler.capacity.*`                        | [varies](#sparse-array-column-source-recyclers) |
| [Stateless by default](#stateless-by-default)                       | `QueryTable.statelessFiltersByDefault`                   | true                                            |
| [Stateless by default](#stateless-by-default)                       | `QueryTable.statelessSelectByDefault`                    | true                                            |
| [Stateless by default](#stateless-by-default)                       | `QueryTable.serialSelectImplicitBarriers`                | false                                           |

The sections below group the properties by the operation they affect.

## Memoization

Deephaven utilizes [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations to improve performance by eliminating duplicate work. See [Query Memoization](../reference/community-questions/query-memoization.md) for more details.

It can be beneficial to disable memoization when benchmarking or testing, as memoized results can hide true operational costs and skew performance metrics.

| Property Name               | Default Value | Description                        |
| --------------------------- | ------------- | ---------------------------------- |
| `QueryTable.memoizeResults` | true          | Enables memoizing table operations |

## Redirection

Every Deephaven table has a 63-bit row-key space. Each row key maps a logical row to its data. A column source stores the data of one column. Many column sources use a multi-level data layout to avoid allocating more resources than necessary to fulfill operational requirements. See [selection method properties](../reference/community-questions/selection-method-properties.md) for more details.

Redirection maps the column source of a parent table to the column source of an operation's result. A [sorted](../reference/table-operations/sort/sort.md) column, for example, is redirected from the original so that it presents rows in the sort order. Redirection can also turn a sparse row-key space into a flat, dense one. A table is flat when its row keys run from 0 to n-1 with no gaps.

| Property Name                                  | Default Value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueryTable.redirectUpdate`                    | false         | Forces non-flat refreshing [`update`](../reference/table-operations/select/update.md) operations to redirect despite the increased performance costs                                                                                                                                                                                                                                                                                                                                                                           |
| `QueryTable.redirectSelect`                    | false         | Forces non-flat refreshing [`select`](../reference/table-operations/select/select.md) operations to redirect despite the increased performance costs                                                                                                                                                                                                                                                                                                                                                                           |
| `QueryTable.maximumStaticSelectMemoryOverhead` | 1.1 (double)  | Applies to a static (non-refreshing) `select` on a non-flat table. The engine flattens the table first when its sparse row-key space would use more than this multiple of the memory blocks that a flat table needs. At `1.1`, the allowed overhead is 10%. A negative value never flattens. `0` always flattens. A value between `0` and `1` also always flattens, because a sparse row-key space never uses fewer blocks than a flat table. A static `update` that exceeds the limit uses redirection instead of flattening. |

## DataIndex

A Deephaven [data index](../how-to-guides/data-indexes.md) is an index that can improve the speed of [filtering operations](../how-to-guides/use-filters.md).

| Property Name                           | Default Value | Description                                                                                                                                                                                                                                                                                                      |
| --------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.useDataIndexForWhere`       | true          | Enables data index usage in [`where`](../reference/table-operations/filter/where.md) operations                                                                                                                                                                                                                  |
| `QueryTable.useDataIndexForAggregation` | true          | Enables data index usage in [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), [`selectDistinct`](../reference/table-operations/select/select-distinct.md), [rollup tables](../reference/table-operations/create/rollup.md), and [tree tables](../reference/table-operations/create/tree.md) |
| `QueryTable.useDataIndexForJoins`       | true          | Enables data index usage in [exact and relational joins](../how-to-guides/joins-exact-relational.md#which-method-should-you-use) and [time-series joins](../how-to-guides/joins-timeseries-range.md#which-method-should-you-use)                                                                                 |

The `QueryTable.disableWherePushdownDataIndex` and `QueryTable.dataIndexForWhereThreshold` properties also involve [data indexes](../how-to-guides/data-indexes.md). They are described in [Pushdown predicates with where](#pushdown-predicates-with-where).

## Pushdown predicates with `where`

[Predicate pushdown](../how-to-guides/predicate-pushdown.md) applies the conditions of a [`where`](../reference/table-operations/filter/where.md) filter as early as possible. Ideally the engine applies them at the data source, such as [Parquet](../how-to-guides/data-import-export/parquet-import.md) or another columnar format, before loading data into the system. Because the engine annotates source reads with the predicates, it pulls in only the rows that satisfy them, which reduces I/O and improves performance.

| Property Name                                            | Default Value | Description                                                                                                                                                                                                          |
| -------------------------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.disableWherePushdownDataIndex`               | false         | Disables the use of a [data index](../how-to-guides/data-indexes.md) within the pushdown predicates of [`where`](../reference/table-operations/filter/where.md)                                                      |
| `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false         | Disables the usage of [Parquet](../how-to-guides/data-import-export/parquet-import.md) [row group metadata](../how-to-guides/predicate-pushdown.md#parquet-row-group-metadata) during pushdown filtering             |
| `QueryTable.disableWherePushdownMergedTables`            | false         | Disables predicate pushdown when filtering [merged tables](../how-to-guides/merge-tables.md)                                                                                                                         |
| `QueryTable.disableWherePushdownDictionary`              | false         | Disables [dictionary-encoding predicate pushdown](../how-to-guides/predicate-pushdown.md#dictionary-encoding) operations                                                                                             |
| `QueryTable.disableWherePushdownSortedColumn`            | false         | Disables the use of [sorted column binary search](../how-to-guides/predicate-pushdown.md#sorted-data-binary-search) during pushdown filtering                                                                        |
| `QueryTable.dataIndexForWhereThreshold`                  | 0.25 (double) | The maximum size of a [data index](../how-to-guides/data-indexes.md) table, as a fraction of the rows remaining to be filtered, for the index to be used by [`where`](../reference/table-operations/filter/where.md) |
| `QueryTable.dictionaryForWhereThreshold`                 | 0.25 (double) | The [dictionary](../how-to-guides/predicate-pushdown.md#dictionary-encoding) size, as a fraction of the rows remaining to be filtered, that the dictionary must fall below for pushdown filtering to use it          |

The two `*ForWhereThreshold` properties are cost heuristics, not on/off switches. Scanning a [data index](../how-to-guides/data-indexes.md) table or a [dictionary](../how-to-guides/predicate-pushdown.md#dictionary-encoding) takes time proportional to its size, so the engine scans one only when it is small relative to the rows it can eliminate. When the engine skips the scan, the skipped rows are not eliminated at that stage. A later stage filters them directly.

- Raise a threshold toward `1.0` to apply the optimization to columns with more distinct values.
- Set a threshold to `0` to effectively disable the technique.

For more details, see [Predicate pushdown filtering](../how-to-guides/predicate-pushdown.md).

## Parallel processing with `where`

The engine does not parallelize [`where`](../reference/table-operations/filter/where.md) operations until the filter's input size exceeds twice `QueryTable.parallelWhereRowsPerSegment`, about 131,072 rows with defaults. The engine needs enough rows to fill more than one segment before it splits the work. This avoids the overhead of using threads for small operations. The engine parallelizes only filters that it treats as stateless. See [Stateless by default](#stateless-by-default).

The input that the engine counts depends on when the [filter](../reference/table-operations/filter/where.md) runs:

- **Initial filtering**: the rows reaching the [filter](../reference/table-operations/filter/where.md). The first filter sees the whole parent table. Each later filter sees only the rows that the earlier filters kept.
- **Updates to a refreshing table**: that cycle's added and modified rows. A large table that receives small updates may not parallelize its per-update [filtering](../reference/table-operations/filter/where.md) even though its initial filtering did.
- **Refilters**: the broader set of rows that the engine re-evaluates. A refilter re-runs a [filter](../reference/table-operations/filter/where.md) over rows it already evaluated. A [`whereIn`](../reference/table-operations/filter/where-in.md) filter refilters when its set table changes, for example.

`QueryTable.parallelWhereSegments` sets the number of segments.

- At its default of `-1`, the engine derives the segment count from the available worker threads. [Initial filtering](../reference/table-operations/filter/where.md) uses the threads of the operation initialization thread pool. [Filtering](../reference/table-operations/filter/where.md) of a refreshing table's updates uses the threads of the [update graph](./dag.md) thread pool.
- A positive value sets a fixed number of segments.

Tune these properties to avoid unnecessary parallelism when the overhead exceeds the potential gains.

| Property Name                                  | Default Value | Description                                                                                                                                                        |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueryTable.disableParallelWhere`              | false         | Disables parallelized optimizations for [`where`](../reference/table-operations/filter/where.md) operations                                                        |
| `QueryTable.parallelWhereRowsPerSegment`       | `1 << 16`     | The number of rows per segment when the number of segments is not fixed                                                                                            |
| `QueryTable.parallelWhereSegments`             | -1            | The number of segments to use when dividing all work equally into a fixed number of tasks. At `-1`, the engine derives the count from the available worker threads |
| `QueryTable.forceParallelWhere` (test-focused) | false         | Forces [`where`](../reference/table-operations/filter/where.md) operations to parallelize even when row requirements are not met                                   |

## Parallel processing with `select`

The [`QueryTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) operations [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) can parallelize work during two separate phases of each operation:

- **Initial creation of the table**: the engine parallelizes the initial computation of the resulting table state.
- **Updates**: when the parent table updates, the engine parallelizes processing of that cycle's added and modified rows.

The engine does not parallelize [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) until the rows to process reach `QueryTable.minimumParallelSelectRows`. On initialization, that is the whole table. On each update, it is that cycle's added and modified rows. Tune this threshold to avoid unnecessary parallelism when the overhead exceeds the potential gains. The engine parallelizes only formulas that it treats as stateless. See [Stateless by default](#stateless-by-default).

| Property Name                                            | Default Value | Description                                                                                                                                                                                       |
| -------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSelectAndUpdate`               | true          | Enables parallelized optimizations for [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) operations                       |
| `QueryTable.minimumParallelSelectRows`                   | `1L << 22`    | The minimum number of rows required to enable parallel [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) operations       |
| `QueryTable.forceParallelSelectAndUpdate` (test-focused) | false         | Forces [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) operations to parallelize even when row requirements are not met |

## Parallel sorting

[`sort`](../reference/table-operations/sort/sort.md) can parallelize three steps:

- Reading the column values that the [sort](../reference/table-operations/sort/sort.md) compares.
- [Sorting](../reference/table-operations/sort/sort.md) separate segments of the rows, then merging the sorted segments in pairs.
- Gathering the row keys in their sorted order.

The engine does not parallelize a [`sort`](../reference/table-operations/sort/sort.md) until the number of rows being sorted reaches `QueryTable.minimumParallelSortRows`. That number is the table's row count, or the row count of the [data index](../how-to-guides/data-indexes.md) when the sort uses one. Below the threshold, dividing the work into segments costs more than the work itself, so the sort runs entirely on the calling thread.

Two more properties control sorting:

- `QueryTable.parallelSort`: set it to `false` to disable [sort](../reference/table-operations/sort/sort.md) parallelization entirely, regardless of table size.
- `QueryTable.parallelSortSegmentSize`: the minimum number of rows in each segment. The engine uses fewer, larger segments rather than splitting the [sort](../reference/table-operations/sort/sort.md) into segments smaller than that size.

| Property Name                        | Default Value | Description                                                                                                                  |
| ------------------------------------ | ------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.parallelSort`            | true          | Whether the engine may parallelize [sorts](../reference/table-operations/sort/sort.md) at all                                |
| `QueryTable.minimumParallelSortRows` | `1L << 20`    | The minimum number of rows in a [sort](../reference/table-operations/sort/sort.md) for which the engine may parallelize work |
| `QueryTable.parallelSortSegmentSize` | `1L << 18`    | The minimum number of rows in each segment of a parallel [sort](../reference/table-operations/sort/sort.md)                  |

## Parallel snapshotting

This section covers the snapshots that the engine takes of a table's rows and columns to answer Barrage subscription requests. It does not cover the `snapshot` table operation.

[Barrage](./what-is-barrage.md) clients, including our JavaScript implementation used on the web, fulfill subscription requests by snapshotting the required rows and columns. When the table is refreshing, they also listen for relevant changes.

Parallel snapshotting is a feature that parallelizes the snapshot across columns. If those columns are slow to access, parallel snapshotting reduces latency. However, parallel snapshotting may open many file handles to the same data source.

The engine parallelizes a snapshot when it covers more than one non-empty column and at least one of the following holds:

- The snapshot includes at least `QueryTable.minimumParallelSnapshotRows` rows.
- The snapshot reads a column that is not held in memory, such as a column backed by Parquet.

Tune the row threshold to avoid unnecessary parallelism when the overhead exceeds the potential gains.

| Property Name                            | Default Value | Description                                                                                                                   |
| ---------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSnapshot`      | true          | Enables parallelized optimizations for snapshotting operations, such as [Barrage](./what-is-barrage.md) subscription requests |
| `QueryTable.minimumParallelSnapshotRows` | `1L << 20`    | The minimum number of rows required to enable parallel snapshotting operations                                                |

## Ungroup operations

The [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) table operation can expand one row into multiple rows. `QueryTable.minimumUngroupBase` controls the initial allocation used by `ungroup`.

| Property Name                   | Default Value | Description                                                                                                                                                                |
| ------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.minimumUngroupBase` | 10            | The minimum base used for [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) output row allocation. Each input row starts with `2^base` output rows |

## `SoftRecycler` configuration

Deephaven uses [`SoftRecycler`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/SoftRecycler.html) objects to manage memory for array and sparse array column sources on refreshing tables that track previous values. When a modified block needs memory to record previous values, the engine borrows it from the recycler rather than allocating fresh memory on each cycle, and returns it at the end of the update cycle. These pools can improve performance and reduce garbage collection pressure.

A recycler's capacity is how many arrays it holds. You can configure the capacity per type to tune memory usage for your workload.

### Array column source recyclers

Array-backed column sources (dense arrays) use [`SoftRecycler`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/SoftRecycler.html) objects to manage blocks of data for each primitive type.

| Property Name                     | Default Value | Description                                                                                                                                |
| --------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `array.recycler.capacity.default` | 1024          | Default recycler capacity for all array types (used if type-specific property is not set)                                                  |
| `array.recycler.capacity.boolean` | 1024          | Recycler capacity for boolean array blocks                                                                                                 |
| `array.recycler.capacity.byte`    | 1024          | Recycler capacity for byte array blocks                                                                                                    |
| `array.recycler.capacity.char`    | 1024          | Recycler capacity for character array blocks                                                                                               |
| `array.recycler.capacity.double`  | 1024          | Recycler capacity for double array blocks                                                                                                  |
| `array.recycler.capacity.float`   | 1024          | Recycler capacity for float array blocks                                                                                                   |
| `array.recycler.capacity.int`     | 1024          | Recycler capacity for integer array blocks                                                                                                 |
| `array.recycler.capacity.long`    | 1024          | Recycler capacity for long array blocks                                                                                                    |
| `array.recycler.capacity.short`   | 1024          | Recycler capacity for short array blocks                                                                                                   |
| `array.recycler.capacity.object`  | 1024          | Recycler capacity for object array blocks                                                                                                  |
| `array.recycler.capacity.inuse`   | 1024          | Recycler capacity for "in use" bitmap blocks. Defaults to the maximum capacity of the other array types, and should be at least that value |

### Sparse array column source recyclers

Sparse array column sources use a multi-level hierarchical structure and maintain separate recyclers at each level. Each level can be configured independently to optimize memory usage for your access patterns. A property without a level suffix sets the capacity of the lowest level, which holds the blocks of data. Levels 2, 1, and 0 are successively higher in the hierarchy, and level 0 is the top.

| Property Name                             | Default Value | Description                                                                                                               |
| ----------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `sparsearray.recycler.capacity.default`   | 1024          | Default recycler capacity for all sparse array types                                                                      |
| `sparsearray.recycler.capacity.boolean`   | 1024          | Base recycler capacity for boolean sparse arrays                                                                          |
| `sparsearray.recycler.capacity.byte`      | 1024          | Base recycler capacity for byte sparse arrays                                                                             |
| `sparsearray.recycler.capacity.char`      | 1024          | Base recycler capacity for character sparse arrays                                                                        |
| `sparsearray.recycler.capacity.double`    | 1024          | Base recycler capacity for double sparse arrays                                                                           |
| `sparsearray.recycler.capacity.float`     | 1024          | Base recycler capacity for float sparse arrays                                                                            |
| `sparsearray.recycler.capacity.int`       | 1024          | Base recycler capacity for integer sparse arrays                                                                          |
| `sparsearray.recycler.capacity.long`      | 1024          | Base recycler capacity for long sparse arrays                                                                             |
| `sparsearray.recycler.capacity.short`     | 1024          | Base recycler capacity for short sparse arrays                                                                            |
| `sparsearray.recycler.capacity.object`    | 1024          | Base recycler capacity for object sparse arrays                                                                           |
| `sparsearray.recycler.capacity.boolean.2` | 1024          | Level 2 recycler capacity for boolean sparse arrays                                                                       |
| `sparsearray.recycler.capacity.byte.2`    | 1024          | Level 2 recycler capacity for byte sparse arrays                                                                          |
| `sparsearray.recycler.capacity.char.2`    | 1024          | Level 2 recycler capacity for character sparse arrays                                                                     |
| `sparsearray.recycler.capacity.double.2`  | 1024          | Level 2 recycler capacity for double sparse arrays                                                                        |
| `sparsearray.recycler.capacity.float.2`   | 1024          | Level 2 recycler capacity for float sparse arrays                                                                         |
| `sparsearray.recycler.capacity.int.2`     | 1024          | Level 2 recycler capacity for integer sparse arrays                                                                       |
| `sparsearray.recycler.capacity.long.2`    | 1024          | Level 2 recycler capacity for long sparse arrays                                                                          |
| `sparsearray.recycler.capacity.short.2`   | 1024          | Level 2 recycler capacity for short sparse arrays                                                                         |
| `sparsearray.recycler.capacity.object.2`  | 1024          | Level 2 recycler capacity for object sparse arrays                                                                        |
| `sparsearray.recycler.capacity.boolean.1` | 1024          | Level 1 recycler capacity for boolean sparse arrays                                                                       |
| `sparsearray.recycler.capacity.byte.1`    | 1024          | Level 1 recycler capacity for byte sparse arrays                                                                          |
| `sparsearray.recycler.capacity.char.1`    | 1024          | Level 1 recycler capacity for character sparse arrays                                                                     |
| `sparsearray.recycler.capacity.double.1`  | 1024          | Level 1 recycler capacity for double sparse arrays                                                                        |
| `sparsearray.recycler.capacity.float.1`   | 1024          | Level 1 recycler capacity for float sparse arrays                                                                         |
| `sparsearray.recycler.capacity.int.1`     | 1024          | Level 1 recycler capacity for integer sparse arrays                                                                       |
| `sparsearray.recycler.capacity.long.1`    | 1024          | Level 1 recycler capacity for long sparse arrays                                                                          |
| `sparsearray.recycler.capacity.short.1`   | 1024          | Level 1 recycler capacity for short sparse arrays                                                                         |
| `sparsearray.recycler.capacity.object.1`  | 1024          | Level 1 recycler capacity for object sparse arrays                                                                        |
| `sparsearray.recycler.capacity.boolean.0` | 1024          | Level 0 (top) recycler capacity for boolean sparse arrays                                                                 |
| `sparsearray.recycler.capacity.byte.0`    | 1024          | Level 0 (top) recycler capacity for byte sparse arrays                                                                    |
| `sparsearray.recycler.capacity.char.0`    | 1024          | Level 0 (top) recycler capacity for character sparse arrays                                                               |
| `sparsearray.recycler.capacity.double.0`  | 1024          | Level 0 (top) recycler capacity for double sparse arrays                                                                  |
| `sparsearray.recycler.capacity.float.0`   | 1024          | Level 0 (top) recycler capacity for float sparse arrays                                                                   |
| `sparsearray.recycler.capacity.int.0`     | 1024          | Level 0 (top) recycler capacity for integer sparse arrays                                                                 |
| `sparsearray.recycler.capacity.long.0`    | 1024          | Level 0 (top) recycler capacity for long sparse arrays                                                                    |
| `sparsearray.recycler.capacity.short.0`   | 1024          | Level 0 (top) recycler capacity for short sparse arrays                                                                   |
| `sparsearray.recycler.capacity.object.0`  | 1024          | Level 0 (top) recycler capacity for object sparse arrays                                                                  |
| `sparsearray.recycler.capacity.inuse`     | 9216          | Recycler capacity for "in use" bitmap blocks at the lowest level. Defaults to the sum of the base capacities of all types |
| `sparsearray.recycler.capacity.inuse.2`   | 1024          | Recycler capacity for "in use" bitmap blocks at level 2. Defaults to the maximum level 2 capacity of any type             |
| `sparsearray.recycler.capacity.inuse.1`   | 1024          | Recycler capacity for "in use" bitmap blocks at level 1. Defaults to the maximum level 1 capacity of any type             |
| `sparsearray.recycler.capacity.inuse.0`   | 1024          | Recycler capacity for "in use" bitmap blocks at level 0 (top). Defaults to the maximum level 0 capacity of any type       |

### Tuning `SoftRecycler` capacity

The recycler capacity determines how many array blocks are kept in memory for potential reuse. Increasing capacity can improve performance if your workload uses more blocks within an update cycle than the recycler can hold, at the cost of higher baseline memory usage. Decreasing capacity reduces baseline memory requirements, but may increase garbage collection.

- **High-throughput workloads**: increase capacities to reduce allocation and deallocation overhead.
- **Uneven type usage**: increase the capacity of the types you use most and reduce the others.

## Stateless by default

Starting in Deephaven 41.0, the engine assumes by default that [`where`](../reference/table-operations/filter/where.md) filters and [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) formulas are [stateless](./query-engine/parallelization.md#when-parallelization-is-safe-by-default). A stateless operation depends only on the values in its own row, so the engine can run it in parallel. To run one serially instead, call [`withSerial`](../reference/query-language/types/Selectable.md#withserial) on the [`Filter`](../reference/query-language/types/Filter.md) or [`Selectable`](../reference/query-language/types/Selectable.md). See [Serial execution](./query-engine/parallelization.md#serial-execution) for details.

| Property Name                             | Default Value | Description                                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.statelessFiltersByDefault`    | true          | Enables the engine to assume that [`where`](../reference/table-operations/filter/where.md) filters are stateless by default                                                                                                                                                                                                                                                         |
| `QueryTable.statelessSelectByDefault`     | true          | Enables the engine to assume that [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) formulas are stateless by default                                                                                                                                                                                       |
| `QueryTable.serialSelectImplicitBarriers` | false         | Whether Deephaven adds an [implicit barrier](./query-engine/parallelization.md#implicit-barriers) to each serial [`Selectable`](../reference/query-language/types/Selectable.md) in the same [`select`](../reference/table-operations/select/select.md) or [`update`](../reference/table-operations/select/update.md), so that it runs only after all earlier serial columns finish |

A serial [`Selectable`](../reference/query-language/types/Selectable.md) is one that is not stateless. The default for `QueryTable.serialSelectImplicitBarriers` is the opposite of `QueryTable.statelessSelectByDefault`. When stateless select is on, the default is `false` and serial columns have no implicit ordering. When stateless select is off, the default is `true`.

For more details, see [`ConcurrencyControl`](../reference/query-language/types/ConcurrencyControl.md) and its [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html).

## Related documentation

- [`QueryTable` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html)
- [Table API](../getting-started/crash-course/table-ops.md)
- [Incremental update model](./table-update-model.md)
- [Query Memoization](../reference/community-questions/query-memoization.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Parallelizing queries](./query-engine/parallelization.md)
