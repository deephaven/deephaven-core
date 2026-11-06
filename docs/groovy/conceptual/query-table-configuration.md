---
title: Query table configuration
---

This guide discusses how to control various `QueryTable` features that affect your Deephaven tables' latency and throughput.

## `QueryTable`

[`QueryTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html) is Deephaven's primary implementation of our [Table API](../getting-started/crash-course/table-ops.md).

The `QueryTable` has the following user-configurable properties:

| Category                                                            | Property                                                 | Default    |
| ------------------------------------------------------------------- | -------------------------------------------------------- | ---------- |
| [Memoization](#memoization)                                         | `QueryTable.memoizeResults`                              | true       |
| [Memoization](#memoization)                                         | `QueryTable.redirectUpdate`                              | false      |
| [Memoization](#memoization)                                         | `QueryTable.redirectSelect`                              | false      |
| [Memoization](#memoization)                                         | `QueryTable.maximumStaticSelectMemoryOverhead`           | 1.1        |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForWhere`                        | true       |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForAggregation`                  | true       |
| [DataIndex](#dataindex)                                             | `QueryTable.useDataIndexForJoins`                        | true       |
| [DataIndex](#dataindex)                                             | `QueryTable.dataIndexForWhereThreshold`                  | 0.0625     |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownDataIndex`               | false      |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false      |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownMergedTables`            | false      |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownDictionary`              | false      |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.disableWherePushdownSortedColumn`            | false      |
| [Pushdown predicates with where](#pushdown-predicates-with-where)   | `QueryTable.dictionaryForWhereThreshold`                 | 0.25       |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.disableParallelWhere`                        | false      |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.parallelWhereRowsPerSegment`                 | `1 << 16`  |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.parallelWhereSegments`                       | -1         |
| [Parallel processing with where](#parallel-processing-with-where)   | `QueryTable.forceParallelWhere` (test-focused)           | false      |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.enableParallelSelectAndUpdate`               | true       |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.minimumParallelSelectRows`                   | `1L << 22` |
| [Parallel processing with select](#parallel-processing-with-select) | `QueryTable.forceParallelSelectAndUpdate` (test-focused) | false      |
| [Parallel snapshotting](#parallel-snapshotting)                     | `QueryTable.enableParallelSnapshot`                      | true       |
| [Parallel snapshotting](#parallel-snapshotting)                     | `QueryTable.minimumParallelSnapshotRows`                 | `1L << 20` |
| [Ungroup operations](#ungroup-operations)                           | `QueryTable.minimumUngroupBase`                          | 10         |
| [SoftRecycler configuration](#softrecycler-configuration)           | `array.recycler.capacity.*`                              | 1024       |
| [SoftRecycler configuration](#softrecycler-configuration)           | `sparsearray.recycler.capacity.*`                        | 1024       |
| [SoftRecycler configuration](#softrecycler-configuration)           | `array.recycler.maximumCapacity.*`                       | unlimited  |
| [SoftRecycler configuration](#softrecycler-configuration)           | `sparsearray.recycler.maximumCapacity.*`                 | unlimited  |
| [SoftRecycler configuration](#softrecycler-configuration)           | `SoftRecycler.windowMillis`                              | 1000       |
| [SoftRecycler configuration](#softrecycler-configuration)           | `SoftRecycler.maximumWindowMillis`                       | 60000      |
| [SoftRecycler configuration](#softrecycler-configuration)           | `SoftRecycler.windowMargin`                              | 1.5        |
| [SoftRecycler configuration](#softrecycler-configuration)           | `SoftRecycler.shrinkFraction`                            | 0.1        |
| [Stateless filters by default](#stateless-by-default)               | `QueryTable.statelessFiltersByDefault`                   | false      |
| [Stateless select by default](#stateless-by-default)                | `QueryTable.statelessSelectByDefault`                    | false      |

Each property is described below, roughly categorized by similarity.

## Memoization

Deephaven utilizes [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations to improve performance by eliminating duplicate work. See [Query Memoization](../reference/community-questions/query-memoization.md) for more details.

It can be beneficial to disable memoization when benchmarking or testing, as memoized results can hide true operational costs and skew performance metrics.

| Property Name               | Default Value | Description                        |
| --------------------------- | ------------- | ---------------------------------- |
| `QueryTable.memoizeResults` | true          | Enables memoizing table operations |

## Redirection

Deephaven Tables maintain a 63-bit keyspace that maps a logical row in row-key space to its data. Many of Deephaven's column sources use a multi-level data layout to avoid allocating more resources than necessary to fulfill operational requirements. See [selection method properties](../reference/community-questions/selection-method-properties.md) for more details.

Redirection is a mapping between a parent column source and the resulting column source for a given operation. A sorted column, for example, is redirected from the original to present the rows in the targeted sort order. Redirection may also flatten from a sparse keyspace to a flat and dense keyspace.

| Property Name                                  | Default Value | Description                                                                                                   |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------- |
| `QueryTable.redirectUpdate`                    | false         | Forces non-flat refreshing `QueryTable#update` operations to redirect despite the increased performance costs |
| `QueryTable.redirectSelect`                    | false         | Forces non-flat refreshing `QueryTable#select` operations to redirect despite the increased performance costs |
| `QueryTable.maximumStaticSelectMemoryOverhead` | 1.1 (double)  | The maximum overhead as a fraction (e.g. 1.1 is 10% overhead; always sparse if < 0, never sparse if 0)        |

## DataIndex

A Deephaven [DataIndex](../how-to-guides/data-indexes.md) is an index that can improve the speed of filtering operations.

| Property Name                              | Default Value   | Description                                                                                                                                                                                                         |
| ------------------------------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.useDataIndexForWhere`          | true            | Enables data index usage in `QueryTable#where` operations                                                                                                                                                           |
| `QueryTable.useDataIndexForAggregation`    | true            | Enables data index usage in `QueryTable#aggBy`, `QueryTable#selectDistinct`, within [rollup-tables](../reference/table-operations/create/rollup.md) and [tree-tables](../reference/table-operations/create/tree.md) |
| `QueryTable.useDataIndexForJoins`          | true            | Enables data index usage in [Deephaven Joins](../how-to-guides/joins-timeseries-range.md#which-method-should-you-use)                                                                                               |
| `QueryTable.disableWherePushdownDataIndex` | false           | Disables data index usage within [where's pushdown predicates](#pushdown-predicates-with-where)                                                                                                                     |
| `QueryTable.dataIndexForWhereThreshold`    | 0.0625 (double) | The maximum size of a data index table, as a fraction of the rows remaining to be filtered, for the index to be used by `where`                                                                                     |

## Pushdown predicates with `where`

Pushdown predicates refer to the mechanism whereby filtering conditions are applied as early as possible, ideally at the data source (e.g., Parquet or other columnar formats), before loading data into the system. By annotating source reads with predicates, the engine pulls in only the rows that satisfy the conditions, significantly reducing I/O and improving performance.

| Property Name                                            | Default Value   | Description                                                                                                                                    |
| -------------------------------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.disableWherePushdownDataIndex`               | false           | Disables the use of [data index](../how-to-guides/data-indexes.md) within where's pushdown predicates                                          |
| `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false           | Disables the usage of Parquet row group metadata during pushdown filtering                                                                     |
| `QueryTable.disableWherePushdownMergedTables`            | false           | Disables predicate pushdown when filtering merged tables                                                                                       |
| `QueryTable.disableWherePushdownDictionary`              | false           | Disables dictionary-encoding predicate pushdown operations                                                                                     |
| `QueryTable.disableWherePushdownSortedColumn`            | false           | Disables the use of sorted column binary search during pushdown filtering                                                                      |
| `QueryTable.dataIndexForWhereThreshold`                  | 0.0625 (double) | The maximum size of a data index table, as a fraction of the rows remaining to be filtered, for the index to be used by `where`                |
| `QueryTable.dictionaryForWhereThreshold`                 | 0.25 (double)   | The dictionary size, as a fraction of the rows remaining to be filtered, that the dictionary must fall below for push-down filtering to use it |

The two `*ForWhereThreshold` properties are cost heuristics rather than on/off switches. Scanning a data index table or a dictionary costs time proportional to its size, so the engine only does so when that structure is small relative to the rows it can eliminate. When the index table or dictionary exceeds its threshold, the engine does not use it, and the rows are filtered normally. Raise the value toward `1.0` to apply these optimizations to columns with more distinct values, or set it to `0` to effectively disable the technique.

For more details, see [Predicate pushdown filtering](../how-to-guides/predicate-pushdown.md).

## Parallel processing with `where`

Parallelism for `where` operations is not enabled until the parent's size exceeds `QueryTable.parallelWhereRowsPerSegment` rows. This avoids the overhead of using threads for small operations. For tables larger than this threshold, the `where` operation uses a fixed number of parallel segments defined by `QueryTable.parallelWhereSegments`. These parameters can be tuned to avoid unnecessary parallelism when the overhead exceeds potential gains.

| Property Name                                  | Default Value | Description                                                                                                               |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.disableParallelWhere`              | false         | Disables parallel evaluation of `where` filters                                                                           |
| `QueryTable.parallelWhereRowsPerSegment`       | `1 << 16`     | The number of rows per segment when the number of segments is not fixed                                                   |
| `QueryTable.parallelWhereSegments`             | -1            | The number of segments to use when dividing all work equally into a fixed number of tasks; -1 implies one thread per core |
| `QueryTable.forceParallelWhere` (test-focused) | false         | Forces Where operations to parallelize even when row requirements are not met                                             |

## Parallel processing with `select`

The `QueryTable` operations `select` and `update` have performance enhancements that try to take advantage of parallelism during two separate phases of each table operation invocation. The first opportunity for parallelism is on the initial creation of the table. The engine will parallelize the initial computation of the resulting table state. The second opportunity for parallelism is when the operation's parent-table listener is notified that the parent was updated.

Parallelism for `select` operations is not enabled until the parent's size exceeds `QueryTable.minimumParallelSelectRows` rows. This can be tuned to avoid unnecessary parallelism (e.g., when the overhead exceeds potential gains).

| Property Name                                            | Default Value | Description                                                                                   |
| -------------------------------------------------------- | ------------- | --------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSelectAndUpdate`               | true          | Enables parallelized optimizations for `QueryTable#select` and `QueryTable#update` operations |
| `QueryTable.minimumParallelSelectRows`                   | `1L << 22`    | The minimum number of rows required to enable parallel select and update operations           |
| `QueryTable.forceParallelSelectAndUpdate` (test-focused) | false         | Forces Select and Update operations to parallelize even when row requirements are not met     |

## Parallel snapshotting

Barrage clients, including our JavaScript implementation used on the web, fulfill subscription requests by snapshotting the required rows and columns in addition to listening for relevant changes when the table is refreshing. Parallel snapshotting is a feature that parallelizes this process across columns. If those columns are slow to access then parallel snapshotting will greatly reduce latency. However, parallel snapshotting may open many file handles to the same data source.

Parallel snapshotting is not enabled until the snapshot size exceeds `QueryTable.minimumParallelSnapshotRows` rows. This can be tuned to avoid unnecessary parallelism when the overhead exceeds potential gains.

| Property Name                            | Default Value | Description                                                                                           |
| ---------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelSnapshot`      | true          | Enables parallelized optimizations for snapshotting operations, such as Barrage subscription requests |
| `QueryTable.minimumParallelSnapshotRows` | `1L << 20`    | The minimum number of rows required to enable parallel snapshotting operations                        |

## Ungroup operations

The `ungroup` table operation can expand one row into multiple rows. `QueryTable.minimumUngroupBase` controls the initial allocation used by `ungroup`.

| Property Name                   | Default Value | Description                                                                  |
| ------------------------------- | ------------- | ---------------------------------------------------------------------------- |
| `QueryTable.minimumUngroupBase` | 10            | The minimum base used for ungroup output row allocation (uses `2^base` rows) |

## `SoftRecycler` configuration

Deephaven uses [`SoftRecycler`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/SoftRecycler.html) objects to manage memory for array and sparse array column sources. These column sources must maintain previous values during an update graph cycle. Rather than allocating fresh memory on each cycle, when memory is needed to record previous values it is borrowed from the recycler and returned at the end of the update cycle. These pools can improve performance and reduce garbage collection pressure.

A recycler's capacity is the most arrays it holds at once. Each recycler adapts its capacity to its traffic: it starts at a configured capacity, grows when a cycle needs more arrays than it holds, and shrinks back toward the configured capacity when arrays go unused, but never below it and never above a configured maximum. Both values can be set per element type, and the [growth and shrink rules](#how-recyclers-grow-and-shrink) are described below.

### Array column source recyclers

Array-backed column sources (dense arrays) use SoftRecyclers to manage blocks of data for each primitive type. Each `capacity` property sets the capacity a recycler starts with and never shrinks below, and each `maximumCapacity` property sets the most it grows to.

| Property Name                            | Default Value | Description                                                                                                                                                                                         |
| ---------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `array.recycler.capacity.default`        | 1024          | Default recycler capacity for all array types (used if type-specific property is not set)                                                                                                           |
| `array.recycler.capacity.boolean`        | 1024          | Recycler capacity for boolean array blocks                                                                                                                                                          |
| `array.recycler.capacity.byte`           | 1024          | Recycler capacity for byte array blocks                                                                                                                                                             |
| `array.recycler.capacity.char`           | 1024          | Recycler capacity for character array blocks                                                                                                                                                        |
| `array.recycler.capacity.double`         | 1024          | Recycler capacity for double array blocks                                                                                                                                                           |
| `array.recycler.capacity.float`          | 1024          | Recycler capacity for float array blocks                                                                                                                                                            |
| `array.recycler.capacity.int`            | 1024          | Recycler capacity for integer array blocks                                                                                                                                                          |
| `array.recycler.capacity.long`           | 1024          | Recycler capacity for long array blocks                                                                                                                                                             |
| `array.recycler.capacity.short`          | 1024          | Recycler capacity for short array blocks                                                                                                                                                            |
| `array.recycler.capacity.object`         | 1024          | Recycler capacity for object array blocks                                                                                                                                                           |
| `array.recycler.maximumCapacity.default` | unlimited     | Default maximum recycler capacity for all array types (used if type-specific property is not set)                                                                                                   |
| `array.recycler.maximumCapacity.<type>`  | unlimited     | Maximum recycler capacity for blocks of `<type>` — one of `boolean`, `byte`, `char`, `double`, `float`, `int`, `long`, `short`, or `object` — never less than that type's `array.recycler.capacity` |

The "in use" bitmaps, which record which entries of a block hold a previous value, have a recycler of their own that every type shares. Its capacity and maximum are the sums of the types' values, and it has no properties.

### Sparse array column source recyclers

Sparse array column sources use a multi-level hierarchical structure and maintain separate recyclers at each level. Each level can be configured independently to optimize memory usage for your access patterns. As for array column sources, each `capacity` property sets the capacity a recycler starts with and never shrinks below, and each `maximumCapacity` property sets the most it grows to.

| Property Name                                   | Default Value | Description                                                                                                                                       |
| ----------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sparsearray.recycler.capacity.default`         | 1024          | Default recycler capacity for all sparse array types                                                                                              |
| `sparsearray.recycler.capacity.boolean`         | 1024          | Base recycler capacity for boolean sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.byte`            | 1024          | Base recycler capacity for byte sparse arrays                                                                                                     |
| `sparsearray.recycler.capacity.char`            | 1024          | Base recycler capacity for character sparse arrays                                                                                                |
| `sparsearray.recycler.capacity.double`          | 1024          | Base recycler capacity for double sparse arrays                                                                                                   |
| `sparsearray.recycler.capacity.float`           | 1024          | Base recycler capacity for float sparse arrays                                                                                                    |
| `sparsearray.recycler.capacity.int`             | 1024          | Base recycler capacity for integer sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.long`            | 1024          | Base recycler capacity for long sparse arrays                                                                                                     |
| `sparsearray.recycler.capacity.short`           | 1024          | Base recycler capacity for short sparse arrays                                                                                                    |
| `sparsearray.recycler.capacity.object`          | 1024          | Base recycler capacity for object sparse arrays                                                                                                   |
| `sparsearray.recycler.capacity.boolean.2`       | 1024          | Level 2 recycler capacity for boolean sparse arrays                                                                                               |
| `sparsearray.recycler.capacity.byte.2`          | 1024          | Level 2 recycler capacity for byte sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.char.2`          | 1024          | Level 2 recycler capacity for character sparse arrays                                                                                             |
| `sparsearray.recycler.capacity.double.2`        | 1024          | Level 2 recycler capacity for double sparse arrays                                                                                                |
| `sparsearray.recycler.capacity.float.2`         | 1024          | Level 2 recycler capacity for float sparse arrays                                                                                                 |
| `sparsearray.recycler.capacity.int.2`           | 1024          | Level 2 recycler capacity for integer sparse arrays                                                                                               |
| `sparsearray.recycler.capacity.long.2`          | 1024          | Level 2 recycler capacity for long sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.short.2`         | 1024          | Level 2 recycler capacity for short sparse arrays                                                                                                 |
| `sparsearray.recycler.capacity.object.2`        | 1024          | Level 2 recycler capacity for object sparse arrays                                                                                                |
| `sparsearray.recycler.capacity.boolean.1`       | 1024          | Level 1 recycler capacity for boolean sparse arrays                                                                                               |
| `sparsearray.recycler.capacity.byte.1`          | 1024          | Level 1 recycler capacity for byte sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.char.1`          | 1024          | Level 1 recycler capacity for character sparse arrays                                                                                             |
| `sparsearray.recycler.capacity.double.1`        | 1024          | Level 1 recycler capacity for double sparse arrays                                                                                                |
| `sparsearray.recycler.capacity.float.1`         | 1024          | Level 1 recycler capacity for float sparse arrays                                                                                                 |
| `sparsearray.recycler.capacity.int.1`           | 1024          | Level 1 recycler capacity for integer sparse arrays                                                                                               |
| `sparsearray.recycler.capacity.long.1`          | 1024          | Level 1 recycler capacity for long sparse arrays                                                                                                  |
| `sparsearray.recycler.capacity.short.1`         | 1024          | Level 1 recycler capacity for short sparse arrays                                                                                                 |
| `sparsearray.recycler.capacity.object.1`        | 1024          | Level 1 recycler capacity for object sparse arrays                                                                                                |
| `sparsearray.recycler.capacity.boolean.0`       | 1024          | Level 0 (top) recycler capacity for boolean sparse arrays                                                                                         |
| `sparsearray.recycler.capacity.byte.0`          | 1024          | Level 0 (top) recycler capacity for byte sparse arrays                                                                                            |
| `sparsearray.recycler.capacity.char.0`          | 1024          | Level 0 (top) recycler capacity for character sparse arrays                                                                                       |
| `sparsearray.recycler.capacity.double.0`        | 1024          | Level 0 (top) recycler capacity for double sparse arrays                                                                                          |
| `sparsearray.recycler.capacity.float.0`         | 1024          | Level 0 (top) recycler capacity for float sparse arrays                                                                                           |
| `sparsearray.recycler.capacity.int.0`           | 1024          | Level 0 (top) recycler capacity for integer sparse arrays                                                                                         |
| `sparsearray.recycler.capacity.long.0`          | 1024          | Level 0 (top) recycler capacity for long sparse arrays                                                                                            |
| `sparsearray.recycler.capacity.short.0`         | 1024          | Level 0 (top) recycler capacity for short sparse arrays                                                                                           |
| `sparsearray.recycler.capacity.object.0`        | 1024          | Level 0 (top) recycler capacity for object sparse arrays                                                                                          |
| `sparsearray.recycler.maximumCapacity.default`  | unlimited     | Default maximum recycler capacity for all sparse array types                                                                                      |
| `sparsearray.recycler.maximumCapacity.<type>`   | unlimited     | Maximum base recycler capacity for `<type>` sparse arrays, and the default maximum for its other levels; never less than the type's base capacity |
| `sparsearray.recycler.maximumCapacity.<type>.2` | unlimited     | Maximum level 2 recycler capacity for `<type>` sparse arrays; never less than its level 2 capacity                                                |
| `sparsearray.recycler.maximumCapacity.<type>.1` | unlimited     | Maximum level 1 recycler capacity for `<type>` sparse arrays; never less than its level 1 capacity                                                |
| `sparsearray.recycler.maximumCapacity.<type>.0` | unlimited     | Maximum level 0 (top) recycler capacity for `<type>` sparse arrays; never less than its level 0 capacity                                          |

The "in use" bitmaps have a recycler of their own at each level, which every type shares. Each one's capacity and maximum are the sums of the types' values at that level, and none has properties.

### How recyclers grow and shrink

A recycler grows during the cycle that needs more arrays than it holds. When a cycle returns arrays to a full recycler, the recycler turns them away and counts them. Each later borrow that finds the recycler empty is an array it could have kept, so the recycler grows by one for each such borrow until the count is spent. Growth therefore never exceeds what some cycle used, and it never exceeds the configured maximum.

A recycler shrinks at the end of each window. A window is sized to span a whole cycle of every update graph in the process: it lasts `SoftRecycler.windowMargin` times the longest interval between consecutive cycle starts among the update graphs, but no less than `SoftRecycler.windowMillis` and no more than `SoftRecycler.maximumWindowMillis`. If the recycler never emptied during the window, the arrays it never lent out were not needed, and it discards the fraction of them set by `SoftRecycler.shrinkFraction`, never shrinking below its configured capacity.

Windows end only when an update graph starts a cycle. In a process whose update graphs never cycle, recyclers never shrink. The recyclers hold their arrays through soft references, so the garbage collector reclaims them under memory pressure in any case.

| Property Name                      | Default Value | Description                                                                                                                                                                                                                                              |
| ---------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SoftRecycler.windowMillis`        | 1000          | The shortest window, in milliseconds, and the window when no update graph has a longer interval between cycles                                                                                                                                           |
| `SoftRecycler.maximumWindowMillis` | 60000         | The longest window, in milliseconds, between possibly shrinking the recycler. Even if one of the update graphs has a longer interval between cycles, the next update graph to start a cycle will shrink the recycler after the window exceeds this value |
| `SoftRecycler.windowMargin`        | 1.5           | The multiple of the longest interval between update graph cycle starts that a window lasts                                                                                                                                                               |
| `SoftRecycler.shrinkFraction`      | 0.1           | The fraction, from 0 to 1, of the arrays a window never needed that the recycler discards when the window ends; 0 keeps the capacity once grown, and 1 shrinks to what the window needed                                                                 |

### Tuning recyclers

Because recyclers adapt, most workloads need no tuning. The configured capacity sets how many arrays a recycler keeps through quiet periods, and the maximum bounds how many it keeps through busy ones.

- **Workloads that alternate busy and quiet cycles**: Raise `SoftRecycler.windowMillis`, or set `SoftRecycler.shrinkFraction` to 0, so that a recycler keeps what a busy cycle needed through the quiet cycles between.
- **Memory-constrained environments**: Set a `maximumCapacity` to bound what a recycler holds, or raise `SoftRecycler.shrinkFraction` toward 1 to discard unneeded arrays sooner.
- **Type-specific tuning**: If certain types dominate, raise their capacity or maximum while reducing others.

## Stateless by default

These flags enable the engine to assume more often that a given Filter or Selectable can be executed in parallel (unless the Filter or Selectable is [marked serial or has barriers](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where) interface).

| Property Name                          | Default Value | Description                                                                                             |
| -------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------- |
| `QueryTable.statelessFiltersByDefault` | true          | Enables the engine to assume that filters are stateless by default, allowing for more optimizations     |
| `QueryTable.statelessSelectByDefault`  | true          | Enables the engine to assume that Selectables are stateless by default, allowing for more optimizations |

## Related documentation

- [QueryTable JavaDocs](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html)
- [Table API](../getting-started/crash-course/table-ops.md)
- [Incremental update model](./table-update-model.md)
- [Query Memoization](../reference/community-questions/query-memoization.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Parallelizing queries](./query-engine/parallelization.md)
