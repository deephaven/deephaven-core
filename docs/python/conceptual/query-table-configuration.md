---
title: Query table configuration
sidebar_label: Query table configuration
---

This guide discusses how to control various `QueryTable` features that affect your Deephaven tables' latency and throughput.

# Query Table

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
| [Pushdown Predicates](#pushdown-predicates-with-where)              | `QueryTable.disableWherePushdownDataIndex`               | false      |
| [Pushdown Predicates](#pushdown-predicates-with-where)              | `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false      |
| [Parallel Processing with Where](#parallel-processing-with-where)   | `QueryTable.disableParallelWhere`                        | false      |
| [Parallel Processing with Where](#parallel-processing-with-where)   | `QueryTable.parallelWhereRowsPerSegment`                 | `1 << 16`  |
| [Parallel Processing with Where](#parallel-processing-with-where)   | `QueryTable.parallelWhereSegments`                       | -1         |
| [Parallel Processing with Where](#parallel-processing-with-where)   | `QueryTable.forceParallelWhere` (test-focused)           | false      |
| [Parallel Processing with Select](#parallel-processing-with-select) | `QueryTable.enableParallelSelectAndUpdate`               | true       |
| [Parallel Processing with Select](#parallel-processing-with-select) | `QueryTable.minimumParallelSelectRows`                   | `1L << 22` |
| [Parallel Processing with Select](#parallel-processing-with-select) | `QueryTable.forceParallelSelectAndUpdate` (test-focused) | false      |
| [Parallel Snapshotting](#parallel-snapshotting)                     | `QueryTable.enableParallelSnapshot`                      | true       |
| [Parallel Snapshotting](#parallel-snapshotting)                     | `QueryTable.minimumParallelSnapshotRows`                 | `1L << 20` |
| [SoftRecycler Configuration](#softrecycler-configuration)           | `array.recycler.capacity.*`                              | 1024       |
| [SoftRecycler Configuration](#softrecycler-configuration)           | `sparsearray.recycler.capacity.*`                        | 1024       |
| [Stateless by Default](#stateless-by-default-experimental)          | `QueryTable.statelessFiltersByDefault`                   | false      |

Each property is described below, roughly categorized by similarity.

## Memoization

Deephaven utilizes [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations to improve performance by eliminating duplicate work. See [Query Memoization](../reference/community-questions/query-memoization.md) for more details.

It can be beneficial to disable memoization when benchmarking or testing, as memoized results can hide true operational costs and skew performance metrics.

| Property Name               | Default Value | Description                        |
| --------------------------- | ------------- | ---------------------------------- |
| `QueryTable.memoizeResults` | true          | Enables memoizing table operations |

## Redirection

Deephaven Tables maintain a 63-bit keyspace that maps a logical row in row-key space to its data. Many of Deephaven's column sources use a multi-level data layout to avoid allocating more resources than necessary to fulfill operational requirements. See [selection method properties](/core/groovy/docs/reference/community-questions/selection-method-properties/) for more details.

Redirection is a mapping between a parent column source and the resulting column source for a given operation. A sorted column, for example, is redirected from the original to present the rows in the targeted sort order. Redirection may also flatten from a sparse keyspace to a flat and dense keyspace.

| Property Name                                  | Default Value | Description                                                                                                   |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------- |
| `QueryTable.redirectUpdate`                    | false         | Forces non-flat refreshing `QueryTable#update` operations to redirect despite the increased performance costs |
| `QueryTable.redirectSelect`                    | false         | Forces non-flat refreshing `QueryTable#select` operations to redirect despite the increased performance costs |
| `QueryTable.maximumStaticSelectMemoryOverhead` | 1.1 (double)  | The maximum overhead as a fraction (e.g. 1.1 is 10% overhead; always sparse if < 0, never sparse if 0)        |

## DataIndex

A Deephaven [DataIndex](../how-to-guides/data-indexes.md) is an index that can improve the speed of filtering operations.

| Property Name                              | Default Value | Description                                                                                                                                                                                                         |
| ------------------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.useDataIndexForWhere`          | true          | Enables data index usage in `QueryTable#where` operations                                                                                                                                                           |
| `QueryTable.useDataIndexForAggregation`    | true          | Enables data index usage in `QueryTable#aggBy`, `QueryTable#selectDistinct`, within [rollup-tables](../reference/table-operations/create/rollup.md) and [tree-tables](../reference/table-operations/create/tree.md) |
| `QueryTable.useDataIndexForJoins`          | true          | Enables data index usage in [Deephaven Joins](../how-to-guides/joins-timeseries-range.md#which-method-should-you-use)                                                                                               |
| `QueryTable.disableWherePushdownDataIndex` | false         | Disables data index usage within [where's pushdown predicates](#pushdown-predicates-with-where)                                                                                                                     |

## Pushdown predicates with `where`

Pushdown predicates refer to the mechanism whereby filtering conditions are applied as early as possible, ideally at the data source (e.g., Parquet or other columnar formats), before loading data into the system. By annotating source reads with predicates, the engine pulls in only the rows that satisfy the conditions, significantly reducing I/O and improving performance.

| Property Name                                            | Default Value | Description                                                                                           |
| -------------------------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------- |
| `QueryTable.disableWherePushdownDataIndex`               | false         | Disables the use of [data index](../how-to-guides/data-indexes.md) within where's pushdown predicates |
| `QueryTable.disableWherePushdownParquetRowGroupMetadata` | false         | Disables the usage of Parquet row group metadata during push-down filtering                           |

## Parallel processing with `where`

Parallelism for `where` operations is not enabled until the parent's size exceeds `QueryTable.parallelWhereRowsPerSegment` rows. This avoids the overhead of using threads for small operations. For tables larger than this threshold, the `where` operation uses a fixed number of parallel segments defined by `QueryTable.parallelWhereSegments`. These parameters can be tuned to avoid unnecessary parallelism when the overhead exceeds potential gains.

| Property Name                                  | Default Value | Description                                                                                                               |
| ---------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `QueryTable.enableParallelWhere`               | false         | Enables parallelized optimizations for `QueryTable#where` operations                                                      |
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

## SoftRecycler configuration

Deephaven uses [SoftRecycler](https://docs.deephaven.io/core/javadoc/io/deephaven/util/SoftRecycler.html) objects to manage memory for array and sparse array column sources. These recyclers maintain pools of allocated arrays that can be reused rather than constantly allocating and deallocating memory, which can improve performance and reduce garbage collection pressure.

The capacity of these recyclers (how many arrays each recycler holds) can be configured on a per-type basis, allowing you to tune memory usage based on your workload characteristics.

### Array column source recyclers

Array-backed column sources (dense arrays) use SoftRecyclers to manage blocks of data for each primitive type.

| Property Name                    | Default Value | Description                                                                                         |
| -------------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
| `array.recycler.capacity.default`   | 1024          | Default recycler capacity for all array types (used if type-specific property is not set)           |
| `array.recycler.capacity.boolean`   | 1024          | Recycler capacity for boolean array blocks                                                         |
| `array.recycler.capacity.byte`      | 1024          | Recycler capacity for byte array blocks                                                            |
| `array.recycler.capacity.char`      | 1024          | Recycler capacity for character array blocks                                                       |
| `array.recycler.capacity.double`    | 1024          | Recycler capacity for double array blocks                                                          |
| `array.recycler.capacity.float`     | 1024          | Recycler capacity for float array blocks                                                           |
| `array.recycler.capacity.int`       | 1024          | Recycler capacity for integer array blocks                                                         |
| `array.recycler.capacity.long`      | 1024          | Recycler capacity for long array blocks                                                            |
| `array.recycler.capacity.short`     | 1024          | Recycler capacity for short array blocks                                                           |
| `array.recycler.capacity.object`    | 1024          | Recycler capacity for object array blocks                                                          |
| `array.recycler.capacity.inuse`     | 9216 (max of all types) | Recycler capacity for "in use" bitmap blocks (should be at least the maximum capacity of other types) |

### Sparse array column source recyclers

Sparse array column sources use a multi-level hierarchical structure and maintain separate recyclers at each level. Each level can be configured independently to optimize memory usage for your access patterns.

| Property Name                               | Default Value      | Description                                                                          |
| ------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------ |
| `sparsearray.recycler.capacity.default`       | 1024               | Default recycler capacity for all sparse array types                                 |
| `sparsearray.recycler.capacity.boolean`       | 1024               | Base recycler capacity for boolean sparse arrays                                     |
| `sparsearray.recycler.capacity.byte`          | 1024               | Base recycler capacity for byte sparse arrays                                        |
| `sparsearray.recycler.capacity.char`          | 1024               | Base recycler capacity for character sparse arrays                                   |
| `sparsearray.recycler.capacity.double`        | 1024               | Base recycler capacity for double sparse arrays                                      |
| `sparsearray.recycler.capacity.float`         | 1024               | Base recycler capacity for float sparse arrays                                       |
| `sparsearray.recycler.capacity.int`           | 1024               | Base recycler capacity for integer sparse arrays                                     |
| `sparsearray.recycler.capacity.long`          | 1024               | Base recycler capacity for long sparse arrays                                        |
| `sparsearray.recycler.capacity.short`         | 1024               | Base recycler capacity for short sparse arrays                                       |
| `sparsearray.recycler.capacity.object`        | 1024               | Base recycler capacity for object sparse arrays                                      |
| `sparsearray.recycler.capacity.boolean.2`     | 1024               | Level 2 recycler capacity for boolean sparse arrays                                  |
| `sparsearray.recycler.capacity.byte.2`        | 1024               | Level 2 recycler capacity for byte sparse arrays                                     |
| `sparsearray.recycler.capacity.char.2`        | 1024               | Level 2 recycler capacity for character sparse arrays                                |
| `sparsearray.recycler.capacity.double.2`      | 1024               | Level 2 recycler capacity for double sparse arrays                                   |
| `sparsearray.recycler.capacity.float.2`       | 1024               | Level 2 recycler capacity for float sparse arrays                                    |
| `sparsearray.recycler.capacity.int.2`         | 1024               | Level 2 recycler capacity for integer sparse arrays                                  |
| `sparsearray.recycler.capacity.long.2`        | 1024               | Level 2 recycler capacity for long sparse arrays                                     |
| `sparsearray.recycler.capacity.short.2`       | 1024               | Level 2 recycler capacity for short sparse arrays                                    |
| `sparsearray.recycler.capacity.object.2`      | 1024               | Level 2 recycler capacity for object sparse arrays                                   |
| `sparsearray.recycler.capacity.boolean.1`     | 1024               | Level 1 recycler capacity for boolean sparse arrays                                  |
| `sparsearray.recycler.capacity.byte.1`        | 1024               | Level 1 recycler capacity for byte sparse arrays                                     |
| `sparsearray.recycler.capacity.char.1`        | 1024               | Level 1 recycler capacity for character sparse arrays                                |
| `sparsearray.recycler.capacity.double.1`      | 1024               | Level 1 recycler capacity for double sparse arrays                                   |
| `sparsearray.recycler.capacity.float.1`       | 1024               | Level 1 recycler capacity for float sparse arrays                                    |
| `sparsearray.recycler.capacity.int.1`         | 1024               | Level 1 recycler capacity for integer sparse arrays                                  |
| `sparsearray.recycler.capacity.long.1`        | 1024               | Level 1 recycler capacity for long sparse arrays                                     |
| `sparsearray.recycler.capacity.short.1`       | 1024               | Level 1 recycler capacity for short sparse arrays                                    |
| `sparsearray.recycler.capacity.object.1`      | 1024               | Level 1 recycler capacity for object sparse arrays                                   |
| `sparsearray.recycler.capacity.boolean.0`     | 1024               | Level 0 (top) recycler capacity for boolean sparse arrays                            |
| `sparsearray.recycler.capacity.byte.0`        | 1024               | Level 0 (top) recycler capacity for byte sparse arrays                               |
| `sparsearray.recycler.capacity.char.0`        | 1024               | Level 0 (top) recycler capacity for character sparse arrays                          |
| `sparsearray.recycler.capacity.double.0`      | 1024               | Level 0 (top) recycler capacity for double sparse arrays                             |
| `sparsearray.recycler.capacity.float.0`       | 1024               | Level 0 (top) recycler capacity for float sparse arrays                              |
| `sparsearray.recycler.capacity.int.0`         | 1024               | Level 0 (top) recycler capacity for integer sparse arrays                            |
| `sparsearray.recycler.capacity.long.0`        | 1024               | Level 0 (top) recycler capacity for long sparse arrays                               |
| `sparsearray.recycler.capacity.short.0`       | 1024               | Level 0 (top) recycler capacity for short sparse arrays                              |
| `sparsearray.recycler.capacity.object.0`      | 1024               | Level 0 (top) recycler capacity for object sparse arrays                             |
| `sparsearray.recycler.capacity.inuse`         | 9216 (sum of all base types) | Recycler capacity for "in use" bitmap blocks at the lowest level                     |
| `sparsearray.recycler.capacity.inuse.2`       | 9216 (max of level 2) | Recycler capacity for "in use" bitmap blocks at level 2                              |
| `sparsearray.recycler.capacity.inuse.1`       | 9216 (max of level 1) | Recycler capacity for "in use" bitmap blocks at level 1                              |
| `sparsearray.recycler.capacity.inuse.0`       | 9216 (max of level 0) | Recycler capacity for "in use" bitmap blocks at level 0 (top)                        |

#### Tuning SoftRecycler capacity

The recycler capacity determines how many array blocks are kept in memory for potential reuse. Increasing capacity can improve performance if your workload frequently allocates and deallocates arrays, at the cost of higher memory usage. Decreasing capacity reduces memory overhead but may increase garbage collection frequency.

- **Low memory environments**: Consider reducing capacities to reduce memory footprint
- **High throughput environments**: Consider increasing capacities to reduce allocation/deallocation overhead
- **Type-specific tuning**: If certain types are used more frequently, you can increase their capacity while reducing others

## Stateless by default (experimental)

Anticipated in a future release of Deephaven, the flag(s) in this category will flip from a default of false to a default of true. These flags enable the engine to assume more often that a given formula, filter, or selectable can be optimized (unless otherwise noted by the user via API usages).

This is experimental; more details can be learned by reading the Javadoc on io.deephaven.api.ConcurrencyControl.

| Property Name                          | Default Value | Description                                                                                         |
| -------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
| `QueryTable.statelessFiltersByDefault` | false         | Enables the engine to assume that filters are stateless by default, allowing for more optimizations |

## Related documentation

- [QueryTable JavaDocs](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html)
- [Table API](../getting-started/crash-course/table-ops.md)
- [Incremental update model](./table-update-model.md)
- [Query Memoization](../reference/community-questions/query-memoization.md)
- [Data indexes](../how-to-guides/data-indexes.md)
- [Parallelizing queries](./query-engine/parallelization.md)
