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

Static tables can be created by reading from a static data source, such as [CSV](../how-to-guides/data-import-export/csv-import.md), Iceberg<!--TODO: add link-->, [Parquet](../how-to-guides/data-import-export/parquet-import.md), SQL<!--TODO: add link when SQL docs exist in groovy-->. Or, they can be created with Deephaven's table creation functions, like [`newTable`](../how-to-guides/new-and-empty-table.md#newtable) or [`emptyTable`](../how-to-guides/new-and-empty-table.md#emptytable). This example uses [`emptyTable`](../how-to-guides/new-and-empty-table.md#newtable) to construct a static table:

```groovy test-set=1 order=t
// create a static table with 10 rows and 2 columns
t = emptyTable(10).update("IntIdx = i", "LongIdx = ii")
```

Check whether a table is a static table with the [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md) property. This property will be `False` for static tables:

```groovy test-set=1 order=:log
println t.isRefreshing()
```

Any streaming table can be converted to a static table by taking a [`snapshot`](../reference/table-operations/snapshot/snapshot.md). This will produce a static copy of the streaming table at the moment in time the snapshot is taken:

```groovy test-set=2 ticking-table order=null
// create a streaming table with timeTable
t = timeTable("PT1s")

// at some point in the future, take a snapshot of the streaming table
tSnapshot = t.snapshot()
```

![A user creates a static snapshot of the ticking table `t`](../assets/conceptual/table-types/table-types-1.gif)

Verify that the snapshot is static with [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md):

```groovy test-set=2 order=:log
println t.isRefreshing()
println tSnapshot.isRefreshing()
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

Blink tables keep only the set of rows received during the current update cycle. Users can create blink tables when ingesting [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md), creating [time tables](../how-to-guides/time-table.md), or using [Table Publishers](../how-to-guides/table-publisher.md#table-publisher). They have the following key properties:

- The table only consists of rows added in the previous update cycle.
- No rows persist for more than one update cycle.
- The table's size is bounded by the size of the largest update it receives.

These properties have the following consequences:

1. Since blink tables see a brand new world at every update cycle, index-based operations, stateful operations, or operations using [special variables](../reference/query-language/variables/special-variables.md) are guaranteed to yield results that do not change unexpectedly between update cycles.
2. The entire table changes every update cycle, so preserving row order from cycle to cycle is irrelevant.
3. Blink tables can only cause memory problems if a single update receives more data than fits in available RAM. This is unusual, but not impossible.

Blink tables are the default table type for Kafka ingestion within Deephaven because they use little memory. They are most useful for low-memory aggregations, deriving downstream tables, or using programmatic listeners to react to data.

Check whether a table is a blink table with the [`isBlink`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BlinkTableTools.html#isBlink(io.deephaven.engine.table.Table)) method:

```groovy test-set=3 ticking-table order=null
import io.deephaven.engine.table.impl.BlinkTableTools
import io.deephaven.engine.table.impl.TimeTable.Builder

// TimeTable.Builder can be used to create a blink table with blinkTable=true
builder = new Builder().period("PT0.2s").blinkTable(true)

t = builder.build().update("X = ii")
```

![A ticking blink table](../assets/conceptual/table-types/table-types-2.gif)

```groovy test-set=3 order=:log
println BlinkTableTools.isBlink(t)
```

### Specialized semantics for blink tables

Aggregation operations such as [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) operate with special semantics on blink tables, allowing the result to aggregate over the entire observed stream of rows from the time the operation is initiated. That means, for example, that a [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) on a blink table will contain the resulting sums for each aggregation group over all observed rows since the [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) was applied, rather than just the sums for the current update cycle. This allows for aggregations over the full history of a stream to be performed with greatly reduced memory costs when compared to the alternative strategy of holding the entirety of the stream as an in-memory table.

Here is an example that demonstrates a blink table's specialized aggregation semantics:

```groovy test-set=4 ticking-table order=null
// create blink table with two groups of data to sum
builder = new Builder().period("PT0.1s").blinkTable(true)

t = builder.build().update("X = ii", "Group = ii % 2 == 0 ? `A` : `B`")

// note that the sums continue to grow by including all previous data
tSum = t.view("X", "Group").sumBy("Group")
```

![Two Deephaven tables - table 't' is a blink table and 't_sum' is aggregated with `sum_by`](../assets/conceptual/table-types/table-types-3.gif)

These special aggregation semantics may not always be desirable. Disable them by calling `removeBlink` on the blink table:

<!---TODO: Link remove_blink pydoc when it exists-->

```groovy test-set=4 ticking-table order=null
tNoBlink = t.removeBlink()

// sum is no longer over all data, but only over data in this cycle
tSumNoBlink = tNoBlink.view("X", "Group").sumBy("Group")
```

![Two Deephaven tables. Without the blink attribute, 't_sum_no_blink' only aggregates over data in this cycle](../assets/conceptual/table-types/table-types-4.gif)

Most operations on blink tables behave exactly as they do on other tables (see the [exclusions below](#unsupported-operations)); that is, added rows are processed as usual. For example, [`select`](../reference/table-operations/select/select.md) on a blink table will contain only the newly added rows from the current update cycle.

Because Deephaven does not need to keep all the history of rows read from the input stream in memory, table operations on blink tables may require less memory.

### Unsupported operations

Attempting to use the following operations on a blink table will raise an error:

- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) (see [below](#partition-a-blink-table) for a workaround)
- [`partitionedAggBy`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`headPct`](../reference/table-operations/filter/head-pct.md)
- [`tailPct`](../reference/table-operations/filter/tail-pct.md)
- [`slice`](../reference/table-operations/filter/slice.md)
- [`slicePct`](../reference/table-operations/filter/slice-pct.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) if either `group` or `partition` is used.
- [`rollup`](../reference/table-operations/create/rollup.md) if `includeConstituents=true`.
- [`tree`](../reference/table-operations/create/tree.md)

### Create an append-only table from a blink table

It is common to create an append-only table from a blink table to preserve the entire data history. Use [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md) to do this:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.BlinkTableTools
import io.deephaven.engine.table.impl.TimeTable.Builder

builder = new Builder().period("PT1s").blinkTable(true)

t = builder.build().update("X = ii")

// get an append-only table from t
tAppendOnly = BlinkTableTools.blinkToAppendOnly(t)
```

![An append-only table that preserves the data from table 't' as it ticks](../assets/conceptual/table-types/table-types-5.gif)

> [!TIP]
> To disable blink table semantics, use [`removeBlink`](../reference/table-operations/create/remove-blink.md), which returns a child table that is identical to the parent blink table in every way, but is no longer marked for special blink table semantics. The resulting table will still exhibit the “blink” table update pattern, removing all previous rows on each cycle, and thus only containing “new” rows.

### Partition a blink table

To partition a blink table, drop the blink attribute, create the partition, and then use a transform to add back the blink table attribute:

```groovy
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.Table

builder = new Builder().period("PT0.1s").blinkTable(true)

table = builder.build().update("X = ii", "Group = ii % 2 == 0 ? `A` : `B`")

partitionedBlinkTable =
    table.removeBlink()
    .partitionBy("Group")
    .transform(t -> t.withAttributes(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true)))
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

It is common to create a ring table from a blink table to preserve _some_ data history, _but not all_. Use [`RingTableTools.of`](../reference/cheat-sheets/simple-table-constructors.md#ringtabletoolsof) to do this.

The following example creates a ring table that holds the five most recent observations from a blink table:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import io.deephaven.engine.table.impl.TimeTable.Builder

builder = new Builder().period("PT0.5s").blinkTable(true)

t = builder.build()

// get ring table from t that holds last five rows
tRing = RingTableTools.of(t, 5)
```

![Two Deephaven tables - a time table 't', and a 5-row ring table, 't_ring'](../assets/conceptual/table-types/table-types-6.gif)

### Create a ring table from an append-only table

> [!CAUTION]
> Creating a ring table from an append-only table does not give the memory savings that ring tables are useful for.

Ring tables can also be created from append-only tables using [`RingTableTools.of`](../reference/cheat-sheets/simple-table-constructors.md#ringtabletoolsof). This is a less common use case because the typical memory savings that ring tables afford is lost. If there is an append-only table anywhere in the equation, it can grow until it eats up all available memory. A downstream ring table will only _appear_ to save on memory, and is effectively equivalent to applying a [`tail`](../reference/table-operations/filter/tail.md) operation to an append-only table.

This example creates a ring table with a 5-row capacity from a simple append-only time table:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

// t is an append-only table
t = timeTable("PT0.5s")

// get ring table from t that holds last three rows
tRing = RingTableTools.of(t, 3)
```

![An append-only time table and a 5-row ring table](../assets/conceptual/table-types/table-types-7.gif)

If the source append-only table _already_ has rows in it when [`RingTableTools.of`](../reference/cheat-sheets/simple-table-constructors.md#ringtabletoolsof) is called, the resulting ring table will include those rows by default:

```groovy test-set=5 ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

// create append-only table that starts with five rows
tStatic = emptyTable(5).update("X = ii")
tDynamic = timeTable("PT1s").update("X = ii + 5").dropColumns("Timestamp")
t = merge(tStatic, tDynamic)

// get ring table from t that holds last ten rows
tRingWithInitial = RingTableTools.of(t, 10)
```

![A ring table initializes with the rows that are already present in the source append-only table](../assets/conceptual/table-types/table-types-8.gif)

To disable this behavior, set `initialize = false`:

```groovy test-set=5 ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

tRingWithoutInitial = RingTableTools.of(t, 10, false)
```

![When `initialize=False`, a ring table does not initialize with data that existed in the source table before the ring table was created](../assets/conceptual/table-types/table-types-9.gif)

## Related documentation

- [How to use a TablePublisher](../how-to-guides/table-publisher.md#table-publisher)
- [Create a time table](../how-to-guides/time-table.md)
- [Kafka basic terminology](./kafka-basic-terms.md)
