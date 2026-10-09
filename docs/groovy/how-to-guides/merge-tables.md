---
title: Merge tables
---

Deephaven combines tables in two ways. Merge operations stack tables vertically, one on top of the other. Join operations place columns from two tables side by side.

This guide shows how to merge tables. To join tables horizontally, see [Exact and relational joins](./joins-exact-relational.md) and [Inexact, time-series, and range joins](./joins-timeseries-range.md).

This guide covers two methods for merging tables. [`merge`](../reference/table-operations/merge/merge.md) stacks whole tables in the order you pass them and works with static and [ticking](../conceptual/table-update-model.md) tables. [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md) interleaves the rows of static tables that are each already sorted on a key column, so the result is sorted by that column. To merge the tables that make up a [partitioned table](./partitioned-tables.md), use [`PartitionedTable.merge`](../reference/table-operations/partitioned-tables/merge.md).

The following code block creates three tables, each with two columns. The examples in the `merge` and `mergeSorted` sections use these tables. Each table is sorted by its `Number` column.

```groovy test-set=1 order=source1,source2,source3
source1 = newTable(
    stringCol("Letter", "A", "B", "D"),
    intCol("Number", 1, 12, 23)
)
source2 = newTable(
    stringCol("Letter", "C", "D", "E"),
    intCol("Number", 5, 15, 16)
)
source3 = newTable(
    stringCol("Letter", "E", "F", "A"),
    intCol("Number", 3, 22, 25)
)
```

## `merge`

The [`merge`](../reference/table-operations/merge/merge.md) method stacks one or more tables on top of one another.

```groovy syntax
merge(Table... tables)
merge(Collection<Table> tables)
```

Pass the source tables either as separate arguments or as a collection, such as a `List<Table>`.

> [!NOTE]
> Every table must have columns with the same names and types, or `merge` fails with an error. `merge` skips `null` inputs, but at least one table must be non-null.

The following example merges two of the tables:

```groovy test-set=1 order=result
result = merge(source1, source2)
```

`result` contains the rows of `source1` followed by the rows of `source2`. Rows from each source table stay together, in the order you pass the tables to `merge`.

This order also holds when the source tables are [ticking](../conceptual/table-update-model.md). Deephaven inserts each new row with the other rows from its source table, not at the end of the result. For example, if `source1` gains a new last row, that row appears in `result` after all other `source1` rows and before all `source2` rows.

## `mergeSorted`

The [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md) method merges tables that are each already sorted on a key column into one table sorted by that column.

```groovy syntax
mergeSorted(String keyColumn, Table... tables)
mergeSorted(String keyColumn, Collection<Table> tables)
```

`keyColumn` is the name of the key column, and `tables` are the source tables.

> [!NOTE]
> Each input table must already be sorted by the key column in ascending order, or the results are undefined. `mergeSorted` does not support ticking tables. Unlike [`merge`](../reference/table-operations/merge/merge.md), `mergeSorted` does not skip `null` inputs.

When the key column contains no null values, `mergeSorted` produces the same table as `merge` followed by [`sort`](../reference/table-operations/sort/sort.md). It interleaves rows that are already in order instead of sorting the whole merged table, so it typically does less work. The following example merges all three tables by `Number` both ways:

```groovy test-set=1 order=sortedAfterMerge,result
// Using `merge` followed by `sort`
sortedAfterMerge = merge(source1, source2, source3).sort("Number")

// Using `mergeSorted`
result = mergeSorted("Number", source1, source2, source3)
```

Both tables contain all rows from the three source tables, interleaved in `Number` order. If any source table is ticking or isn't sorted on the key column, use `merge` followed by `sort`.

## Perform efficient merges

### Merge all tables in one call

When you have several tables to merge, pass them all to a single [`merge`](../reference/table-operations/merge/merge.md) call instead of merging them one at a time.

The following example is inefficient. It merges each new table into `result` inside the loop, so every iteration after the first creates a new intermediate merged table:

```groovy order=result
result = null

for (int i = 0; i < 5; i++) {
    newResult = newTable(
        stringCol("Code", String.format("A%d", i), String.format("B%d", i)),
        intCol("Val", i, 10 * i)
    )
    if (result == null) {
        result = newResult
    } else {
        result = merge(result, newResult)
    }
}
```

The following example produces the same table more efficiently. It collects the new tables in a list and calls `merge` once on that list:

```groovy order=result
List<Table> tables = []

for (int i = 0; i < 5; i++) {
    newResult = newTable(
        stringCol("Code", String.format("A%d", i), String.format("B%d", i)),
        intCol("Val", i, 10 * i)
    )
    tables.add(newResult)
}

result = merge(tables)
```

### Order ticking tables by how fast they grow

When you merge ticking tables, pass static tables first, then tables that grow slowly, and tables that grow without bound last. To keep each source table's rows together, the result reserves a range of [row keys](../conceptual/table-update-model.md) for each source table, in the order you pass them. When a source table outgrows its range, the engine shifts the rows of every table after it. Some downstream operations do extra work for every shifted row.

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [Sort table data](./sort.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [Javadoc: `merge`](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#merge(java.util.Collection))
- [Javadoc: `mergeSorted`](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#mergeSorted(java.lang.String,io.deephaven.engine.table.Table...))
