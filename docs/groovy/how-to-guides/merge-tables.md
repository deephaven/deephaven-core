---
title: Merge tables
sidebar_label: Merge
---

Deephaven tables can be combined via two different categories of operation: merge operations and join operations. Merge operations combine tables by stacking them vertically, one on top of the other. Join operations combine tables (or specific columns from tables) horizontally, side by side.

This guide discusses how to merge tables in Deephaven. If you want to join tables horizontally, see [Joins: Exact and Relational](./joins-exact-relational.md) and [Joins: Time-Series and Range](./joins-timeseries-range.md).

There are two methods for merging tables in Deephaven: [`merge`](../reference/table-operations/merge/merge.md) and [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md).

The following code block initializes three tables, each with two columns. We will use these tables in the following examples.

```groovy test-set=1 order=source1,source2,source3
source1 = newTable(
   stringCol("Letter", "A", "B", "D"),
   intCol("Number", 1, 2, 3)
)
source2 = newTable(
   stringCol("Letter", "C", "D", "E"),
   intCol("Number", 14, 15, 16)
)
source3 = newTable(
   stringCol("Letter", "E", "F", "A"),
   intCol("Number", 22, 25, 27)
)
```

## `merge`

The [`merge`](../reference/table-operations/merge/merge.md) method simply stacks one or more tables on top of another.

```groovy syntax
t = merge(tables)
```

> [!NOTE]
> The columns for each table must have the same names and types, or a column mismatch error will occur. `NULL` inputs are ignored.

Let's merge two of our tables using the [`merge`](../reference/table-operations/merge/merge.md) method.

```groovy test-set=1 order=result
result = merge(source1, source2)
```

The resulting table `result` is all of the source tables stacked vertically. If the source tables dynamically change, such as for ticking data, rows will be inserted within the stack. For example, if a row is added to the end of the third source table, in the resulting table, that new row appears after all other rows from the third source table and before all rows from the fourth source table.

## `mergeSorted`

The [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md) method sorts the result table after merging the data.

```groovy syntax
t = mergeSorted(keyColumn, tables)
```

Where `keyColumn` is the column by which to sort the merged table, and `tables` are the source tables.

Let's merge our three tables and sort by `Number` with [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md).

```groovy test-set=1 order=result
result = mergeSorted("Number", source1, source2, source3)
```

The resulting table is all of the source tables stacked vertically and sorted by the `Number` column.

## Perform efficient merges

When performing more than one merge operation, it is best to perform all the merges at the same time, rather than nesting several merges.

In this example, a table named `result` is initialized. As new tables are generated, the results are merged at every iteration. Calling the merge method on each iteration makes this example inefficient.

```groovy order=result
result = null

for (int i = 0; i < 5; i++) {
   new_result = newTable(stringCol("Code", String.format("A%d", i), String.format("A%d", i)), intCol("Val", i, 10*i))
   if (result = null) {
       result = new_result
   } else {
       result = merge(result, new_result)
   }
}
```

Instead, we can make the operation more efficient by calling the [`merge`](../reference/table-operations/merge/merge.md) method just once. Here [`merge`](../reference/table-operations/merge/merge.md) is applied to an array containing all of the source tables:

```groovy order=result
List<Object> tableArray = new ArrayList<>();

for (int i = 0; i < 5; i++) {
   new_result = newTable(stringCol("Code", String.format("A%d", i), String.format("A%d", i)), intCol("Val", i, 10*i))
   tableArray.add(new_result)
}
result = merge(tableArray)
```

If you are sorting the data you want to merge, it is more efficient to use the [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md) method instead of [`merge`](../reference/table-operations/merge/merge.md) followed by [`sort`](../reference/table-operations/sort/sort.md). Your code will be easier to read, too.

```groovy order=null
source1 = newTable(stringCol("Letter", "A", "B", "D"), intCol("Number", 1, 2, 3))
source2 = newTable(stringCol("Letter", "C", "D", "E"), intCol("Number", 14, 15, 16))
source3 = newTable(stringCol("Letter", "E", "F", "A"), intCol("Number", 22, 25, 27))

// using `merge` followed by `sort`
t_merged = merge(source1, source2, source3).sort("Number")

// using `mergeSorted`
result = mergeSorted("Number", source1, source2, source3)
```

![Log readout showing how long it took for `t_merged` vs `result`](../assets/how-to/merge-sort-mergesorted-10x.png)

When we use [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md), our query completes more than _ten times faster_ (on average) than it does when using [`merge`](../reference/table-operations/merge/merge.md) followed by [`sort`](../reference/table-operations/sort/sort.md).

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`mergeSorted`](../reference/table-operations/merge/merge-sorted.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#merge(java.util.Collection))
