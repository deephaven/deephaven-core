---
title: Merge tables
---

Deephaven combines tables in two ways. Merge operations stack tables vertically, one on top of the other. Join operations place columns from two tables side by side.

This guide shows how to merge tables. To join tables horizontally, see [Exact and relational joins](./joins-exact-relational.md) and [Inexact, time-series, and range joins](./joins-timeseries-range.md).

This guide covers two methods for merging tables. [`merge`](../reference/table-operations/merge/merge.md) stacks whole tables in the order you pass them and works with static and [ticking](../conceptual/table-update-model.md) tables. [`merge_sorted`](../reference/table-operations/merge/merge-sorted.md) interleaves the rows of static tables that are each already sorted on a key column, so the result is sorted by that column. To merge the tables that make up a [partitioned table](./partitioned-tables.md), use [`PartitionedTable.merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md).

The following code block creates three tables, each with two columns. The examples in the `merge` and `merge_sorted` sections use these tables. Each table is sorted by its `Number` column.

```python test-set=1 order=source1,source2,source3
from deephaven import merge, merge_sorted, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "B", "D"]), int_col("Number", [1, 12, 23])]
)
source2 = new_table(
    [string_col("Letter", ["C", "D", "E"]), int_col("Number", [5, 15, 16])]
)
source3 = new_table(
    [string_col("Letter", ["E", "F", "A"]), int_col("Number", [3, 22, 25])]
)
```

## `merge`

The [`merge`](../reference/table-operations/merge/merge.md) method stacks one or more tables on top of one another.

```python syntax
merge(tables: Sequence[Table]) -> Table
```

`tables` is a list (or other sequence) of the source tables.

> [!NOTE]
> Every table must have columns with the same names and types, or `merge` fails with an error. All elements of `tables` must be tables. `merge` does not accept `None`.

The following example merges two of the tables:

```python test-set=1 order=result
result = merge([source1, source2])
```

`result` contains the rows of `source1` followed by the rows of `source2`. Rows from each source table stay together, in the order the tables appear in `tables`.

This order also holds when the source tables are [ticking](../conceptual/table-update-model.md). Deephaven inserts each new row with the other rows from its source table, not at the end of the result. For example, if `source1` gains a new last row, that row appears in `result` after all other `source1` rows and before all `source2` rows.

## `merge_sorted`

The [`merge_sorted`](../reference/table-operations/merge/merge-sorted.md) method merges tables that are each already sorted on a key column into one table sorted by that column.

```python syntax
merge_sorted(tables: Sequence[Table], order_by: str) -> Table
```

`tables` is a list (or other sequence) of the source tables, and `order_by` is the name of the key column.

> [!NOTE]
> Each input table must already be sorted by the key column in ascending order, or the results are undefined. `merge_sorted` does not support ticking tables. As with [`merge`](../reference/table-operations/merge/merge.md), all elements of `tables` must be tables.

When the key column contains no null values, `merge_sorted` produces the same table as `merge` followed by [`sort`](../reference/table-operations/sort/sort.md). It interleaves rows that are already in order instead of sorting the whole merged table, so it typically does less work. The following example merges all three tables by `Number` both ways:

```python test-set=1 order=sorted_after_merge,result
# Using `merge` followed by `sort`
sorted_after_merge = merge([source1, source2, source3]).sort(order_by="Number")

# Using `merge_sorted`
result = merge_sorted([source1, source2, source3], order_by="Number")
```

Both tables contain all rows from the three source tables, interleaved in `Number` order. If any source table is ticking or isn't sorted on the key column, use `merge` followed by `sort`.

## Perform efficient merges

### Merge all tables in one call

When you have several tables to merge, pass them all to a single [`merge`](../reference/table-operations/merge/merge.md) call instead of merging them one at a time.

The following example is inefficient. It merges each new table into `result` inside the loop, so every iteration after the first creates a new intermediate merged table:

```python order=result
from deephaven import merge, new_table
from deephaven.column import int_col, string_col

result = None

for i in range(5):
    new_result = new_table(
        [string_col("Code", [f"A{i}", f"B{i}"]), int_col("Val", [i, 10 * i])]
    )
    if result is None:
        result = new_result
    else:
        result = merge([result, new_result])
```

The following example produces the same table more efficiently. It collects the new tables in a list and calls `merge` once on that list:

```python order=result
from deephaven import merge, new_table
from deephaven.column import int_col, string_col

tables = []

for i in range(5):
    new_result = new_table(
        [string_col("Code", [f"A{i}", f"B{i}"]), int_col("Val", [i, 10 * i])]
    )
    tables.append(new_result)

result = merge(tables)
```

### Order ticking tables by how fast they grow

When you merge ticking tables, pass static tables first, then tables that grow slowly, and tables that grow without bound last. To keep each source table's rows together, the result reserves a range of [row keys](../conceptual/table-update-model.md) for each source table, in the order you pass them. When a source table outgrows its range, the engine shifts the rows of every table after it. Some downstream operations do extra work for every shifted row.

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [Sort table data](./sort.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`merge_sorted`](../reference/table-operations/merge/merge-sorted.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [Javadoc: `merge`](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#merge(java.util.Collection))
- [Pydoc: `merge`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.merge)
- [Pydoc: `merge_sorted`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.merge_sorted)
