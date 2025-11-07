---
title: merge_sorted
---

`merge_sorted` combines two or more tables into one sorted, aggregate table. This essentially stacks the tables one on top of the other and sorts the result. Null tables are ignored. `merge_sorted` is more efficient than using [`merge`](./merge.md) followed by [`sort`](../sort/sort.md).

## Syntax

```
merge_sorted(tables: list[Table], order_by: str) -> Table
```

## Parameters

<ParamTable>
<Param name="tables" type="list[Table]">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Each table must be already sorted.
- Null inputs are skipped.

</Param>
<Param name="order_by" type="str">

The column by which to sort the merged table.

</Param>
</ParamTable>

## Returns

A new table with the source tables stacked one on top of the other and sorted by the specified column.

## Examples

In the following example, `source1` is stacked on top of `source2`, and the result is sorted based on the `Number` column.

```python order=source1,source2,result
from deephaven import merge_sorted, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "D", "E"]), int_col("Number", [3, 4, 7])]
)
source2 = new_table(
    [string_col("Letter", ["B", "C", "D"]), int_col("Number", [1, 2, 5])]
)

result = merge_sorted([source1, source2], "Letter")
```

In the following example, three tables are merged and sorted based on the `Number` column.

```python order=source1,source2,source3,result
from deephaven import merge_sorted, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "C", "G"]), int_col("Number", [1, 6, 9])]
)
source2 = new_table(
    [string_col("Letter", ["B", "D", "G"]), int_col("Number", [3, 5, 8])]
)
source3 = new_table(
    [string_col("Letter", ["D", "E", "F"]), int_col("Number", [2, 4, 7])]
)

result = merge_sorted([source1, source2, source3], "Number")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to vertically stack tables](../../../how-to-guides/merge-tables.md)
- [Joins: Exact and Relational](../../../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../../../how-to-guides/joins-timeseries-range.md)
- [sort](../sort/sort.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#mergeSorted(java.lang.String,io.deephaven.engine.table.Table...))
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.merge_sorted)
