---
title: merge
slug: ./merge
---

`merge` combines two or more tables into one aggregate table. This essentially appends the tables one on top of the other. Null tables are ignored.

## Syntax

```
merge(tables: list[Table]) -> Table
```

## Parameters

<ParamTable>
<Param name="tables" type="list[Table]">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Null inputs are skipped.

</Param>
</ParamTable>

## Returns

A new table with the source tables stacked one on top of the other. The resulting table's rows will maintain the same order as the source tables. If the source tables tick, rows will be inserted within the merged table where they appear in the source (rather than at the end of the merged table).

## Examples

In the following example, `source1` is stacked on top of `source2`.

```python order=source1,source2,result
from deephaven import merge, merge_sorted, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "B", "D"]), int_col("Number", [1, 2, 3])]
)
source2 = new_table(
    [string_col("Letter", ["C", "D", "E"]), int_col("Number", [14, 15, 16])]
)

result = merge([source1, source2])
```

In the following example, three tables are merged.

```python order=source1,source2,source3,result
from deephaven import merge, merge_sorted, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "B", "D"]), int_col("Number", [1, 2, 3])]
)
source2 = new_table(
    [string_col("Letter", ["C", "D", "E"]), int_col("Number", [14, 15, 16])]
)
source3 = new_table(
    [string_col("Letter", ["E", "F", "A"]), int_col("Number", [22, 25, 27])]
)

result = merge([source1, source2])
```

In the following example, three tables are merged using an array of tables.

```python order=source1,source2,source3,result
from deephaven import merge, new_table
from deephaven.column import int_col, string_col

source1 = new_table(
    [string_col("Letter", ["A", "B", "D"]), int_col("Number", [1, 2, 3])]
)
source2 = new_table(
    [string_col("Letter", ["C", "D", "E"]), int_col("Number", [14, 15, 16])]
)
source3 = new_table(
    [string_col("Letter", ["E", "F", "A"]), int_col("Number", [22, 25, 27])]
)
table_array = [source1, source2, source3]

result = merge(table_array)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to vertically stack tables](../../../how-to-guides/merge-tables.md)
- [Joins: Exact and Relational](../../../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#merge(java.util.Collection))
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.merge)
