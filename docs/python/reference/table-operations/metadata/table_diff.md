---
title: table_diff
---

The `table_diff` method returns the differences between two provided tables as a string. If the two tables are the same, an empty string is returned.

This method starts by comparing the table sizes and then the schema of the two tables, such as the number of columns, column names, column types, and column orders. If the schemas are different, the comparison stops, and the differences are returned. If the schemas are the same, the method compares the table data. The method compares the data in the tables column by column (not row by row) and only records the first difference found in each column.

Note that inexact comparison of floating numbers may sometimes be desirable due to their inherent imprecision. When that is the case, the `floating_comparison` should be set to either 'absolute' or 'relative':

- `absolute` - the absolute value of the difference between two floating numbers is used to compare against a threshold. The threshold is set to 0.0001 for Doubles and 0.005 for Floats. Only differences that are greater than the threshold are recorded.
- `relative` - the relative difference between two floating numbers is used to compare against the threshold. The relative difference is calculated as the absolute difference divided by the smaller absolute value between the two numbers.

## Syntax

```python syntax
table_diff(
  t1: Table,
  t2: Table,
  max_diffs: int = 1,
  floating_comparison: Literal['exact', 'absolute', 'relative'] = 'exact',
  ignore_column_order: bool = False
)
```

## Parameters

<ParamTable>
<Param name="t1" type="Table">

The table to compare.

</Param>
<Param name="t2" type="Table">

The table to compare `t1` against.

</Param>
<Param name="max_diffs" type="int" optional>

The maximum number of differences to return. Default is `1`.

</Param>
<Param name="floating_comparison" type="Literal['exact', 'absolute', 'relative']" optional>

The type of comparison to use for floating numbers. Default is `'exact'`.

</Param>
<Param name="ignore_column_order" type="bool" optional>

Whether columns that exist in both tables but in different orders are treated as differences. `False` indicates that column order matters, and `True` indicates that column order does not matter. Default is `False`.

</Param>
</ParamTable>

## Returns

The differences between `t1` and `t2` as a string.

## Examples

```python order=:log
from deephaven import empty_table
from deephaven.table import table_diff

## Create some tables
t1 = empty_table(10).update(["A = i", "B = i", "C = i"])
t2 = empty_table(10).update(
    ["A = i", "C = i % 2 == 0? i: i + 1", "C = i % 2 == 0? i + 1: i"]
)

## get the diff string
d = table_diff(t1, t2, max_diffs=10).split("\n")

print(d)
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.table_diff)
