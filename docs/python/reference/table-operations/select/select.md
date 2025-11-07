---
title: select
slug: ./select
---

The `select` method creates a new in-memory table that includes one column for each argument. Any columns not specified in the arguments will not appear in the resulting table.

When using `select`, the entire requested dataset is evaluated and stored in memory.

> [!NOTE]
> The syntax for the `select` and [`view`](./view.md) methods is identical, as is the resulting table. `select` is recommended when:
>
> 1. not all the source columns are desired in the result,
> 2. the formula is expensive to evaluate,
> 3. cells are accessed many times, and/or
> 4. a large amount of memory is available.
>
> When memory usage or computation needs to be reduced, consider using `view`, `update_view`, `update`, or `lazy_update`. These methods have different memory and computation expenses.

## Syntax

```
select(formulas: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="formulas" type="Union[str, Sequence[str]]">

Formulas to compute columns in the new table:

- `NULL`: returns all columns
- Column from `table`: `"A"` (equivalent to `"A = A"`)
- Renamed column from `table`: `"X = A"`
- Calculated column: `"X = A * sqrt(B)"`

</Param>
</ParamTable>

## Returns

A new in-memory table that includes one column for each argument. If no arguments are provided, there will be one column for each column of the source table.

## Examples

In the following example, `select` has zero arguments. All columns are selected. While this selection appears to do nothing, it is creating a compact, in-memory representation of the input table, with all formulas evaluated.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 6, 7, 8]),
    ]
)

result = source.select()
```

In the following example, column `B` is selected for the new table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 6, 7, 8]),
    ]
)

result = source.select(formulas=["B"])
```

In the following example, the new table contains source column `A` (renamed as `X`), and column `B`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 6, 7, 8]),
    ]
)
result = source.select(formulas=["X = A", "B"])
```

In the following example, mathematical operations are evaluated and stored in memory for the new table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 6, 7, 8]),
    ]
)

result = source.select(formulas=["A", "X = B", "Y = sqrt(C)"])
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#select(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.select)
