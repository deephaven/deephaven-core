---
title: Special variables and constants
---

Deephaven provides special variables and constants to increase ease of access to row indexes and save users time when writing queries.

## Special variables

Special variables inside the query language allow access to the row index of a table.

- `i` is a row index, as a primitive int.
- `ii` is a row index, as a primitive long.
- `k` is a Deephaven internal indexing value, as a primitive long.

> [!NOTE]
> The variables `i` and `ii` both represent row numbers. Integers are limited to values up to 2^31-1 (2,147,483,647), while longs can represent values up to 2^63-1. In other words, to avoid precision problems, use the `ii` variable, unless an `int` needs to be passed to another function. Using the `i` variable in a table with more than 2 billion rows will result in an error.

> [!NOTE]
> `k` does not correspond to traditional row numbers and should only be used in limited circumstances, such as debugging or advanced query operations.

> [!CAUTION]
> These variables are unreliable within a ticking table. Inconsistent results occur since previously created row indexes do not automatically update.

> [!CAUTION]
> Do not use `i` and `ii` in add-only tables to access preceding or following column values using array notation (e.g., `ColA_[ii-1]`). In add-only tables, rows can be inserted at any position, causing row indices to shift and previously computed values to reference different rows than originally intended. See [Alternatives for add-only tables](../../../how-to-guides/built-in-variables.md#alternatives-for-add-only-tables) for recommended approaches.

Row numbers `i` and `ii` are frequently used with the [`_` and `[]`](../../query-language/types/arrays.md) operators to retrieve values from prior or future rows in the table. For example, `Column_[ii-1]` references the value in `Column` one row before the current row.

### Examples

In the following example, a table is created with the row index as an `i` int, `ii` long and `k` long. The meta-data is assessed to see the variable types.

```python order=result,meta
from deephaven import empty_table

result = empty_table(10).update(formulas=["I = i", "II = ii", "K = k"])

meta = result.meta_table
```

In the following example, row indices, `i` and `ii`, are used to access the rows before and after the current row in the table by using the [`_` and `[]`](../../query-language/types/arrays.md) operators.

> [!CAUTION]
> Because `i` and `ii` are used, this example will not reliably work on dynamic tables.

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = i"])

result = source.update(
    formulas=[
        "A = X_[i-1]",
        "B = X_[i+1]",
        "C = X_[i+2]",
        "D = sqrt(X_[i-1] + X_[i+1])",
    ]
)
```

## Deephaven global constants

The [`deephaven.constants`](/core/pydoc/code/deephaven.constants.html) module defines the global constants including Deephaven’s special numerical values. Other constants are defined at the individual module level because they are only locally applicable.

Deephaven provides the following global constants:

- `MAX_BYTE`: The maximum value of type `byte`.
- `MAX_CHAR`: The maximum value of type `char`.
- `MAX_DOUBLE`: The maximum value of type `double`.
- `MAX_FINITE_DOUBLE`: The maximum finite value of type `double`.
- `MAX_FINITE_FLOAT`: The maximum finite value of type `float`.
- `MAX_FLOAT`: The maximum value of type `float`.
- `MAX_INT`: The maximum value of type `int`.
- `MAX_LONG`: The maximum value of type `long`.
- `MAX_SHORT`: The maximum value of type `short`.
- `MIN_BYTE`: The minimum value of type `byte`.
- `MIN_CHAR`: The minimum value of type `char`.
- `MIN_DOUBLE`: The minimum value of type `double`.
- `MIN_FINITE_DOUBLE`: The minimum finite value of type `double`.
- `MIN_FINITE_FLOAT`: The minimum finite value of type `float`.
- `MIN_FLOAT`: The minimum value of type `float`.
- `MIN_INT`: The minimum value of type `int`.
- `MIN_LONG`: The minimum value of type `long`.
- `MIN_POS_DOUBLE`: The minimum positive value of type `double`.
- `MIN_POS_FLOAT`: The minimum positive value of type `float`.
- `MIN_SHORT`: The minimum value of type `short`.
- `NAN_DOUBLE`: Not-a-number (NaN) value of type `double`.
- `NAN_FLOAT`: Not-a-number (NaN) value of type `float`.
- `NEG_INFINITY_DOUBLE`: Negative infinity value of type `double`.
- `NEG_INFINITY_FLOAT`: Negative infinity value of type `float`.
- `NULL_BOOLEAN`: Null value of type `bool`.
- `NULL_BYTE`: Null value of type `byte`.
- `NULL_CHAR`: Null value of type `char`.
- `NULL_DOUBLE`: Null value of type `double`.
- `NULL_FLOAT`: Null value of type `float`.
- `NULL_INT`: Null value of type `int`.
- `NULL_LONG`: Null value of type `long`.
- `NULL_SHORT`: Null value of type `short`.
- `POS_INFINITY_DOUBLE`: Positive infinity value of type `double`.
- `POS_INFINITY_FLOAT`: Positive infinity value of type `float`.

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Filters](../../../how-to-guides/filters.md)
- [Formulas](../../../how-to-guides/formulas.md)
- [Operators](../../../how-to-guides/operators.md)
- [`empty_table`](../../table-operations/create/emptyTable.md)
- [`update`](../../table-operations/select/update.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
