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

Row numbers `i` and `ii` are frequently used with the [`_` and `[]`](../../query-language/types/arrays.md) operators to retrieve values from prior or future rows in the table. For example, `Column_[ii-1]` references the value in `Column` one row prior to the current row.

### Examples

In the following example, a table is created with the row index as an `i` int, `ii` long and `k` long. The meta-data is assessed to see the variable types.

```groovy order=result,meta
result = emptyTable(10).update("I = i", "II = ii", "K = k")

meta = result.meta()
```

In the following example, row indices, `i` and `ii`, are used to access the rows before and after the current row in the table by using the [`_` and `[]`](../../query-language/types/arrays.md) operators.

> [!CAUTION]
> Because `i` and `ii` are used, this example will not reliably work on dynamic tables.

```groovy order=source,result
source = emptyTable(10).update("X = i")

result = source.update("A = X_[i-1]", "B = X_[i+1]", "C = X_[i+2]", "D = sqrt(X_[i-1] + X_[i+1])")
```

## Constants

The [`deephaven.util.QueryConstants`](/core/javadoc/io/deephaven/util/QueryConstants.html) class defines the global constants including Deephaven’s special numerical values. Other constants are defined at the individual module level because they are only locally applicable.

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
- `NULL_BYTE_BOXED`: Null boxed value of type `byte`.
- `NULL_CHAR`: Null value of type `char`.
- `NULL_CHAR_BOXED`: Null boxed value of type `char`.
- `NULL_DOUBLE`: Null value of type `double`.
- `NULL_DOUBLE_BOXED`: Null boxed value of type `double`.
- `NULL_FLOAT`: Null value of type `float`.
- `NULL_FLOAT_BOXED`: Null boxed value of type `float`.
- `NULL_INT`: Null value of type `int`.
- `NULL_INT_BOXED`: Null boxed value of type `int`.
- `NULL_LONG`: Null value of type `long`.
- `NULL_LONG_BOXED`: Null boxed value of type `long`.
- `NULL_SHORT`: Null value of type `short`.
- `NULL_SHORT_BOXED`: Null boxed value of type `short`.
- `POS_INFINITY_DOUBLE`: Positive infinity value of type `double`.
- `POS_INFINITY_FLOAT`: Positive infinity value of type `float`.

## Related documentation

- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`emptyTable`](../../table-operations/create/emptyTable.md)
- [`meta`](../../table-operations/metadata/meta.md)
- [operators](../../../how-to-guides/formulas-how-to.md#operators)
- [`update`](../../table-operations/select/update.md)
