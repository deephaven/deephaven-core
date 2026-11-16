---
title: Operators
---

An operator is a symbol, like `+` or `-`, that tells a program to perform a specific action on values or variables. You use operators inside [query strings](./query-string-overview.md), written in the Deephaven Query Language (DQL), to build [filters](./filters.md) and [formulas](./formulas.md). Deephaven provides access to [most of Java's operators](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/operators.html).

Filters and formulas work on real-time tables as well as static ones, so the results of your operators update as the data changes.

The following code block uses a variety of operators to create new columns of values.

```python
from deephaven import empty_table


class MyObj:
    def __init__(self, a):
        self.a = a

    def compute(self, value1):
        return self.a + value1


obj = MyObj(1)

result = empty_table(10).update(
    formulas=[
        "A = i",
        "B = A * A",
        "C = A / 2",
        "D = A % 3",
        "E = (int)C",
        "F = A_[i - 2]",
        "G = (int)obj.a",
        "H = (int)obj.compute(A)",
        "I = sqrt(A)",
    ]
)
```

`A = i` uses the [assignment operator](#assignment-operators) to fill `A` with each row's position, read from the [special variable](../reference/query-language/variables/special-variables.md) `i`. Each column after `A` demonstrates one or more operators:

- `B`, `C`, and `D` use the [arithmetic operators](#arithmetic-operators) `*`, `/`, and `%`. Dividing two integers returns a `double`, so `C` is a `double` column.
- `E` uses a [cast](#other-java-operators) to convert `C` to an `int`.
- `F` uses the [underscore and index operators](#access-operators) to read the value of `A` two rows earlier. In the first two rows, there is no earlier row to read, so `F` is `null`.
- `G` and `H` use the [dot operator](#access-operators): `G` reads the attribute `a` of `obj`, and `H` calls its `compute` method. The engine can't infer the type of a Python attribute or of a method called through an object, so each one has an [`(int)` cast](#other-java-operators). Without the cast, `G` would be an [`org.jpy.PyObject`](./pyobjects.md) column. See [Python classes and objects in query strings](./python-classes.md) for more information.
- `I` uses the [function call operator](#other-java-operators) to call the built-in [`sqrt`](../reference/query-language/query-library/auto-imported/math.md) function.

The following example uses the [comparison operators](#comparison-operators) `>`, `>=`, `<`, and `<=` in [filters](./filters.md) to keep only the matching rows of the `source` table.

```python order=source,greater_than,greater_than_or_equal,less_than,less_than_or_equal
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Value", [0, 1, 2, 3, 4, 5, 6])])

greater_than = source.where(filters=["Value > 3"])
greater_than_or_equal = source.where(filters=["Value >= 3"])
less_than = source.where(filters=["Value < 3"])
less_than_or_equal = source.where(filters=["Value <= 3"])
```

## Available operators

The subsections below group the operators available in [query strings](./query-string-overview.md) by category.

### Arithmetic operators

| Symbol | Name           | Description                                                                                                                         |
| ------ | -------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `+`    | Addition       | Adds values. If either operand is a [`String`](../reference/query-language/types/strings.md), concatenates the operands as strings. |
| `-`    | Subtraction    | Subtracts the right value from the left value.                                                                                      |
| `-`    | Negation       | Negates a single value, as in `-A`.                                                                                                 |
| `*`    | Multiplication | Multiplies the left and right values.                                                                                               |
| `/`    | Division       | Divides the left value by the right value. Dividing two integer values returns a `double`.                                          |
| `%`    | Modulus        | Divides the left value by the right value and returns the remainder.                                                                |

### Access operators

| Symbol | Name       | Description                                                                                                                                                         |
| ------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_`    | Underscore | Appended to a column name, as in `A_`, accesses the whole column as an [array](./work-with-arrays.md). This operator is Deephaven-specific and is not part of Java. |
| `[]`   | Index      | Reads an [array](./work-with-arrays.md) element by position, as in `A_[i - 2]`. For a column array such as `A_`, a position outside the column returns `null`.      |
| `.`    | Dot        | Accesses a field or method of an object, class, or package, as in `obj.compute(A)`.                                                                                 |

### Assignment operators

| Symbol | Name       | Description                                                                                                                                                                                                                                                                            |
| ------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `=`    | Assignment | In a [formula](./formulas.md), the first `=` assigns the expression on the right to the column named on the left. Everywhere else in a query string, [`=`](../reference/query-language/match-filters/equals.md) tests for equality. See [Comparison operators](#comparison-operators). |

### Comparison operators

| Symbol                                                          | Name                  | Description                                                                   |
| --------------------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------- |
| [`==`](../reference/query-language/match-filters/equals.md)     | Equal to              | Returns `true` if the two values are equal. Two `null` values are equal.      |
| [`!=`](../reference/query-language/match-filters/not-equals.md) | Not equal to          | Returns `true` if the two values are not equal.                               |
| `>`                                                             | Greater than          | Returns `true` if the left value is greater than the right value.             |
| `>=`                                                            | Greater than or equal | Returns `true` if the left value is greater than or equal to the right value. |
| `<`                                                             | Less than             | Returns `true` if the left value is less than the right value.                |
| `<=`                                                            | Less than or equal    | Returns `true` if the left value is less than or equal to the right value.    |

A single [`=`](../reference/query-language/match-filters/equals.md) also tests for equality, the same as [`==`](../reference/query-language/match-filters/equals.md). This applies everywhere except the first `=` in a [formula](./formulas.md), which assigns the column. For example, the [filter](./filters.md) `Value = 3` keeps rows where `Value` is 3, and `A = 3 && B = 5` keeps rows where `A` is 3 and `B` is 5. Prefer [`==`](../reference/query-language/match-filters/equals.md), which makes the intent clear.

> [!NOTE]
> Deephaven orders `null` before every non-null value, and comparisons follow that order. When `A` is `null`, `A < 10` and `A <= 10` are `true`. To exclude nulls, add an explicit check with [`isNull`](../reference/query-language/types/nulls.md), as in `!isNull(A) && A < 10`. See [Null, NaN, and infinity values](./null-inf-nan.md).

> [!NOTE]
> Comparisons with `NaN` follow IEEE 754 rules. When either operand is `NaN`, [`==`](../reference/query-language/match-filters/equals.md), `<`, `<=`, `>`, and `>=` return `false`, and [`!=`](../reference/query-language/match-filters/not-equals.md) returns `true`. Set inclusion with [`in`](../reference/query-language/match-filters/in.md) is the exception: the filter `A in NaN, 10.0` keeps `NaN` rows. To test for `NaN` explicitly, use [`isNaN`](../reference/query-language/types/NaNs.md), as in `isNaN(A) || A < 10.0`.

To see these rules in action, let's [filter](./filters.md) a column that holds a null, an ordinary value, and a `NaN`:

```python order=source,less_than_ten,non_null_less_than_ten,nan_or_ten,equals_five
from deephaven import new_table
from deephaven.column import double_col
from deephaven.constants import NULL_DOUBLE, NAN_DOUBLE

source = new_table([double_col("A", [NULL_DOUBLE, 5.0, NAN_DOUBLE])])

less_than_ten = source.where(filters=["A < 10"])
non_null_less_than_ten = source.where(filters=["!isNull(A) && A < 10"])
nan_or_ten = source.where(filters=["A in NaN, 10.0"])
equals_five = source.where(filters=["A = 5"])
```

- `less_than_ten` keeps the null row and the `5.0` row. The null row passes `A < 10` because `null` sorts first, and the `NaN` row fails it.
- `non_null_less_than_ten` adds an [`isNull`](../reference/query-language/types/nulls.md) check, so it keeps only the `5.0` row.
- `nan_or_ten` keeps the `NaN` row, because set inclusion with [`in`](../reference/query-language/match-filters/in.md) matches `NaN`.
- `equals_five` uses a single [`=`](../reference/query-language/match-filters/equals.md) to test for equality and keeps the `5.0` row.

### Logical operators

| Symbol            | Name                            | Description                                                                                   |
| ----------------- | ------------------------------- | --------------------------------------------------------------------------------------------- |
| `!`               | Logical NOT                     | Inverts the value of a boolean.                                                               |
| `&`               | Logical AND (non-short-circuit) | Returns `true` if both operands are `true`. Always evaluates both operands.                   |
| <code>\|</code>   | Logical OR (non-short-circuit)  | Returns `true` if either operand is `true`. Always evaluates both operands.                   |
| `^`               | Logical XOR                     | Returns `true` if exactly one operand is `true`.                                              |
| `&&`              | Logical AND                     | Returns `true` if both operands are `true`. Skips the right operand when the left is `false`. |
| <code>\|\|</code> | Logical OR                      | Returns `true` if either operand is `true`. Skips the right operand when the left is `true`.  |

See [Combine filters](./filters.md#combine-filters) for examples that combine [filter](./filters.md) conditions with `&&` and `||`.

### Bitwise operators

With integer operands (`byte`, `short`, `char`, `int`, or `long`), `&`, `|`, and `^` operate on the individual bits of each value. With boolean operands, they are the [logical operators](#logical-operators) described above.

| Symbol          | Name        | Description                                                                             |
| --------------- | ----------- | --------------------------------------------------------------------------------------- |
| `&`             | Bitwise AND | Sets each result bit to 1 if both corresponding operand bits are 1, and to 0 otherwise. |
| <code>\|</code> | Bitwise OR  | Sets each result bit to 1 if either corresponding operand bit is 1, and to 0 otherwise. |
| `^`             | Bitwise XOR | Sets each result bit to 1 if the corresponding operand bits differ, and to 0 otherwise. |

[Query strings](./query-string-overview.md) don't support the bitwise complement (`~`) or shift (`<<`, `>>`, `>>>`) operators.

### Other Java operators

| Symbol   | Name                                          | Description                                                               |
| -------- | --------------------------------------------- | ------------------------------------------------------------------------- |
| `(type)` | [Casting](./casting.md)                       | Casts from one type to another.                                           |
| `()`     | Function call                                 | Calls a function.                                                         |
| `?:`     | [Ternary conditional](./ternary-if-how-to.md) | Returns one of two values depending on the value of a boolean expression. |

[Query strings](./query-string-overview.md) don't support these Java operators:

- `instanceof`
- Unary plus, as in `+A`
- Increment (`++`) and decrement (`--`)
- Compound assignment, such as `+=`

### Filter-only operators

These Deephaven-specific operators work only in [filter](./filters.md) strings, such as those passed to [`where`](../reference/table-operations/filter/where.md). Three rules apply to them:

- [Formulas](./formulas.md) can't use them.
- The left side of each must be a column name.
- Each one must be the whole filter string, so you can't combine it with other conditions using `&&` or `||`.

To combine one with another condition, pass the conditions as separate filters, as in `where(["X in 2, 4", "Y > 3"])`. The result keeps only rows that match every filter. To keep rows that match either condition instead, combine the filters with [`or_`](/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_), as in `where(or_(["X in 2, 4", "Y > 3"]))`.

See [Match filters](./filters.md#match-filters) and [String filters](./filters.md#string-filters) for examples.

| Symbol                                                                                                                                           | Name                       | Description                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`in`](../reference/query-language/match-filters/in.md)                                                                                          | Set inclusion              | Returns `true` if the column value matches any of the listed values, as in `X in 2, 4, 6`.                                                                                                                                    |
| [`not in`](../reference/query-language/match-filters/not-in.md)                                                                                  | Set exclusion              | Returns `true` if the column value matches none of the listed values.                                                                                                                                                         |
| [`icase in`](../reference/query-language/match-filters/icase-in.md), [`icase not in`](../reference/query-language/match-filters/icase-not-in.md) | Case-insensitive set match | Work like [`in`](../reference/query-language/match-filters/in.md) and [`not in`](../reference/query-language/match-filters/not-in.md), but ignore case when matching [strings](../reference/query-language/types/strings.md). |
| [`includes`](./filters.md#string-filters)                                                                                                        | Substring match            | Returns `true` if a [string](../reference/query-language/types/strings.md) column contains the listed substring. With several substrings, works like [`includes any`](./filters.md#string-filters).                           |
| [`includes any`](./filters.md#string-filters)                                                                                                    | Any-substring match        | Returns `true` if a [string](../reference/query-language/types/strings.md) column contains any of the listed substrings.                                                                                                      |
| [`includes all`](./filters.md#string-filters)                                                                                                    | All-substring match        | Returns `true` if a [string](../reference/query-language/types/strings.md) column contains all of the listed substrings.                                                                                                      |

Prefix [`includes`](./filters.md#string-filters) with `icase` to ignore case, `not` to invert the match, or both, as in `icase not includes`.

## Related documentation

- [Query strings](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Filter table data](./use-filters.md)
- [Select and create columns](./use-select-view-update.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Arrays](./work-with-arrays.md)
- [Arrays reference](../reference/query-language/types/arrays.md)
- [Casting](./casting.md)
- [Casting reference](../reference/query-language/control-flow/casting.md)
- [Use the ternary conditional operator in query strings](./ternary-if-how-to.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Query language built-in constants](./built-in-constants.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [`isNull`](../reference/query-language/types/nulls.md)
- [`isNaN`](../reference/query-language/types/NaNs.md)
- [`sqrt`](../reference/query-language/query-library/auto-imported/math.md)
- [`=`](../reference/query-language/match-filters/equals.md)
- [`!=`](../reference/query-language/match-filters/not-equals.md)
- [`in`](../reference/query-language/match-filters/in.md)
- [`not in`](../reference/query-language/match-filters/not-in.md)
- [`icase in`](../reference/query-language/match-filters/icase-in.md)
- [`icase not in`](../reference/query-language/match-filters/icase-not-in.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`new_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
- [`int_col` Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.int_col)
- [`double_col` Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.double_col)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
- [`where` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where)
- [`or_` Pydoc](/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_)
