---
title: not in
---

The `not in` match filter returns rows that **do not** contain a match of one or more specified values.

## Syntax

```
columnName not in valueList
```

- `columnName` - the column the filter will search for non-matching values.
- `valueList` - the set of values to remove. This supports:
  - a comma-separated list of values: `A not in X, Y, Z`. The filter will return `true` for all rows where the value in column `A` is not equal to `X`, `Y`, and `Z`.
  - a java array: `A not in X`. The filter will return `true` for all rows where `A` is not equal to every element of the java array `X`.
  - a `java.util.Collection`: `A not in X`. The filter will return `true` for all rows where `A` is not equal to every element of the collection `X`.
  - all other types: `A not in X`. The filter will return `true` for all rows where `A` is not equal to `X`.

## Examples

The following example returns rows where `Color` is _not_ in the comma-separated list of values.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)


result = source.where(filters=["Color not in `blue`, `orange`"])
```

The following example returns rows where `Number` is _not_ in an array of values.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

array = [2, 4, 6]
source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)

result = source.where(filters=["Number not in array"])
```

The following example returns rows where `Number` is _not_ in an array of values _or_ `Code` is in an array of values.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

number_array = [2, 4, 6]
code_array = [10, 12, 14]
source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)
result = source.where_one_of(
    filters=["Number not in number_array", "Code in code_array"]
)
```

The following example returns rows where `Color` is _not_ in a collection of values.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

my_list = ["pink", "purple", "blue"]

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)

result = source.where(filters=["Color not in my_list"])
```

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [equals](./equals.md)
- [`in`](./in.md)
- [`icase in`](./icase-in.md)
- [`icase not in`](./icase-not-in.md)
- [not equals (`!=`)](./not-equals.md)
- [`where`](../../table-operations/filter/where.md)
- [`where_in`](../../table-operations/filter/where-in.md)
- [`where_not_in`](../../table-operations/filter/where-not-in.md)

<!--TODO: [#409](https://github.com/deephaven/deephaven.io/issues/409) publish link to !=  when it is a match filter. -->
