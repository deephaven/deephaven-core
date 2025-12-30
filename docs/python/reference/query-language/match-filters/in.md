---
title: in
---

The `in` match filter returns rows that contain a match of one or more specified values.

## Syntax

```
columnName in valueList
```

- `columnName` - the column the filter will search for matching values.
- `valueList` - the set of values to match on. This supports:
  - a comma-separated list of values: `A in X, Y, Z`. The filter will return `true` for all rows where the value in column `A` is equal to `X`, `Y`, or `Z`.
  - a java array: `A in X`. The filter will return `true` for all rows where `A` is equal to at least one element of the java array `X`.
  - a `java.util.Collection`: `A in X`. The filter will return `true` for all rows where `A` is equal to at least one element of the collection `X`.
  - all other types: `A in X`. The filter will return `true` for all rows where `A` is equal to `X`.

## Examples

The following example returns rows where `Color` is in a comma-separated list of values.

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


result = source.where(filters=["Color in `blue`, `orange`"])
```

The following example returns rows where `Number` is in an array of values.

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

result = source.where(filters=["Number in array"])
```

The following example returns rows where `Number` is in an array of values _or_ `Code` is in an array of values.

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
result = source.where_one_of(filters=["Number in number_array", "Code in code_array"])
```

The following example returns rows where `Color` is in a collection of values.

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


result = source.where(filters=["Color in my_list"])
```

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [equals](./equals.md)
- [not equals (`!=`)](./not-equals.md)
- [`not in`](./not-in.md)
- [`icase in`](./icase-in.md)
- [`icase not in`](./icase-not-in.md)
- [`where`](../../table-operations/filter/where.md)
- [`where_in`](../../table-operations/filter/where-in.md)
- [`where_not_in`](../../table-operations/filter/where-not-in.md)
