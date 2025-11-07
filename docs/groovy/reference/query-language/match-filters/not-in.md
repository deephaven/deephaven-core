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

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color not in `blue`, `orange`")
```

The following example returns rows where `Number` is _not_ in an array of values.

```groovy order=source,result
array = [2, 4, 6]

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Number not in array")
```

The following example returns rows where `Number` is _not_ in an array of values _or_ `Code` is in an array of values.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

numberArray = [2, 4, 6]
codeArray = [10, 12, 14]

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Number not in numberArray", "Code in codeArray")))
```

The following example returns rows where `Color` is _not_ in a collection of values.

```groovy order=source,result
myList = ["pink", "purple", "blue"]

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color not in myList")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/filters.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [where](../../table-operations/filter/where.md)
- [where_in](../../table-operations/filter/where-in.md)
- [where_not_in](../../table-operations/filter/where-not-in.md)
