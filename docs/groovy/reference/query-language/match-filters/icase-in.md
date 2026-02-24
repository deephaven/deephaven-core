---
title: icase in
---

The `icase in` match filter returns rows that contain a match of one or more specified values, regardless of the capitalization of the values.

## Syntax

```
columnName icase in valueList
```

- `columnName` - the column the filter will search for matching values.
- `valueList` - the set of values to match on. This supports:
  - a comma-separated list of variables: `A icase in X, Y, Z`. The filter will return `true` for all rows where the value in column `A` is equal to `X`, `Y`, or `Z`.
  - a java array: `A icase in X`. The filter will return `true` for all rows where `A` is equal to at least one element of the java array `X`.
  - a `java.util.Collection`: `A icase in X`. The filter will return `true` for all rows where `A` is equal to at least one element of the collection `X`.
  - all other types: `A icase in X`. The filter will return `true` for all rows where `A` is equal to `X`.

## Examples

The following example returns rows where `Color` is in the comma-separated list of values. Capitalization is ignored.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color icase in `Blue`, `Orange`")
```

The following example returns rows where `Color` is `blue` _or_ `Letter` is `a`. Capitalization is ignored.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Color icase in `Blue`", "Letter icase in `a`")))
```

The following example returns rows where `Color` is in a collection of values.

```groovy order=source,result
myList = ["Pink", "purple", "BLUE"]

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color icase in myList")
```

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [equals](./equals.md)
- [not equals (`!=`)](./not-equals.md)
- [`in`](./in.md)
- [`not in`](./not-in.md)
- [`icase not in`](./icase-not-in.md)
- [`where`](../../table-operations/filter/where.md)
- [`whereIn`](../../table-operations/filter/where-in.md)
- [`whereNotIn`](../../table-operations/filter/where-not-in.md)
