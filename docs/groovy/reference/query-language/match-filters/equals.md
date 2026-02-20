---
title: equals
---

The equals (`=`) match filter returns rows that are an exact match to the specified value.

## Syntax

```
columnName = value
columnName == value
```

- `columnName` - the column the filter will search for the matching value.
- `value` - the value to match on.

## Examples

The following example returns rows where `Color` is `blue`.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`")
```

The following example returns rows where `Color` is `blue` and `Code` is 14.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`", "Code = 14" )
```

The following example returns rows where `Color` is `blue` _or_ `Code` is 14.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Color = `blue`", "Code = 14")))
```

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [not equals (`!=`)](./not-equals.md)
- [`in`](./in.md)
- [`not in`](./not-in.md)
- [`icase in`](./icase-in.md)
- [`icase not in`](./icase-not-in.md)
- [`where`](../../table-operations/filter/where.md)
- [`whereIn`](../../table-operations/filter/where-in.md)
- [`whereNotIn`](../../table-operations/filter/where-not-in.md)