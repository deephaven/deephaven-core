---
title: not equals
---

The not equals (`!=`) match filter returns rows that **do not** exactly match the specified value.

## Syntax

```
columnName != value
```

- `columnName` - the column the filter will search for non-matching values.
- `value` - the value to match on.

## Examples

The following example returns rows where `Color` is _not_ `blue`.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color != `blue`")
```

The following example returns rows where `Color` is _not_ `blue` and `Code` is _not_ 14.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color != `blue`", "Code != 14" )
```

The following example returns rows where `Color` is _not_ `blue` _or_ `Code` is _not_ 14.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Color != `blue`", "Code != 14")))
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/filters.md)
- [where](../../table-operations/filter/where.md)
