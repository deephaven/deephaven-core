---
title: where
---

The `where` method filters rows of data from the source table.

> [!NOTE]
> The engine will address filters in series, consistent with the ordering of arguments. It is _best practice_ to place filters related to partitioning and grouping columns first, as significant data volumes can then be excluded. Additionally, match filters are highly optimized and should usually come before conditional filters.

## Syntax

```
table.where(filters...)
```

## Parameters

<ParamTable>
<Param name="filters" type="String...">

Formula for filtering.

Filters can be:

- A match filter. <!--TODO: add links [#474](https://github.com/deephaven/deephaven.io/issues/474) -->
- A conditional filter. This may be a custom boolean function. <!--TODO: add links [#201](https://github.com/deephaven/deephaven.io/issues/201) -->

</Param>
<Param name="filters" type="Collection">

Collection of formulas for filtering.

</Param>
</ParamTable>

## Returns

A new table with only the rows meeting the filter criteria in the column(s) of the source table.

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

The following example returns rows where `Number` is greater than 3.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Number > 3")
```

The following returns rows where `Color` is `blue` and `Number` is greater than 3.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where("Color = `blue`", "Number > 3")
```

The following returns rows where `Color` is `blue` or `Number` is greater than 3.

```groovy order=source,result
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.Filter

source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.where(FilterOr.of(Filter.from("Color = `blue`", "Number > 3")))
```

The following shows how to apply a custom function as a filter. Take note that the function call must be explicitly cast to a `(boolean)`.

```groovy order=source,result_filtered,result_not_filtered
my_filter = { int a -> a <= 4 }

source = newTable(
    intCol("IntegerColumn", 1, 2, 3, 4, 5, 6, 7, 8)
)

result_filtered = source.where("(boolean)my_filter(IntegerColumn)")
result_not_filtered = source.where("!((boolean)my_filter(IntegerColumn))")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [equals](../../query-language/match-filters/equals.md)
- [not equals](../../query-language/match-filters/not-equals.md)
- [icase in](../..//query-language/match-filters/icase-in.md)
- [icase not in](../../query-language/match-filters/icase-not-in.md)
- [in](../../query-language/match-filters/in.md)
- [not in](../../query-language/match-filters/not-in.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
