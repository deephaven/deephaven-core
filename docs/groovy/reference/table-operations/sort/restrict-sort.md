---
title: restrictSortTo
---

`restrictSortTo` only allows sorting on specified table columns. This can be useful to prevent users from accidentally performing expensive sort operations as they interact with tables in the UI.

## Syntax

```
table.restrictSortTo(allowedSortingColumns...)
```

## Parameters

<ParamTable>
<Param name="allowedSortingColumns" type="String...">

The column(s) on which sorting is allowed.

</Param>
</ParamTable>

## Returns

A new table that only allows sorting on the columns specified in the `allowedSortingColumns` argument.

## Examples

```groovy test-set=1 order=result,tickerSorted,exchangeSorted
source = newTable(
    stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "GOOG", "GOOG"),
    stringCol("Exchange", "Nyse", "Arca", "Nyse", "Arca", "Nyse", "Arca"),
    intCol("Price", 75, 80, 110, 100, 25, 30),
)

result = source.restrictSortTo("Ticker", "Exchange")

tickerSorted = result.sort("Ticker")
exchangeSorted = result.sort("Exchange")
```

The table can be sorted by the columns `Ticker` or `Exchange` with success:

An attempt to sort a column outside the arguments of `restrictSortTo` will result in an error:

```groovy skip-test
priceSorted = result.sort("Price")
```

![An error message stating that sorting is not allowed on this table](../../../assets/reference/sorts/restrict_sort2.png)

<!-- TODO: issue 594 - error fix; update image https://github.com/deephaven/deephaven-core/issues/594 -->

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/GridAttributes.html#restrictSortTo(java.lang.String...))
