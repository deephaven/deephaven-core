---
title: oneClick
---

The `oneClick` method creates a [`SelectableDataSetOneClick`](../query-language/types/SelectableDataSetOneClick.md) with the specified columns from a source table. This is useful for methods like dynamic plotting, where Deephaven requires a [`SelectableDataSetOneClick`](../query-language/types/SelectableDataSetOneClick.md) instead of a standard table to execute certain operations.

## Syntax

```groovy syntax
oneClick(pTable)
oneClick(pTable, requireAllFiltersToDisplay)
oneClick(t, requireAllFiltersToDisplay, byColumns)
oneClick(t, byColumns...)
```

## Parameters

<ParamTable>
<Param name="pTable" type="PartitionedTable">

The source table.

</Param>
<Param name="t" type="Table">

The source table.

</Param>
<Param name="byColumns" type="String...">

The selected columns.

</Param>
<Param name="requireAllFiltersToDisplay" type="bool">

Whether to display data when some, but not all, [Input Filters](../../how-to-guides/user-interface/filters.md#input-filters) are applied. `False` will display data when not all filters are selected; `True` will only display data when appropriate filters are selected.

</Param>
</ParamTable>

## Returns

A `SelectableDataSetOneClick`.

## Examples

In this example, we create a source table, then use `oneClick` to create a [`SelectableDataSetOneClick`](../../reference/query-language/types/SelectableDataSetOneClick.md) copy of the table. Then, we use [`plot`](../plot/plot.md) to turn our [`SelectableDataSetOneClick`](../query-language/types/SelectableDataSetOneClick.md) into a plot, which can then be filtered via [**Controls > Input Filter**](../../how-to-guides/user-interface/filters.md#input-filters) in the user interface.

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
oc = oneClick(source, "Instrument")
plot = plot("Plot", oc, "Timestamp", "Price").show()
```

In this example, we create two plots, demonstrating the `requireAllFiltersToDisplay` parameter's function.

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
oc = oneClick(source, true, "Instrument")
oc2 = oneClick(source, false, "Instrument")
plot = plot("Plot", oc, "Timestamp", "Price").show()
plot2 = plot("Plot", oc2, "Timestamp", "Price").show()
```

![A dialog appears on the newly created `plot` stating that a filter control or table link must be created to display the plot](../../assets/reference/oneClickTrueFalse.png)

<!--Due to a potential snapshot tool issue, I have not included examples that deal with partitioned tables.-->

## Related documentation

- [`readCsv`](../data-import-export/CSV/readCsv.md)
- [`plot`](../plot/plot.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/plot/filters/Selectables.html#oneClick(io.deephaven.engine.table.PartitionedTable))
