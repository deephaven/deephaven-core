---
title: piePlot
---

The `piePlot` method creates pie charts using data from Deephaven tables.

## Syntax

```
piePlot(seriesName, t, categories, y)
piePlot(seriesName, categories, y)
piePlot(seriesName, sds, categories, y)
```

## Parameters

<ParamTable>
<Param name="seriesName" type="String">

The name (as a [String](../query-language/types/strings.md)) you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table">

The table that holds the data to be plotted.

</Param>
<Param name="sds" type="SelectableDataSet">

A selectable data set (e.g., OneClick filterable table).

</Param>
<Param name="categories" type="String">

The name of the column in `t` or `sds` to be used for the categories.

</Param>
<Param name="categories" type="IndexableData">

Data.

</Param>
<Param name="categories" type="List<T>">

Data.

</Param>
<Param name="categories" type="<T>[]">

Data.

</Param>
<Param name="y" type="String">

Column in `t` or `sds` containing numeric data.

</Param>
<Param name="y" type="double[]">

Numeric data.

</Param>
<Param name="y" type="float[]">

Numeric data.

</Param>
<Param name="y" type="int[]">

Numeric data.

</Param>
<Param name="y" type="long[]">

Numeric data.

</Param>
<Param name="y" type="short[]">

Numeric data.

</Param>
<Param name="y" type="List<T>">

Numeric data.

</Param>
<Param name="y" type="<T>[]">

Numeric data.

</Param>
</ParamTable>

## Returns

A pie chart.

## Examples

The following example plots data from a Deephaven table.

```groovy order=totalInsurance,regionInsurance,pieChart default=pieChart
import static io.deephaven.csv.CsvTools.readCsv

totalInsurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

regionInsurance = totalInsurance.view("region", "expenses").sumBy("region")

pieChart = piePlot("Expense per region", regionInsurance, "region", "expenses")\
     .chartTitle("Expenses per region")\
     .show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create pie charts](../../how-to-guides/plotting/api-plotting.md#pie)
