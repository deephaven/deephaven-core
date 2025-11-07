---
title: catPlot
---

The `catPlot` method creates category histograms using data from Deephaven tables.

## Syntax

```
catPlot(seriesName, t, categories, y)
catPlot(seriesName, categories, y)
catPlot(seriesName, sds, categories, y)
catPlot(seriesName, categories, y)
```

## Parameters

<ParamTable>
<Param name="seriesName" type="String">

The name (as a [String](../query-language/types/strings.md)) you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table">

The table that holds the data to be plotted.

</Param>
<Param name="categories" type="String">

The name of the column from `t` or `sds` to be used for the categories.

</Param>
<Param name="categories" type="IndexableData<T>">

The indexable data to be used for the categories.

</Param>
<Param name="categories" type="List<T>">

The list of data to be used for the categories.

</Param>
<Param name="categories" type="double[]">

The array of data to be used for the categories.

</Param>
<Param name="categories" type="float[]">

The array of data to be used for the categories.

</Param>
<Param name="categories" type="int[]">

The array of data to be used for the categories.

</Param>
<Param name="categories" type="long[]">

The array of data to be used for the categories.

</Param>
<Param name="categories" type="<T>[]">

The array of data to be used for the categories.

</Param>
<Param name="sds" type="SelectableDataSet">

Selectable data set (e.g., OneClick filterable table).

</Param>
<Param name="y" type="String">

Y-values.

</Param>
<Param name="y" type="IndexableNumericData<T>">

Y-values.

</Param>
<Param name="y" type="double[]">

Y-values.

</Param>
<Param name="y" type="float[]">

Y-values.

</Param>
<Param name="y" type="int[]">

Y-values.

</Param>
<Param name="y" type="long[]">

Y-values.

</Param>
<Param name="y" type="short[]">

Y-values.

</Param>
<Param name="y" type="DateTime[]">

Y-values.

</Param>
<Param name="y" type="Date[]">

Y-values.

</Param>
<Param name="y" type="List<T>">

Y-values.

</Param>
<Param name="y" type="<T>[]">

Y-values.

</Param>
</ParamTable>

## Returns

A category plot.

## Examples

The following example plots data from a Deephaven table.

```groovy order=source,result default=result
source = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 1, 3, 5)
)

result = catPlot("Categories Plot", source, "Categories", "Values")
    .chartTitle("Categories And Values")
    .show()
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2459 Add code examples -->

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create category plots with the built-in API](../../how-to-guides/plotting/api-plotting.md#category)
