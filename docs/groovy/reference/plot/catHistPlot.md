---
title: catHistPlot
---

The `catHistPlot` method creates category histograms using data from Deephaven tables or [arrays](../query-language/types/arrays.md).

## Syntax

```
catHistPlot(seriesName, categories)
catHistPlot(seriesName, t, categories)
catHistPlot(seriesName, sds, categories)
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

The SelectableDataSet that holds the data to be plotted.

</Param>
<Param name="categories" type="double[]">

The array containing the discrete values.

</Param>
<Param name="categories" type="float[]">

The array containing the discrete values.

</Param>
<Param name="categories" type="int[]">

The array containing the discrete values.

</Param>
<Param name="categories" type="long[]">

The array containing the discrete values.

</Param>
<Param name="categories" type="String">

The name of a column in Table `t`. This argument cannot be used without a Table or a SelectableDataSet.

</Param>
<Param name="categories" type="List<T>">

The list of objects containing the discrete values.

</Param>
</ParamTable>

## Returns

A category histogram plot.

## Examples

The following example plots data from a Deephaven table.

```groovy order=source,result default=result
source = newTable(
    intCol("Keys", 3, 2, 2, 1, 1, 1)
)

result = catHistPlot("Keys Count", source, "Keys")
    .chartTitle("Count Of Each Key")
    .show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create category histograms with the built-in API](../../how-to-guides/plotting/api-plotting.md#category-histogram)
- [Arrays](../query-language/types/arrays.md)
