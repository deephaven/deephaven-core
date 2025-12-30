---
title: catPlotBy
---

The `catPlotBy` method creates multiple [category plots](./catPlot.md) per distinct grouping value specified in `byColumns`.

## Syntax

```
catPlotBy(seriesName, t, categories, y, byColumns)
catPlotBy(seriesName, sds, categories, y, byColumns)
```

## Parameters

<ParamTable>
<Param name="seriesName" type="Comparable">

The name you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table">

The table.

</Param>
<Param name="sds" type="SelectableDataSet">

Selectable data set (e.g., OneClick filterable table).

</Param>
<Param name="categories" type="String">

The column in `t` contains discrete data.

</Param>
<Param name="y" type="String">

The column in `t` that contains the Y variable data.

</Param>
<Param name="byColumns" type="list[String]">

Column(s) in `t` that holds the grouping data.

</Param>
</ParamTable>

## Returns

A category plot with multiple series.

## Examples

The following example creates two plots with `catPlotBy`. The two plots show the difference changing the `by` column makes.

```groovy order=null
source = newTable(
    stringCol('Department', 'HR', 'HR', 'HR', 'IT', 'IT', 'IT'),
    stringCol('EmployeeType', 'Junior', 'Mid-Level', 'Senior', 'Junior', 'Mid-Level', 'Senior'),
    intCol('Count', 5, 12, 3, 7, 6, 2)
)

employeeTypesByDepartment = catPlotBy('No. of employees per dept.', source, 'EmployeeType', 'Count', 'Department').show()
employeeDepartmentsByType = catPlotBy('Employee departments by type', source, 'Department', 'Count', 'EmployeeType').show()
```

![The above `employeeTypesByDepartment` category plot](../../assets/reference/catplotByDept.png)
![The above `employeeDepartmentsByType` category plot](../../assets/reference/catplotByType.png)

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create category plots](../../how-to-guides/plotting/api-plotting.md#category)
- [Arrays](../query-language/types/arrays.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/plot/Axes.html#catPlotBy(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String...))
