---
title: Access table metadata
sidebar_label: Metadata
---

This guide will show you how to use methods and attributes from Deephaven's `Table` class to access the metadata for your table.

A table's metadata provides basic information about its source data, such as the table type and size, whether the data is refreshing, and the data types of each column. You may want to confirm if a column is an `int` or a `double`, or check whether a column is a partitioning column or grouping column.

## `meta`

The [`meta`](../reference/table-operations/metadata/meta.md) method creates a new table that contains the table's metadata. Specifically, this table contains information about every column in the original table.

```groovy syntax
result = source.meta()
```

This can be useful when you want to confirm which columns in a table are partitioning or grouping, or verify the data type of a column.

Let's create a table of weather data for Miami, Florida.

```groovy test-set=2
miami = newTable(
    stringCol("Month", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
    intCol("Temp", 60, 62, 65, 68, 73, 76, 77, 77, 76, 74, 68, 63),
    doubleCol("Rain", 1.62, 2.25, 3.00, 3.14, 5.34, 9.67, 6.50, 8.88, 9.86, 6.33, 3.27, 2.04),
)
```

We can access the metadata as follows:

```groovy test-set=2
meta = miami.meta()
```

Obviously, this is more useful for a table we are unfamiliar with, but as you can see, the `meta` table provides information about the column and data types.

Now, let's create a table of weather data for Miami over three dates in January, then [averages](../reference/table-operations/group-and-aggregate/avgBy.md) the high and low temperatures by day.

```groovy test-set=3 order=miami,avgTemp
miami = newTable(
    stringCol("Day", "Jan 1", "Jan 1", "Jan 2", "Jan 2", "Jan 3", "Jan 3"),
        intCol("Temp", 45, 62, 48, 63, 39, 59),
)

avgTemp = miami.avgBy("Day")
```

Although the `Temp` column is originally created as an [int column](../reference/table-operations/create/intCol.md), the `Temp` column in the `avgBy` table becomes a double column. We can see this by hovering over the column header in the UI, and also by accessing the table's metadata.

```groovy test-set=3 order=meta
meta = avgTemp.meta()
```

## Table Metadata Methods

The following methods can be used to access specific elements of a table's metadata:

## `getAttribute`

Use the [`getAttribute`](../reference/table-operations/metadata/getAttribute.md) method to get the value of a specific [table attribute](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE).

```groovy test-set=1 order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)
println result.getAttribute("AddOnly")
```

## `getAttributes`

The [`getAttributes`](../reference/table-operations/metadata/getAttributes.md) method returns _all_ the attributes in the source table's `AttributeMap`. An optional `included` parameter allows you to specify which keys should be included in the result.

```groovy test-set=1 order=:log
import java.util.function.Predicate

// include all attributes
println result.getAttributes()

// get attribute keys
println result.getAttributeKeys()

// include only the "AddOnly" attribute
println result.getAttributes(Predicate.isEqual("AddOnly"))
```

## `getAttributeKeys`

[`getAttributeKeys`](../reference/table-operations/metadata/getAttributeKeys.md) returns an immutable set of all the source table's attributes that have values.

```groovy test-set=1 order=:log
println result.getAttributeKeys()
```

## `getDefinition`

Use the [`getDefinition`](../reference/table-operations/metadata/getDefinition.md) method to get the source table's `TableDefinition`.

```groovy test-set=1 order=:log
println result.getDefinition()
```

## `hasAttribute`

The [`hasAttribute`](../reference/table-operations/metadata/hasAttribute.md) method returns `True` if the source table has the specified attribute, and `False` otherwise.

```groovy test-set=1 order=:log
println result.hasAttribute("AddOnly")
println result.hasAttribute("NotAnAttribute")
```

## `hasColumns`

The [`hasColumns`](../reference/table-operations/metadata/hasColumns.md) method returns `True` if the source table has the specified columns, and `False` otherwise.

```groovy test-set=1 order=:log
println result.hasColumns("Doubles")
println result.hasColumns("Doubles", "NotAColumn")
```

## `isEmpty`

To see if a table is empty, use the [`isEmpty`](../reference/table-operations/metadata/isEmpty.md) method. This method returns `True` if the table is empty, or `False` if it is not.

```groovy test-set=1 order=:log
table = emptyTable(1)
println result.isEmpty()
println table.isEmpty()
```

## `isFlat`

The [`isFlat`](../reference/table-operations/metadata/isFlat.md) method returns `True` if the source table is flat, and `False` otherwise.

```groovy test-set=1 order=:log
println result.isFlat()
```

## `isFailed`

The [`isFailed`](../reference/table-operations/metadata/isFailed.md) method returns `True` if the source table is in a failed state and `False` otherwise.

```groovy test-set=1 order=:log
println result.isFailed()
```

## `isRefreshing`

The [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md) method returns `True` if the source table is refreshing, and `False` otherwise.

```groovy test-set=1 order=:log
println result.isRefreshing()
```

## `numColumns`

The [`numColumns`](../reference/table-operations/metadata/numColumns.md) method returns the number of columns in the source table.

```groovy test-set=1 order=:log
println result.numColumns()
```

## Related documentation

- [Create a new table](../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations for groups](./dedicated-aggregations.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`getAttribute`](../reference/table-operations/metadata/getAttribute.md)
- [`getAttributes`](../reference/table-operations/metadata/getAttributes.md)
- [`getAttributeKeys`](../reference/table-operations/metadata/getAttributeKeys.md)
- [`hasAttribute`](../reference/table-operations/metadata/hasAttribute.md)
- [`hasColumns`](../reference/table-operations/metadata/hasColumns.md)
- [`isEmpty`](../reference/table-operations/metadata/isEmpty.md)
- [`isFlat`](../reference/table-operations/metadata/isFlat.md)
- [`isFailed`](../reference/table-operations/metadata/isFailed.md)
- [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md)
- [`numColumns`](../reference/table-operations/metadata/numColumns.md)
