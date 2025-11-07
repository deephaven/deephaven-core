---
title: meta
---

The `meta` method creates a new table containing the metadata about the source table's columns.

## Syntax

```
source.meta()
```

## Parameters

The `meta` method does not take any arguments.

## Returns

A table containing the metadata about the source table's columns.

## Example

The following example first creates a table of weather data for Miama over three dates in January, then [averages](../group-and-aggregate/avgBy.md) the high and low temperatures by day. Finally, the query creates a table of metadata, which shows the that the `Temp` column becomes a double column.

```groovy order=source,result,meta
source = newTable(
    stringCol("Day","Jan 1", "Jan 1", "Jan 2", "Jan 2", "Jan 3", "Jan 3"),
    intCol("Temp", 45, 62, 48, 63, 39, 59),
)

result = source.avgBy("Day")

meta = result.meta()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#meta())
