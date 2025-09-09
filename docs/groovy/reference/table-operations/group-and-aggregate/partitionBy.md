---
title: partitionBy
---

`partitionBy` divides a single table into subtables.

## Syntax

```
table.partitionBy(dropKeys, keyColumnNames...)
table.partitionBy(keyColumnNames...)
```

## Parameters

<ParamTable>
<Param name="keyColumnNames" type="String...">

The column(s) by which to group data.

</Param>
<Param name="dropKeys" type="boolean">

Whether to drop key columns in the output constituent tables.

</Param>
</ParamTable>

## Returns

A `PartitionedTable` containing a subtable for each group.

## Examples

```groovy order=source,result1,result2,result3
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.partitionBy("X")

result1 = result.constituentFor("A")
result2 = result.constituentFor("B")
result3 = result.constituentFor("C")
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2460 add code examples -->

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [How to partition a table into subtables](../../../how-to-guides/partitioned-tables.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#partitionBy(java.lang.String...))
