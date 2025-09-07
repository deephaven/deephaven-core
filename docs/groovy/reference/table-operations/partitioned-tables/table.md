---
title: table
---

The `table` method retuns the "raw" partitioned table underlying the source `PartitionedTable`.

The raw table can be converted back into a partitioned table using `PartitionedTableFactory.of(Table)` or `PartitionedTableFactory.of(Table, Collection, boolean, String, TableDefinition, boolean)`.

## Syntax

```
table()
```

## Parameters

This function does not take any parameters.

## Returns

The "raw" partitioned table underlying the source `PartitionedTable`.

## Examples

```groovy order=result,source
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

result = partitionedTable.table()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#table())
