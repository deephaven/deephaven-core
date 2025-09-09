---
title: sort
---

The `sort` method creates a new `PartitionedTable` from the result of applying `sortColumns` to the underlying partitioned table.

> [!NOTE]
> `sortColumns` must not reference the "constituent" column.

## Syntax

```
sort(sortColumns)
```

## Parameters

<ParamTable>
<Param name="sortColumns" type="Collection<SortColumn">

The columns to sort by. Must not reference the "constituent" column.

</Param>

</ParamTable>

## Returns

A new `PartitionedTable` with the supplied `sortColumns` applied.

## Examples

```groovy order=result,source
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

sortColumns = [
    SortColumn.desc(ColumnName.of("X"))
]

result = partitionedTable.sort(sortColumns).table()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#sort(java.util.Collection))
