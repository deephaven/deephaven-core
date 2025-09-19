---
title: mergeSorted
---

`mergeSorted` combines two or more tables into one sorted, aggregate table. This essentially stacks the tables one on top of the other and sorts the result. Null tables are ignored. `mergeSorted` is more efficient than using [`merge`](./merge.md) followed by [`sort`](../sort/sort.md).

## Syntax

```
mergeSorted(keyColumn, tables...)
```

## Parameters

<ParamTable>
<Param name="keyColumn" type="String">

The column by which to sort the merged table.

</Param>
<Param name="tables" type="Table...">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Each table must be already sorted.
- Null inputs are skipped.

</Param>
<Param name="tables" type="Collection<Table>">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Each table must be already sorted.
- Null inputs are skipped.

</Param>
</ParamTable>

## Returns

A new table with the source tables stacked one on top of the other and sorted by the specified column.

## Examples

In the following example, `source1` is stacked on top of `source2`, and the result is sorted based on the `Number` column.

```groovy order=source1,source2,result
source1 = newTable(col("Letter", "A", "D", "E"), col("Number", 3, 4, 7))
source2 = newTable(col("Letter", "B", "C", "D"), col("Number", 1, 2, 5))

result = mergeSorted("Letter", source1, source2)
```

In the following example, three tables are merged and sorted based on the `Number` column.

```groovy order=source1,source2,source3,result
source1 = newTable(col("Letter", "A", "C", "G"), col("Number", 1, 6, 9))
source2 = newTable(col("Letter", "B", "D", "G"), col("Number", 3, 5, 8))
source3 = newTable(col("Letter", "D", "E", "F"), col("Number", 2, 4, 7))

result = mergeSorted("Number", source1, source2, source3)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to horizontally stack tables](../../../how-to-guides/merge-tables.md)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [`sort`](../sort/sort.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#mergeSorted(java.lang.String,io.deephaven.engine.table.Table...))
