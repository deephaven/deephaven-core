---
title: merge
slug: ./merge
---

`merge` combines two or more tables into one aggregate table. This essentially appends the tables one on top of the other. Null tables are ignored.

## Syntax

```
merge(tables...)
merge(theList)
```

## Parameters

<ParamTable>
<Param name="tables" type="Table...">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Null inputs are skipped.

</Param>
<Param name="tables" type="Collection<Table>">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Null inputs are skipped.

</Param>
<Param name="theList" type="List<Table>">

Source tables to be merged.

- The tables to be merged must include the same columns of the same type.
- Null inputs are skipped.

</Param>
</ParamTable>

## Returns

A new table with the source tables stacked one on top of the other. The resulting table's rows will maintain the same order as the source tables. If the source tables tick, rows will be inserted within the merged table where they appear in the source (rather than at the end of the merged table).

## Examples

In the following example, `source1` is stacked on top of `source2`.

```groovy order=source1,source2,result
source1 = newTable(col("Letter", "A", "B", "D"), col("Number", 1, 2, 3))
source2 = newTable(col("Letter", "C", "D", "E"), col("Number", 14, 15, 16))

result = merge(source1, source2)
```

In the following example, three tables are merged.

```groovy order=source1,source2,source3,result
source1 = newTable(col("Letter", "A", "B", "D"), col("Number", 1, 2, 3))
source2 = newTable(col("Letter", "C", "D", "E"), col("Number", 14, 15, 16))
source3 = newTable(col("Letter", "E", "F", "A"), col("Number", 22, 25, 27))

result = merge(source1, source2, source3)
```

In the following example, three tables are merged using an array of tables.

```groovy order=source1,source2,source3,result
source1 = newTable(col("Letter", "A", "B", "D"), col("Number", 1, 2, 3))
source2 = newTable(col("Letter", "C", "D", "E"), col("Number", 14, 15, 16))
source3 = newTable(col("Letter", "E", "F", "A"), col("Number", 22, 25, 27))

tableArray = [source1, source2, source3]

result = merge(tableArray)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to horizontally stack tables](../../../how-to-guides/merge-tables.md)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#merge(java.util.Collection))
