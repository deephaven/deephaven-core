---
title: AggPartition
---

`AggPartition` returns an aggregator that creates a partition (subtable) for an aggregation group.

## Syntax

```
AggPartition(resultColumn)
AggPartition(resultColumn, includeGroupByColumns)
```

## Parameters

<ParamTable>
<Param name="resultColumn" type="String">

The column to create partitions from.

</Param>
<Param name="includeGroupByColumns" type="boolean">

Whether to include the group by columns in the result; the default value is `true`.

</Param>
</ParamTable>

## Returns

An aggregator that computes a partition aggregation.

## Examples

In this example, `AggPartition` returns a partition of the `X` column, as grouped by `Letter`. The `result` table contains two rows: one for each letter. The subtables can be extracted from `result`.

```groovy order=source,result,resultA,resultB
import static io.deephaven.api.agg.Aggregation.AggPartition

source = emptyTable(20).update("X = i", "Letter = (X % 2 == 0) ? `A` : `B`")

result = source.aggBy([AggPartition("X")], "Letter")

columnSource = result.getColumnSource("X")

resultA = columnSource.get(0)
resultB = columnSource.get(1)
```

## Related documentation

- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`partitionBy`](./partitionBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPartition(java.lang.String))
