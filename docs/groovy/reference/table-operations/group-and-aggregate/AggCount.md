---
title: AggCount
---

`AggCount` returns an aggregator that computes the number of elements within an aggregation group.

## Syntax

```
AggCount(resultColumn)
```

## Parameters

<ParamTable>
<Param name="resultColumn" type="String">

The name of the output column that will contain the number of elements in each group.

</Param>
</ParamTable>

## Returns

An aggregator that computes the number of elements within an aggregation group, for each group.

## Examples

In this example, `AggCount` adds a count column, `Number`, while `AggAvg` returns the average `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggCount("Number"),AggAvg("AvgNumber = Number")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggCount(java.lang.String))
