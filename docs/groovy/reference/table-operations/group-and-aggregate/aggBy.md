---
title: aggBy
---

`aggBy` applies a list of aggregations to table data.

> [!WARNING]
> Aggregation keys consume memory that persists for the lifetime of the worker, even after the keys are removed from the table. Avoid including unnecessary columns in grouping keys, especially if they contain a high number of unique values.

## Syntax

```
aggBy(aggregation)
aggBy(aggregation, groupByColumns...)
aggBy(aggregations)
aggBy(aggregations, preserveEmpty)
aggBy(aggregations, preserveEmpty, initialGroups, groupByColumns)
```

## Parameters

<ParamTable>
<Param name="aggregation" type="Aggregation">

The aggregation to apply. The following aggregations are available:

- [`AggAbsSum`](./AggAbsSum.md)
- [`AggApproxPct`](./AggApproxPct.md)
- [`AggAvg`](./AggAvg.md)
- [`AggCount`](./AggCount.md)
- [`AggCountDistinct`](./AggCountDistinct.md)
- [`AggCountWhere`](./AggCountWhere.md)
- [`AggDistinct`](./AggDistinct.md)
- [`AggFirst`](./AggFirst.md)
- [`AggFormula`](./AggFormula.md)
- [`AggGroup`](./AggGroup.md)
- [`AggLast`](./AggLast.md)
- [`AggMax`](./AggMax.md)
- [`AggMed`](./AggMed.md)
- [`AggMin`](./AggMin.md)
- [`AggPartition`](./AggPartition.md)
- [`AggPct`](./AggPct.md)
- [`AggSortedFirst`](./AggSortedFirst.md)
- [`AggSortedLast`](./AggSortedLast.md)
- [`AggStd`](./AggStd.md)
- [`AggSum`](./AggSum.md)
- [`AggUnique`](./AggUnique.md)
- [`AggVar`](./AggVar.md)
- [`AggWAvg`](./AggWAvg.md)
- [`AggWSum`](./AggWSum.md)

</Param>
<Param name="aggregations" type="Collection<? extends Aggregation>">

A list of aggregations to compute. The following aggregations are available:

- [`AggAbsSum`](./AggAbsSum.md)
- [`AggApproxPct`](./AggApproxPct.md)
- [`AggAvg`](./AggAvg.md)
- [`AggCount`](./AggCount.md)
- [`AggCountDistinct`](./AggCountDistinct.md)
- [`AggCountWhere`](./AggCountWhere.md)
- [`AggDistinct`](./AggDistinct.md)
- [`AggFirst`](./AggFirst.md)
- [`AggFormula`](./AggFormula.md)
- [`AggGroup`](./AggGroup.md)
- [`AggLast`](./AggLast.md)
- [`AggMax`](./AggMax.md)
- [`AggMed`](./AggMed.md)
- [`AggMin`](./AggMin.md)
- [`AggPartition`](./AggPartition.md)
- [`AggPct`](./AggPct.md)
- [`AggSortedFirst`](./AggSortedFirst.md)
- [`AggSortedLast`](./AggSortedLast.md)
- [`AggStd`](./AggStd.md)
- [`AggSum`](./AggSum.md)
- [`AggUnique`](./AggUnique.md)
- [`AggVar`](./AggVar.md)
- [`AggWAvg`](./AggWAvg.md)
- [`AggWSum`](./AggWSum.md)

</Param>
<Param name="groupByColumns" type="String...">

The columns to group by.

</Param>
<Param name="groupByColumns" type="Collection<? extends ColumnName>">

The columns to group by.

</Param>
<Param name="preserveEmpty" type="boolean">

Whether to keep result rows for groups that are initially empty or become empty as a result of updates. Each aggregation operator defines its own value for empty groups.

</Param>
<Param name="initialGroups" type="Table">

A table whose distinct combinations of values for the `groupByColumns` should be used to create an initial set of aggregation groups. All other columns are ignored.
This is useful in combination with `preserveEmpty == true` to ensure that particular groups appear in the result table, or with `preserveEmpty == false` to control the encounter order for a collection of groups and thus their relative order in the result.
Changes to `initialGroups` are not expected or handled; if initialGroups is a refreshing table, only its contents at instantiation time will be used.
If `initialGroups == null`, the result will be the same as if a table with no rows was supplied.

</Param>
</ParamTable>

> [!IMPORTANT]
> In Python queries, `as_list` is needed to convert the Python list of aggregations to a Java compatible list.

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

Aggregated table data based on the aggregation types specified in the `agg_list`.

## Examples

In this example, `AggFirst` returns the first `Y` value as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("Y")], "X")
```

In this example, `AggGroup` returns an array of values from the `Number` column (`Numbers`), and `AggMax` returns the maximum value from the `Number` column (`MaxNumber`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggGroup("Numbers = Number"),AggMax("MaxNumber = Number")], "X")
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2460 add code examples -->

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#aggBy(io.deephaven.api.agg.Aggregation))
