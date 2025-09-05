---
title: rollup
---

The deephaven `rollup` method creates a rollup table from a source table given zero or more aggregations and zero or more grouping columns to create a hierarchy from.

## Syntax

```
source.rollup(aggregations)
source.rollup(aggregations, includeConstituents)
source.rollup(aggregations, groupByColumns...)
source.rollup(aggregations, includeConstituents, groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="aggregations" type="Collection<? extends Aggregation>">

One or more aggregations.

The following aggregations are supported:

- [`AggAbsSum`](../group-and-aggregate/AggAbsSum.md)
- [`AggAvg`](../group-and-aggregate/AggAvg.md)
- [`AggCount`](../group-and-aggregate/AggCount.md)
- [`AggCountDistinct`](../group-and-aggregate/AggCountDistinct.md)
- [`AggCountWhere`](../group-and-aggregate/AggCountWhere.md)
- [`AggFirst`](../group-and-aggregate/AggFirst.md)
- [`AggLast`](../group-and-aggregate/AggLast.md)
- [`AggMax`](../group-and-aggregate/AggMax.md)
- [`AggMin`](../group-and-aggregate/AggMin.md)
- [`AggSortedFirst`](../group-and-aggregate/AggSortedFirst.md)
- [`AggSortedLast`](../group-and-aggregate/AggSortedLast.md)
- [`AggStd`](../group-and-aggregate/AggStd.md)
- [`AggSum`](../group-and-aggregate/AggSum.md)
- [`AggUnique`](../group-and-aggregate/AggUnique.md)
- [`AggVar`](../group-and-aggregate/AggVar.md)
- [`AggWAvg`](../group-and-aggregate/AggWAvg.md)
- [`AggWSum`](../group-and-aggregate/AggWSum.md)

</Param>
<Param name="includeConstituents" optional type="boolean">

Whether or not to include constituent rows at the leaf level. The default value is `False`.

</Param>
<Param name="groupByColumns" type="String...">

One or more columns to group on and create hierarchy from.

</Param>
</ParamTable>

## Methods

### Instance

- `getAggregations` - Get the aggregations performed in the `rollup` operation.
- `getGroupByColumns` - Get the `groupByColumns` used for the `rollup` operation.
- `getLeafNodeType` - Get the [`NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html) at the leaf level.
- `getNodeDefinition(nodeType)` - Get the [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) that should be exposed to node table consumers.
- `includesConstituents` - Returns a boolean indicating whether or not the constituent rows at the lowest level are included.
- `makeNodeOperationsRecorder(nodeType)` - Get a [`recorder`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) for per-node operations to apply during snapshots of the requested [`NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html).
- `translateAggregatedNodeOperationsForConstituentNodes(aggregatedNodeOperationsToTranslate)` - Translate node operations for aggregated nodes to the closest equivalent for a constituent node.
- `withFilter(filter)` - Create a new rollup table that will apply a filter to the Group By or Constituent columns of the rollup table.
- `withNodeOperations(nodeOperations...)` - Create a new rollup table that will apply the [`recorded`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) operations to nodes when gathering snapshots.
- `withUpdateView(columns...)` - Create a new rollup table that applies a set of `updateView` operations to the `groupByColumns` of the rollup table.
- `withNodeOperations(nodeOperations...)` - Create a new rollup table that applies the [`recorded`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) operations to nodes when gathering snapshots.

## Returns

A rollup table.

## Examples

The following example creates a rollup table from a source table of insurance expense data. The aggregated average of the `bmi` and `expenses` columns are calculated, then the table is rolled up by the `region` and `age` column.

```groovy order=insurance,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.spec.AggSpec
import io.deephaven.api.agg.Aggregation

insurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

aggList = [Aggregation.of(AggSpec.avg(), "bmi", "expenses")]

insuranceRollup = insurance.rollup(aggList, false, "region", "age")
```

Similar to the previous example, this example creates a rollup table from a source table of insurance expense data. However, this time we are filtering on the source table before applying the rollup using `withFilter`. Both group and constituent columns can be used in the filter, while aggregation columns cannot.

```groovy order=insurance,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.spec.AggSpec
import io.deephaven.api.agg.Aggregation
import io.deephaven.api.filter.Filter

insurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

aggList = [Aggregation.of(AggSpec.avg(), "bmi", "expenses")]
filterList = List.of("age > 30")

insuranceRollup = insurance.rollup(aggList, false, "region", "age")
    .withFilter(Filter.from(filterList))
```

The following example creates a rollup table from real-time source data. The source data updates 10,000 times per second. The `result` rollup table can be expanded by the `N` column to show unique values of `M` for each `N`. The aggregated average and sum are calculated for the `Value` and `Weight`, respectively.

```groovy ticking-table order=null
import io.deephaven.api.agg.spec.AggSpec
import io.deephaven.api.agg.Aggregation

aggList = [Aggregation.of(AggSpec.avg(), "Value"), Aggregation.of(AggSpec.sum(), "Weight")]

rows = emptyTable(1_000_000).updateView("Group = i", "N = i % 347", "M = i % 29")
changes = timeTable("PT00:00:00.0001").view("Group = i % 1_000_000", "LastModified = Timestamp", "Value = (i * Math.sin(i)) % 6977", "Weight = (i * Math.sin(i)) % 7151").lastBy("Group")

source = rows.join(changes, "Group")

result = source.rollup(aggList, false, "N", "M")
```

![The above `result` rollup table](../../../assets/how-to/rollup-table-realtime.gif)

## Related documentation

- [`AggAvg`](../group-and-aggregate/AggAvg.md)
- [`AggSum`](../group-and-aggregate/AggSum.md)
- [`emptyTable`](./emptyTable.md)
- [`join`](../join/join.md)
- [`timeTable`](./timeTable.md)
- [`treeTable`](./tree.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
