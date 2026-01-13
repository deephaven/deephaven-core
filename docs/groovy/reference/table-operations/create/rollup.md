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
- [`AggFormula`](../group-and-aggregate/AggFormula.md)
- [`AggGroup`](../group-and-aggregate/AggGroup.md)
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

## Formula Aggregations in Rollups

When a rollups includes a formula aggregation, care should be taken with the function being applied. On each tick, the formula is evaluated for each changed row in the output table. Because the aggregated rows include many source rows, the input vectors to a formula aggregation can be very large (at the root level, they are the entire source table). If the formula is not efficient with large input vectors, the performance of the rollup can be poor.

By default, the formula aggregation operates on a group of all the values as they appeared in the source table. In this example, the `Value` column contains the same vector that is used as input to the formula:

```groovy
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
simpleSum = source.rollup(List.of(AggGroup("Value"), AggFormula("Sum = sum(Value)")), "Key")
```

To calculate the sum for the root row, every row in the source table is read. The Deephaven engine provides no mechanism to provide detailed update information for a vector. Thus, every time the table ticks, the formula is completely re-evaluated.

### Formula Reaggregation

Formula reaggregation can be used to limit the size of input vectors while evaluating changes to a rollup. Each level of the rollup must have the same constituent types and names, which can make formulating your query more complicated.

```groovy
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
reaggregatedSum = source.updateView("Sum=(long)Value").rollup(List.of(AggFormula("Sum = sum(Sum)").asReaggregating()), "Key")
```

The results of `reaggregatedSum` are identical to `simpleSum`; but they are evaluated differently. In simpleSum, the source table is read twice: once to calculate the lowest-level sums, and a second time to calculate the top-level sum. In the reaggregated example, the source table is read once to calculate the lowest-level sums; and then the intermediate sums for each `Key` are read to calculate the top-level sum. If a row was added to `source` with the key `Delta`; then `simpleSum` would read that row, calculate a new sum for `Delta` and the top-level would read all eight rows of the table. The `reaggregatedSum` would similarly calculate the new sum for `Delta`, but the top-level would only read the intermediate sums for `Alpha`, `Bravo`, `Charlie`, and `Delta` instead of all eight source rows. As the number of states and the size of the input tables increase, the performance impact of evaluating a formula over all rows the table increases.

In the previous example, the `Sum` column evaluated the [`sum(IntVector)`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.IntVector)) function at every level of the rollup and produced a `long`. If the original table with an `int` column was used, then the lowest-level rollup would provide an `IntVector` as input to the `sum` and the next-level would provide `LongVector`. Similarly, the source table had a column named `Value`; whereas the aggregation produces a result named `Sum`. To address both these issues, before passing `source` to rollup, we called `updateView` to cast the `Value` column to `long` as `Sum`. If we ran the same example without the cast:

```groovy syntax
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
reaggregatedSum = source.rollup(List.of(AggFormula("Value = sum(Value)").asReaggregating()), "Key")
```

We instead get an Exception message indicating that the formula cannot be applied properly:

```text
java.lang.ClassCastException: class io.deephaven.engine.table.vectors.LongVectorColumnWrapper cannot be cast to class io.deephaven.vector.IntVector (io.deephaven.engine.table.vectors.LongVectorColumnWrapper and io.deephaven.vector.IntVector are in unnamed module of loader 'app')
```

### Formula Depth

Formula aggregations may include the constant `__FORMULA_DEPTH__` column, which is the depth of the formula aggregation in the rollup tree. The root node of the rollup has a depth of 0, the next level is 1, and so on. This can be used to implement distinct aggregations at each level of the rollup. For example:

```groovy
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
firstThenSum = source.rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)")), "Key")
```

In this case, for each value of `Key`, the aggregation returns the first value. For the root level, the aggregation returns the sum of all values. When combined with a reaggregating formula, even more interesting semantics are possible. For example, rather than summing all of the values; we can sum the values from the prior level:

```groovy
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
firstThenSum = source.updateView("Value=(long)Value").rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)").asReaggregating()), "Key")
```

Another simple example of reaggration is a capped sum. In this example, the sums below the root level are capped at 40:

```groovy
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 20, 15, 20, 15, 25, 35))
cappedSum = source.updateView("Value=(long)Value").rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : min(sum(Value), 40)").asReaggregating()), "Key")
```

## Related documentation

- [`AggAvg`](../group-and-aggregate/AggAvg.md)
- [`AggSum`](../group-and-aggregate/AggSum.md)
- [`emptyTable`](./emptyTable.md)
- [`join`](../join/join.md)
- [`timeTable`](./timeTable.md)
- [`treeTable`](./tree.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
