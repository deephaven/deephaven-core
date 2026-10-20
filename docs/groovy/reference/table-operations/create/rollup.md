---
title: rollup
---

The Deephaven `rollup` method creates a [rollup table](../../../how-to-guides/rollup-table.md) from a source table given zero or more [aggregations](../group-and-aggregate/aggBy.md) and zero or more grouping columns to create a hierarchy from.

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

A collection of aggregations. The collection can be empty, in which case the rollup builds the hierarchy without computing any aggregated values.

The following aggregations are supported:

- [`AggAbsSum`](../group-and-aggregate/AggAbsSum.md)
- [`AggAvg`](../group-and-aggregate/AggAvg.md)
- [`AggCount`](../group-and-aggregate/AggCount.md)
- [`AggCountDistinct`](../group-and-aggregate/AggCountDistinct.md)
- [`AggCountWhere`](../group-and-aggregate/AggCountWhere.md)
- [`AggDistinct`](../group-and-aggregate/AggDistinct.md)
- [`AggFirst`](../group-and-aggregate/AggFirst.md)
- [`AggFormula`](../group-and-aggregate/AggFormula.md): supported without the `paramToken` argument. The form with a `paramToken` is not supported.
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

A rollup of a [blink table](../../../conceptual/table-types.md#specialization-3-blink) does not support `AggFirst`, `AggLast`, `AggSortedFirst`, `AggSortedLast`, `AggGroup`, or `AggFormula`.

</Param>
<Param name="includeConstituents" optional type="boolean">

Whether or not to include constituent rows at the leaf level. The default value is `false`. A [blink table](../../../conceptual/table-types.md#specialization-3-blink) source does not support `true`.

</Param>
<Param name="groupByColumns" type="String...">

Zero or more columns to group on and create a hierarchy from. With none, the rollup aggregates all rows into a single root node.

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
- [`withFilter(filter)`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withFilter(io.deephaven.api.filter.Filter)) - Create a new rollup table that applies a filter to the Group By or Constituent columns of the rollup table. If every filter uses only group-by columns, Deephaven filters the lowest aggregated level and reaggregates. Otherwise, it applies the filters to the source table and rebuilds the rollup. Filters may use group-by and constituent columns but may not use columns that are inputs to an aggregation, and may not use column arrays.
- `withNodeOperations(nodeOperations...)` - Create a new rollup table that applies the [`recorded`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeOperationsRecorder.html) operations to nodes when gathering snapshots.
- `withUpdateView(columns...)` - Create a new rollup table with additional columns, computed by [`updateView`](../select/update-view.md), at the aggregated node level.

## Returns

A rollup table.

## Examples

The following example creates a rollup table from a source table of insurance expense data. The aggregated average of the `bmi` and `expenses` columns is calculated, then the table is rolled up by the `region` and `age` columns.

```groovy order=insurance,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.spec.AggSpec
import io.deephaven.api.agg.Aggregation

insurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

aggList = [Aggregation.of(AggSpec.avg(), "bmi", "expenses")]

insuranceRollup = insurance.rollup(aggList, false, "region", "age")
```

Similar to the previous example, this example creates a rollup table from a source table of insurance expense data and then filters it with [`withFilter`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html#withFilter(io.deephaven.api.filter.Filter)). The filter uses only the group-by column `age`, so Deephaven applies it to the lowest aggregated level and reaggregates. A filter can use group-by and constituent columns, but not columns that are inputs to an aggregation, and not column arrays. `withFilter` takes a single `Filter`, so `Filter.and` combines the filters from `Filter.from`.

```groovy order=insurance,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.spec.AggSpec
import io.deephaven.api.agg.Aggregation
import io.deephaven.api.filter.Filter

insurance = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv")

aggList = [Aggregation.of(AggSpec.avg(), "bmi", "expenses")]
filterList = List.of("age > 30")

insuranceRollup = insurance.rollup(aggList, false, "region", "age")
    .withFilter(Filter.and(Filter.from(filterList)))
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

## Use formula aggregations in rollups

A [formula aggregation](../group-and-aggregate/AggFormula.md) evaluates its formula for every changed row of the output table on each tick. Its input [vectors](../../../conceptual/arrays-vs-vectors.md) can be large: at the root level, they hold every row of the source table. A formula that is slow on large vectors slows the rollup.

By default, the formula aggregation operates on a group of all the values as they appeared in the source table. In this example, the `Value` column contains the same vector that is used as input to the formula:

```groovy order=simpleSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
simpleSum = source.rollup(List.of(AggGroup("Value"), AggFormula("Sum = sum(Value)")), "Key")
```

The sum for the root row reads every row of the source table. The engine reports row-level changes (added, removed, modified, and shifted rows), but not changes inside a vector, because a vector sits within a single row. When the table ticks, the engine therefore re-evaluates the whole formula.

### Reaggregate formulas

Formula reaggregation limits the size of input vectors while the engine evaluates changes to a rollup. The formula must apply at every level of the rollup and return the same type at each level.

```groovy order=reaggregatedSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
reaggregatedSum = source.updateView("Sum=Value").rollup(List.of(AggFormula("Sum = sum(Sum)").asReaggregating()), "Key")
```

`reaggregatedSum` and `simpleSum` produce the same results but operate differently. `simpleSum` reads the source table twice: first to calculate individual sums and then to compute the overall total. In contrast, `reaggregatedSum` reads the source table only once to gather the individual sums and then uses those intermediate sums to get the total.

If a new row with the key `Delta` is added to the source, `simpleSum` reads all eight rows again to recalculate the sums. `reaggregatedSum` recalculates only the sum for `Delta`, then reads the intermediate sums for `Alpha`, `Bravo`, `Charlie`, and `Delta`, not all rows. As the number of keys and the size of the data grow, this difference can significantly affect performance.

In the previous example, the `Sum` column is the output of the [`sum(IntVector)`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.IntVector)) function, which returns a `long`. At the lowest level of the rollup, `sum` receives an [`IntVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html) from the `int` source column. At each level above, it receives a [`LongVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html) of the results below.

At the lowest level, a formula reads source columns. At every level above, a reaggregating formula can read only the previous level's result columns. The formula must therefore read `Sum`, the column it also writes. The [`updateView`](../select/update-view.md) call adds a `Sum` column that copies `Value` for this purpose.

Without the [`updateView`](../select/update-view.md) call, the formula reads `Value`:

```groovy syntax
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
reaggregatedSum = source.rollup(List.of(AggFormula("Sum = sum(Value)").asReaggregating()), "Key")
```

This raises an exception, because the `Value` column does not exist at the levels above the lowest level of the rollup:

```text
Error running script: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: sum(Value)
...
Full expression           : sum(Value)
Expression having trouble :
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class Value
```

### Use formula depth and keys

Formula aggregations may include the constant `__FORMULA_DEPTH__` or `__FORMULA_KEYS__` columns. The `__FORMULA_DEPTH__` column is the depth of the formula aggregation in the rollup tree. The root node of the rollup has a depth of 0, the next level is 1, and so on. The `__FORMULA_KEYS__` column is an [`ObjectVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html) of `String` containing the names of the group-by columns in effect at the current level of the rollup: empty at the root, `["Key"]` at depth 1, and `["Key", "Key2"]` at depth 2 in the examples below. The following formulas demonstrate the values of depth and keys:

```groovy order=depthAndKeys,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Charlie", "Charlie", "Charlie"),
        stringCol("Key2", "Apple", "Banana", "Banana", "Coconut", "Coconut", "Coconut", "Dragonfruit"),
        intCol("Value", 10, 20, 15, 20, 15, 30, 35))
depthAndKeys = source.rollup(List.of(AggFormula("Depth = __FORMULA_DEPTH__"), AggFormula("Keys = __FORMULA_KEYS__")), "Key", "Key2")
```

At the root, `Depth` is 0 and `Keys` is empty. At each node for a `Key` value, `Depth` is 1 and `Keys` is `["Key"]`. At each node for a `Key2` value within a `Key`, `Depth` is 2 and `Keys` is `["Key", "Key2"]`.

These variables can implement distinct aggregations at each level of the rollup. For example:

```groovy order=firstThenSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
firstThenSum = source.rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)")), "Key")
```

In this case, for each value of `Key`, the aggregation returns the first value. For the root level, the aggregation returns the sum of all values. A reaggregating formula adds further semantics. For example, rather than summing all of the values, the formula can sum the values from the prior level:

```groovy order=firstThenSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 10, 10, 20, 20, 30, 30))
firstThenSum = source.rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)").asReaggregating()), "Key")
```

Another example of reaggregation is a capped sum, in which the sums below the root level are capped at 40:

```groovy order=cappedSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"),
        intCol("Value", 10, 20, 15, 20, 15, 25, 35))
cappedSum = source.rollup(List.of(AggFormula("Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : min(sum(Value), 40)").asReaggregating()), "Key")
```

Alpha sums to 45 and is capped at 40, Bravo sums to 35, Charlie sums to 60 and is capped at 40, and the root row sums the three capped values: 115.

The following capped sum uses `__FORMULA_KEYS__` instead of `__FORMULA_DEPTH__` to cap the sums at the `Key` column. In this case, `__FORMULA_DEPTH__ == 1` would be equivalent:

```groovy order=cappedSum,source
source = newTable(
        stringCol("Key", "Alpha", "Alpha", "Alpha", "Bravo", "Charlie", "Charlie", "Charlie"),
        stringCol("Key2", "Apple", "Banana", "Banana", "Coconut", "Coconut", "Coconut", "Dragonfruit"),
        intCol("Value", 10, 20, 15, 20, 15, 30, 35))
cappedSum = source.rollup(List.of(AggFormula("Value = __FORMULA_KEYS__.get(__FORMULA_KEYS__.size() - 1) == `Key` ?  min(sum(Value), 40) : sum(Value)").asReaggregating()), "Key", "Key2")
```

## Related documentation

- [Rollup table](../../../how-to-guides/rollup-table.md)
- [`AggAvg`](../group-and-aggregate/AggAvg.md)
- [`AggSum`](../group-and-aggregate/AggSum.md)
- [`AggFormula`](../group-and-aggregate/AggFormula.md)
- [`AggGroup`](../group-and-aggregate/AggGroup.md)
- [`updateView`](../select/update-view.md)
- [`newTable`](./newTable.md)
- [`emptyTable`](./emptyTable.md)
- [`join`](../join/join.md)
- [`timeTable`](./timeTable.md)
- [`tree`](./tree.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
