---
title: updateBy
---

`updateBy` creates a table with additional columns calculated from window-based aggregations of columns in the source table.

The aggregations are defined by the provided operations, which support incremental aggregations over the corresponding rows in the source table. The aggregations will apply position or time-based windowing and compute the results over the entire table or each row group as identified by the provided key columns.

## Syntax

```
result = source.updateBy(operation)
result = source.updateBy(operations)
result = source.updateBy(control, operations)
result = source.updateBy(operation, byColumns...)
result = source.updateBy(operations, byColumns...)
result = source.updateBy(operations, byColumns)
result = source.updateBy(control, operations, byColumns)
```

## Parameters

<ParamTable>
<Param name="operation" type="UpdateByOperation">

An `UpdateByOperation` that will produce a calculation from one or more given columns in a source table. The following `UpdateByOperations` are available:

- [`CumCountWhere`](./cum-count-where.md)
- [`CumMax`](./cum-max.md)
- [`CumMin`](./cum-min.md)
- [`CumProd`](./cum-prod.md)
- [`CumSum`](./cum-sum.md)
- [`Delta`](./delta.md)
- [`Ema`](./ema.md)
- [`Ems`](./ems.md)
- [`EmMin`](./em-min.md)
- [`EmMax`](./em-max.md)
- [`EmStd`](./em-std.md)
- [`Fill`](./fill.md)
- [`RollingAvg`](./rolling-avg.md)
- [`RollingCount`](./rolling-count.md)
- [`RollingCountWhere`](./rolling-count-where.md)
- [`RollingFormula`](./rolling-formula.md)
- [`RollingGroup`](./rolling-group.md)
- [`RollingMax`](./rolling-max.md)
- [`RollingMin`](./rolling-min.md)
- [`RollingProduct`](./rolling-product.md)
- [`RollingStd`](./rolling-std.md)
- [`RollingSum`](./rolling-sum.md)
- [`RollingWavg`](./rolling-wavg.md)

</Param>
<Param name="operations" type="Collection<UpdateByOperation>">

A collection of one or more `UpdateByOperations` that will produce calculation(s) from one or more given columns in a source table.

</Param>
<Param name="control" type="UpdateByControl">

An interface to control the behavior of an `updateBy` table operation.

</Param>
<Param name="byColumns" type="String...">

One or more key columns that group rows of the table.

</Param>
</ParamTable>

## Returns

A new table with rolling window operations applied the the specified column(s).

## Examples

In the following example, a `source` table is created. The `source` table contains two columns: `Letter` and `X`. An `updateBy` is applied to the `source` table, which calculates the cumulative sum of the `X` column. The `Letter` column is given as the `by` column. `Letter` is `A` when `X` is even, and `B` when odd. Thus, the `result` table contains a new column, `SumX`, which contains the cumulative sum of the `X` column, grouped by `Letter`.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy(CumSum("SumX = X"), "Letter")
```

The following example takes the same source data, but instead computes a row-based rolling sum using [`RollingSum`](./rolling-sum.md). The rolling sum is calculated given a window of two rows back, and two rows ahead. Thus, `SumX` has the windowed sum of a five-row window, where each value is at the center of the window. Rows at the beginning and end of the table don't have enough data above and below them, respectively, so their summed values are smaller.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy(RollingSum(3, 2, "SumX = X"), "Letter")
```

The following example builds on the previous examples by adding a second data column, `Y`, to the `source` table. The [`CumSum`](./cum-sum.md) `UpdateByOperation` is then given two columns, so that the cumulative sum of the `X` and `Y` columns are both calculated.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 10)")

result = source.updateBy(CumSum("SumX = X", "SumY = Y"), "Letter")
```

The following example modifies the previous example by perfoerming two separate `UpdateByoperations`. The first uses [`CumSum`](./cum-sum.md) on the `X` column like the previous example, but instead performs a tick-based rolling sum on the `Y` column with [`RollingSum`](./rolling-sum.md).

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 10)")

result = source.updateBy([CumSum("SumX = X"), RollingSum(2, 1, "RollingSumY = Y")], "Letter")
```

The following example builds on previous examples by adding a second key column, `Truth`, which contains boolean values. Thus, groups are defined by unique combinations of the `Letter` and `Truth` columns.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(0, 10)")

result = source.updateBy([CumSum("SumX = X"), RollingSum(2, 1, "RollingSumY = Y")], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateBy(java.util.Collection))
