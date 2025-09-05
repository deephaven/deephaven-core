---
title: Ema
---

`Ema` creates an EMA (exponential moving average) for an [`updateBy`](./updateBy.md) table operation. The formula for an EMA is:

$a = e^{\frac{-1}{\tau}}$

$\epsilon_{n} = a\epsilon_{n - 1} + (1 - a)x$

Where:

- $\tau$ is the window size, an [input parameter](#parameters) to the method.
- $\epsilon$ is the EMA.
- $x$ is the current value.

## Syntax

```
Ema(tickDecay, pairs...)
Ema(control, tickDecay, pairs...)
Ema(control, timestampColumn, timeDecay, pairs...)
Ema(control, timestampColumn, durationDecay, pairs...)
Ema(timestampColumn, timeDecay, pairs...)
Ema(timestampColumn, durationDecay, pairs...)
```

## Parameters

<ParamTable>
<Param name="tickDecay" type="long">

The decay rate in ticks (rows).

</Param>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
<Param name="control" type="OperationControl">

Defines how special cases should behave. If not given, default [`OperationControl`](./OperationControl.md) settings are used.

</Param>
<Param name="timestampColumn" type="String">

The column in the source table to use for timestamps.

</Param>
<Param name="timeDecay" type="long">

The decay rate in nanoseconds.

</Param>
<Param name="durationDecay" type="Duration">

The decay rate in a [Duration](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) object.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the tick-based and time-based EMA of the `X` column, renaming the resultant column to `EmaX`. The tick decay rate is set to 5 rows, and the time decay rate is set to 5 seconds. No grouping columns are specified, so the EMA is calculated for all rows.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy([Ema(5, "EmaTickX = X"), Ema("Timestamp", 5 * SECOND, "EmaTimeX = X")])
```

### One EMA column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EMA is calculated on a per-letter basis.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy([Ema(5, "EmaTickX = X"), Ema("Timestamp", 5 * SECOND, "EmaTimeX = X")], "Letter")
```

### Multiple EMA columns, multiple grouping columns

The following example builds on the previous by calculating the EMA of multiple columns with each [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(5, 10)")

result = source.updateBy([Ema(2, "EmaTickX = X", "EmaTickY = Y"), Ema("Timestamp", 3 * SECOND, "EmaTimeX = X", "EmaTimeY = Y")], "Letter", "Truth")
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the tick- and time-based EMA of the X and Y columns using different `EMA` `UpdateByoperations`. This allows each EMA to have its own decay rate. The decay rates are reflected in the renamed resultant columns.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(5, 10)")

emaTickX = Ema(1, "EmaTickX_1row = X")
emaTickY = Ema(5, "EmaTickY_5rows = Y")
emaTimeX = Ema("Timestamp", 2 * SECOND, "EmaTimeX_2sec = X")
emaTimeY = Ema("Timestamp", 4 * SECOND, "EmaTimeY_4sec = Y")

result = source.updateBy([emaTickX, emaTickY, emaTimeX, emaTimeY], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to calculate an EMA](../../../how-to-guides/rolling-calculations.md#exponential-moving-statistics)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Ema(double,java.lang.String...))
