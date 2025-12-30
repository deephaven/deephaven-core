---
title: EmStd
---

`EmStd` creates an EM Std (exponential moving standard deviation) for an [`updateBy`](./updateBy.md) table operation. The formula for an EM Std is:

$a = e^{\frac{-dt}{\tau}}$

$variance_{n} = a * (variance_{n-1} + (1 - a) * (x - \epsilon_{n-1})^2)$

$\epsilon = a * \epsilon_{n-1} + x$

$std = \sqrt{variance}$

Where:

- $\tau$ is the window size, an [input parameter](#parameters) to the method.
- $\epsilon$ is the EM Std.
- $x$ is the current value.
- $n$ denotes the step. The current step is $n$, and the previous step is $n - 1$.

## Syntax

```
EmStd(tickDecay, pairs...)
EmStd(control, tickDecay, pairs...)
EmStd(control, timestampColumn, timeDecay, pairs...)
EmStd(control, timestampColumn, durationDecay, pairs...)
EmStd(timestampColumn, timeDecay, pairs...)
EmStd(timestampColumn, durationDecay, pairs...)
```

## Parameters

<ParamTable>
<Param name="tickDecay" type="double">

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

The decay rate in a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) object.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the tick-based and time-based EM Std of the `X` column, renaming the resultant column to `EmStd_X`. The tick decay rate is set to 5 rows, and the time decay rate is set to 5 seconds. No grouping columns are specified, so the EM Std is calculated for all rows.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([EmStd(5, "EmStd_Tick_X = X"), EmStd("Timestamp", 5 * SECOND, "EmStd_Time_X = X")])
```

### One EM Std column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EM Std is calculated on a per-letter basis.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([EmStd(5, "EmStd_Tick_X = X"), EmStd("Timestamp", 5 * SECOND, "EmStd_Time_X = X")], "Letter")
```

### Multiple EM Max columns, multiple grouping columns

The following example builds on the previous by calculating the EM Std of multiple columns with each [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

result = source.updateBy([EmStd(2, "EmStd_Tick_X = X", "EmStd_Tick_Y = Y"), EmStd("Timestamp", 3 * SECOND, "EmStd_Time_X = X", "EmStd_Time_Y = Y")], "Letter", "Truth")
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the tick- and time-based EM Std of the X and Y columns using different `EM Std` [`UpdateByOperations`](./updateBy.md#parameters). This allows each EM Std to have its own decay rate. The decay rates are reflected in the renamed resultant columns.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

emstdTickX = EmStd(1, "EmStd_Tick_X_1row = X")
emstdTickY = EmStd(5, "EmStd_Tick_Y_5rows = Y")
emstdTimeX = EmStd("Timestamp", 2 * SECOND, "EmStd_Time_X_2sec = X")
emstdTimeY = EmStd("Timestamp", 4 * SECOND, "EmStd_Time_Y_4sec = Y")

result = source.updateBy([emstdTickX, emstdTickY, emstdTimeX, emstdTimeY], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#EmStd(double,java.lang.String...))
