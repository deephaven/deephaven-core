---
title: EmMin
---

`EmMin` creates an EM Min (exponential moving minimum) for an [`updateBy`](./updateBy.md) table operation. The formula for an EM Min is:

$a = e^{\frac{-dt}{\tau}}$

$\epsilon_{n} = min((a * \epsilon_{n-1}), x)$

Where:

- $\tau$ is the window size, an [input parameter](#parameters) to the method.
- $\epsilon$ is the EM Min.
- $x$ is the current value.
- $n$ denotes the step. The current step is $n$, and the previous step is $n - 1$.

## Syntax

```
EmMin(tickDecay, pairs...)
EmMin(control, tickDecay, pairs...)
EmMin(control, timestampColumn, timeDecay, pairs...)
EmMin(control, timestampColumn, durationDecay, pairs...)
EmMin(timestampColumn, timeDecay, pairs...)
EmMin(timestampColumn, durationDecay, pairs...)
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

The following example calculates the tick-based and time-based EM Max of the `X` column, renaming the resultant column to `EmMin_X`. The tick decay rate is set to 5 rows, and the time decay rate is set to 5 seconds. No grouping columns are specified, so the EM Max is calculated for all rows.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([EmMin(5, "EmMin_Tick_X = X"), EmMin("Timestamp", 5 * SECOND, "EmMin_Time_X = X")])
```

### One EM Max column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EM Max is calculated on a per-letter basis.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([EmMin(5, "EmMin_Tick_X = X"), EmMin("Timestamp", 5 * SECOND, "EmMin_Time_X = X")], "Letter")
```

### Multiple EM Max columns, multiple grouping columns

The following example builds on the previous by calculating the EM Max of multiple columns with each [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

result = source.updateBy([EmMin(2, "EmMin_Tick_X = X", "EmMin_Tick_Y = Y"), EmMin("Timestamp", 3 * SECOND, "EmMin_Time_X = X", "EmMin_Time_Y = Y")], "Letter", "Truth")
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the tick- and time-based EM Max of the X and Y columns using different `EM Max` [`UpdateByOperations`](./updateBy.md#parameters). This allows each EM Max to have its own decay rate. The decay rates are reflected in the renamed resultant columns.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

emminTickX = EmMin(1, "EmMin_Tick_X_1row = X")
emminTickY = EmMin(5, "EmMin_Tick_Y_5rows = Y")
emminTimeX = EmMin("Timestamp", 2 * SECOND, "EmMin_Time_X_2sec = X")
emminTimeY = EmMin("Timestamp", 4 * SECOND, "EmMin_Time_Y_4sec = Y")

result = source.updateBy([emminTickX, emminTickY, emminTimeX, emminTimeY], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#EmMin(double,java.lang.String...))
