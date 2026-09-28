---
title: Ems
---

`Ems` creates an EMS (exponential moving sum) for an [`updateBy`](./updateBy.md) table operation. The formula for an EMS is:

$a = e^{\frac{-1}{\tau}}$

$\epsilon_{n} = a * \epsilon_{n-1} + x$

Where:

- $\tau$ is the window size, an [input parameter](#parameters) to the method.
- $\epsilon$ is the EMS.
- $x$ is the current value.
- $n$ denotes the step. The current step is $n$, and the previous step is $n - 1$.

## Syntax

```
Ems(tickDecay, pairs...)
Ems(control, tickDecay, pairs...)
Ems(control, timestampColumn, timeDecay, pairs...)
Ems(control, timestampColumn, durationDecay, pairs...)
Ems(timestampColumn, timeDecay, pairs...)
Ems(timestampColumn, durationDecay, pairs...)
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

The following example calculates the tick-based and time-based EMS of the `X` column, renaming the resulting column to `EmsX`. The tick decay rate is set to 5 rows, and the time decay rate is set to 5 seconds. No grouping columns are specified, so the EMS is calculated for all rows.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy([Ems(5, "EmsTickX = X"), Ems("Timestamp", 5 * SECOND, "EmsTimeX = X")])
```

### One EMS column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EMA is calculated on a per-letter basis.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy([Ems(5, "EmsTickX = X"), Ems("Timestamp", 5 * SECOND, "EmsTimeX = X")], "Letter")
```

### Multiple EMS columns, multiple grouping columns

The following example builds on the previous by calculating the EMS of multiple columns with each [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(5, 10)")

result = source.updateBy([Ems(2, "EmsTickX = X", "EmsTickY = Y"), Ems("Timestamp", 3 * SECOND, "EmsTimeX = X", "EmsTimeY = Y")], "Letter", "Truth")
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the tick- and time-based EMS of the X and Y columns using different `EMS` `UpdateByOperations`. This allows each EMS to have its own decay rate. The decay rates are reflected in the renamed resultant columns.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(20).update("Timestamp = baseTime + i *  SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(5, 10)")

emsTickX = Ems(1, "EmsTickX_1row = X")
emsTickY = Ems(5, "EmsTickY_5rows = Y")
emsTimeX = Ems("Timestamp", 2 * SECOND, "EmsTimeX_2sec = X")
emsTimeY = Ems("Timestamp", 4 * SECOND, "EmsTimeY_4sec = Y")

result = source.updateBy([emsTickX, emsTickY, emsTimeX, emsTimeY], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Ema(double,java.lang.String...))
