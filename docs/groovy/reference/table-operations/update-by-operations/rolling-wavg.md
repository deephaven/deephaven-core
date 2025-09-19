---
title: RollingWAvg
---

`RollingWAvg` calculates a window-based rolling weighted average for an [`updateBy`](./updateBy.md) table operation. The rolling weighted average can be calculated using forward and/or backward windows.

## Syntax

```
RollingWAvg(revTicks, fwdTicks, weightCol, pairs...)
RollingWAvg(revTicks, weightCol, pairs...)
RollingWAvg(timestampCol, revTime, fwdTime, weightCol, pairs...)
RollingWAvg(timestampCol, revTime, weightCol, pairs...)
RollingWAvg(timestampCol, revDuration, fwdDuration, weightCol, pairs...)
RollingWAvg(timestampCol, revDuration, weightCol, pairs...)
```

## Parameters

<ParamTable>
<Param name="weightCol" type="String">

The column containing the weight values.

</Param>
<Param name="revTicks" type="long">

The look-behind window size in ticks (rows).

</Param>
<Param name="fwdTicks" type="long">

The look-forward window size in ticks (rows).

</Param>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
<Param name="timestampCol" type="String">

The name of the DateTime column.

</Param>
<Param name="revTime" type="long">

The look-behind window size in nanoseconds.

</Param>
<Param name="fwdTime" type="long">

The look-forward window size in nanoseconds.

</Param>
<Param name="revDuration" type="Duration">

The look-behind window size in Duration.

</Param>
<Param name="fwdDuration" type="Duration">

The look-forward window size in Duration.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using three row-based `RollingWAvg` operations. Each operation gives varying `revTicks` and `fwdTicks` values to show how they affect the output. The windows for each operation are as follows:

- `opBefore`: The window contains two rows. It starts two rows before the current row, and ends at the row before the current row.
- `opAfter`: The window contains three rows. It starts one row after the current row, and ends three rows after the current row.
- `opMiddle`: The window contains three rows. It starts one row before the current row, and ends one row ahead of the current row.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(2,10)")

opPrior = RollingWAvg(3, -1, "Y", "WindowPriorX = X")
opPosterior = RollingWAvg(-1, 3, "Y", "WindowPosteriorX = X")
opMiddle = RollingWAvg(2, 1, "Y", "WindowMiddleX = X")

result = source.updateBy([opPrior, opPosterior, opMiddle], "Letter")
```

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using three time-based `RollingWAvg` operations. Each operation gives varying `revTime` and `fwdTime` values to show how they affect the output. The windows for each operation are as follows:

- `opBefore`: The window starts five seconds before the current row, and ends one second before the current row.
- `opAfter`: The window starts one second after the current row, and ends five seconds after the current row.
- `opMiddle`: The window starts three seconds before the current row, and ends three seconds after the current row.

```groovy order=source,result
baseTime = parseInstant("2023-01-01T00:00:00 ET")

source = emptyTable(10).update("Timestamp = baseTime + i * SECOND", "Letter = (i % 2 == 0) ? `A` : `B`", "X = i * 2", "Y = randomInt(2,10)")

opBefore = RollingWAvg("Timestamp", 5 * SECOND, -1 * SECOND, "Y", "WindowBeforeX = X")
opAfter = RollingWAvg("Timestamp", -1 * SECOND, 5 * SECOND, "Y", "WindowAfterX = X")
opMiddle = RollingWAvg("Timestamp", 3 * SECOND, 3 * SECOND, "Y", "WindowMiddleX = X")

result = source.updateBy([opBefore, opAfter, opMiddle], "Letter")
```

## Related documentation

- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingWAvg(long,long,java.lang.String,java.lang.String...))
