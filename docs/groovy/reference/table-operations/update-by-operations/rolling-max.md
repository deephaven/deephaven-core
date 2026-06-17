---
title: RollingMax
---

`RollingMax` calculates a window-based rolling maximum in an [`updateBy`](./updateBy.md) table operation. The rolling maximum can be calculated using forward and/or backward windows.

## Syntax

```
RollingMax(revTicks, fwdTicks, pairs)
RollingMax(revTicks, pairs)
RollingMax(timestampCol, revTime, fwdTime, pairs)
RollingMax(timestampCol, revTime, pairs)
RollingMax(timestampCol, revDuration, pairs)
RollingMax(timestampCol, revDuration, fwdDuration, pairs)
```

## Parameters

<ParamTable>
<Param name="revTicks" type="long">

The look-behind window size in ticks (rows). If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used.

</Param>
<Param name="fwdTicks" type="long">

The look-forward window size in ticks (rows). If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
<Param name="timestampCol" type="String">

The name of the timestamp column.

</Param>
<Param name="revDuration" type="Duration">

The look-behind window size in Duration.

</Param>
<Param name="fwdDuration" type="Duration">

The look-forward window size in Duration.

</Param>
<Param name="revTime" type="long">

The look-behind window size in nanoseconds.

</Param>
<Param name="fwdTime" type="long">

The look-forward window size in nanoseconds.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using four row-based `RollingMax` operations. Each of the last three operations uses different `revTicks` and `fwdTicks` values to show how they affect the output.

```groovy order=source,result
rng = new Random()

source = emptyTable(10).update("X = rng.nextInt(25)")

opRollMax = RollingMax(3, "RollingMaximum = X")
opPrior = RollingMax(3, -1, "WindowPrior = X")
opPosterior = RollingMax(-1, 3, "WindowPosterior = X")
opMiddle = RollingMax(1, 1, "WindowMiddle = X")

result = source.updateBy([opRollMax, opPrior, opPosterior, opMiddle])
```

## Related documentation

- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingMax(long,long,java.lang.String...))
