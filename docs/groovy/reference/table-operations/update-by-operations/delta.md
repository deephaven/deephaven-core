---
title: Delta
---

Creates a `Delta` [`UpdateByOperation`](./updateBy.md#parameters) for the supplied column names. The `Delta` operation produces values by computing the difference between the current value and the previous value. By default, null values in the current or previous cell produce null output. This behavior can be changed using [`DeltaControl`](./DeltaControl.md).

## Syntax

```
Delta(pairs...)
Delta(control, pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
<Param name="control" type="DeltaControl">

Defines how special cases should behave. See [`DeltaControl`](./DeltaControl.md)

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the `delta` of the `X` column, renaming the resultant column to `Delta_X`. No grouping columns are specified, so the `delta` is calculated for all rows.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([Delta("Delta_X = X")])
```

### One Delta column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the Delta is calculated on a per-letter basis.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)")

result = source.updateBy([Delta("Delta_X = X")], "Letter")
```

### Multiple Delta columns, multiple grouping columns

The following example builds on the previous by calculating the Delta of multiple columns with each [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

result = source.updateBy([Delta("Delta_X = X", "Delta_Y = Y")], "Letter", "Truth")
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating Delta of the X and Y columns using different `Delta` [UpdateByoperations](./updateBy.md#parameters).

```groovy order=source,result
source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(0, 25)")

DeltaX = Delta("DeltaX = X")
DeltaY = Delta("DeltaY = Y")

result = source.updateBy([DeltaX, DeltaY], "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#EmMax(double,java.lang.String...))
