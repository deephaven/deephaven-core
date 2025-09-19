---
title: CumMin
---

`CumMin` calculates a cumulative minimum in an [`updateBy`](./updateBy.md) table operation.

## Syntax

```
CumMin(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative minimum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [updateBy](./updateBy.md) on the `source` table using the `CumMin` operation. No grouping columns are given, so the cumulative minimum is calculated across all rows of the table.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy(CumMin("MinX = X"), "Letter")
```

The following example builds on the previous by specifying `Letter` as the grouping column. Thus, the cumulative minimum is calculated for each unique letter.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy(CumMin("MinX = X"), "Letter")
```

The following example builds on the previous by calculating the cumulative minimum of two different columns using the same [`UpdateByOperation`](./updateBy.md#parameters).

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)", "Y = randomInt(10, 30)")

result = source.updateBy(CumMin("MinX = X", "MinY = Y"), "Letter")
```

The following example builds on the previous by grouping on two columns instead of one. Each group is defined by a unique combination of letter and boolean in the `Letter` and `Truth` columns, respectively.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(10, 30)")

result = source.updateBy(CumMin("MinX = X", "MinY = Y"), "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumMin(java.lang.String...))
