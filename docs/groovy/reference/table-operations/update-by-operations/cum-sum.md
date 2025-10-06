---
title: CumSum
---

`CumSum` calculates a cumulative sum in an [`updateBy`](./updateBy.md) table operation.

## Syntax

```
CumSum(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The column(s) to be operated on. These can include expressions to rename output columns (e.g., `NewCol = Col`). If `None`, the cumulative sum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using the `CumSum` operation. No grouping columns are given, so the cumulative sum is calculated for all rows in the table.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy(CumSum("SumX = X"))
```

The following example builds off the previous by specifying `Letter` as the grouping column. Thus, the cumulative sum of `X` is calculated for each unique letter.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i")

result = source.updateBy(CumSum("SumX = X"), "Letter")
```

The following example builds off the previous by calculating the cumulative sum of two columns using the same [`UpdateByOperation`](./updateBy.md#parameters).

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(1, 11)")

result = source.updateBy(CumSum("SumX = X", "SumY = Y"), "Letter")
```

The following example builds off the previous by specifying two grouping columns: `Letter` and `Truth`. Thus, each group is a unique combination of letter and boolean value in those two columns, respectively.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i", "Y = randomInt(1, 11)")

result = source.updateBy(CumSum("SumX = X", "SumY = Y"), "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumProd(java.lang.String...))
