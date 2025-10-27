---
title: CumProd
---

`CumProd` calculates a cumulative product in an [`updateBy`](./updateBy.md) table operation.

## Syntax

```
CumProd(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative product is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using the `CumProd` operation. No grouping columns are given, so the cumulative product is calculated across all rows of the table.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1")

result = source.updateBy(CumProd("ProdX = X"))
```

The following example builds on the previous by specifying `Letter` as a grouping column. Thus, the cumulative product in the `result` table is calculated on a per-letter basis.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1")

result = source.updateBy(CumProd("ProdX = X"), "Letter")
```

The following example builds on the previous by calculating the cumulative product of two columns using the same [`UpdateByOperation`](./updateBy.md#parameters).

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1", "Y = randomInt(1, 7)")

result = source.updateBy(CumProd("ProdX = X", "ProdY = Y"), "Letter")
```

The following example builds on the previous by specifying two grouping columns: `Letter` and `Truth`. Thus, groups consist of unique combinations of letter in `Letter` and boolean in `Truth`.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = i + 1", "Y = randomInt(1, 7)")

result = source.updateBy(CumProd("ProdX = X", "ProdY = Y"), "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumProd(java.lang.String...))
