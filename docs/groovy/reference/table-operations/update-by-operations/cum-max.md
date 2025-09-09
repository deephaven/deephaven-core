---
title: CumMax
---

`CumMax` calculates the cumulative maximum in an [`updateBy`](./updateBy.md) table operation.

## Syntax

```
CumMax(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative maximum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using the `CumMax` operation. No grouping columns are given. As a result, the cumulative maximum is calculated for all rows.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy(CumMax("MaxX = X"))
```

The following example builds on the previous example by specifying `Letter` as the grouping column. Thus, the `result` table has cumulative maximum calculated per unique letter in that column.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy(CumMax("MaxX = X"), "Letter")
```

The following example builds on the previous example by calculating the cumulative maximum for two columns using a single [`UpdateByOperation`](./updateBy.md#parameters).

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)", "Y = randomInt(25, 50)")

result = source.updateBy(CumMax("MaxX = X", "MaxY = Y"), "Letter")
```

The following example builds on the previous example by specifying two key columns. Thus, each group is a unique combination of letter in the `Letter` column and boolean in the `Truth` column.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(25, 50)")

result = source.updateBy(CumMax("MaxX = X", "MaxY = Y"), "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumMax(java.lang.String...))
