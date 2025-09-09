---
title: Fill
---

`Fill` replaces null values in specified column of a table with the last known non-null value. This operation is forward-only.

## Syntax

```
Fill(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

### Forward fill all columns of a table

The following example performs a `Fill` to replace null values with the most recent non-null. No columns are given to the [`UpdateByOperation`](./updateBy.md#parameters), so the operation is applied to all non-grouping columns in the `source` table. Also, no grouping columns are given, so the operation is applied to all rows.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 3 == 0) ? NULL_INT : i", "Y = (i % 3 == 1) ? i : NULL_INT")

result = source.updateBy(Fill())
```

### Forward fill all non-key columns

The following example builds on the previous by specifying `Letter` as the grouping column. Thus, the forward fill is applied to all columns except for `Letter`, and is done on a per-letter basis.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 3 == 0) ? NULL_INT : i", "Y = (i % 3 == 1) ? i : NULL_INT")

result = source.updateBy(Fill(), "Letter")
```

### Forward fill one column grouped by multiple key columns

The following example builds on the previous by specifying `Letter` and `Truth` as the grouping columns. Thus, groups are defined by unique combinations of letter and boolean, respectively. The `Fill` is only applied to the `X` column, so the `Y` column has no operations applied to it.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = (i % 3 == 0) ? NULL_INT : i", "Y = (i % 3 == 1) ? i : NULL_INT")

result = source.updateBy(Fill("X"), "Letter", "Truth")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Fill(java.lang.String...))
