---
title: cum_prod
---

`cum_prod` calculates a cumulative product in an [`update_by`](./updateBy.md) table operation.

## Syntax

```
cum_prod(cols: list[str]) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative product is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using the `cum_prod` operation. No grouping columns are given, so the cumulative product is calculated across all rows of the table.

```python order=result,source
from deephaven.updateby import cum_prod
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1"])

result = source.update_by(ops=cum_prod(cols=["ProdX = X"]), by=[])
```

The following example builds on the previous by specifying `Letter` as a grouping column. Thus, the cumulative product in the `result` table is calculated on a per-letter basis.

```python order=result,source
from deephaven.updateby import cum_prod
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1"])

result = source.update_by(ops=cum_prod(cols=["CumX = X"]), by=["Letter"])
```

The following example builds on the previous by calculating the cumulative product of two columns using the same [UpdateByOperation](./updateBy.md#parameters).

```python order=result,source
from deephaven.updateby import cum_prod
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i + 1", "Y = randomInt(1, 7)"]
)

result = source.update_by(ops=cum_prod(cols=["ProdX = X", "ProdY = Y"]), by=["Letter"])
```

The following example builds on the previous by specifying two grouping columns: `Letter` and `Truth`. Thus, groups consist of unique combinations of letter in `Letter` and boolean in `Truth`.

```python order=result,source
from deephaven.updateby import cum_prod
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i + 1",
        "Y = randomInt(1, 7)",
    ]
)

result = source.update_by(
    ops=cum_prod(cols=["ProdX = X", "ProdY = Y"]), by=["Letter", "Truth"]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumProd(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_prod)
