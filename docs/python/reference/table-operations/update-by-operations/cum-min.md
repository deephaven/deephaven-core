---
title: cum_min
---

`cum_min` calculates a cumulative minimum in an [`update_by`](./updateBy.md) table operation.

## Syntax

```
cum_min(cols: list[str]) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative minimum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using the `cum_min` operation. No grouping columns are given, so the cumulative minimum is calculated across all rows of the table.

```python order=result,source
from deephaven.updateby import cum_min
from deephaven import empty_table

source = empty_table(25).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=cum_min(cols=["MinX = X"]), by=[])
```

The following example builds on the previous by specifying `Letter` as the grouping column. Thus, the cumulative minimum is calculated for each unique letter.

```python order=result,source
from deephaven.updateby import cum_min
from deephaven import empty_table

source = empty_table(25).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=cum_min(cols=["MinX = X"]), by=["Letter"])
```

The following example builds on the previous by calculating the cumulative minimum of two different columns using the same [`UpdateByOperation`](./updateBy.md#parameters).

```python order=result,source
from deephaven.updateby import cum_min
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
        "Y = randomInt(10, 30)",
    ]
)

result = source.update_by(ops=cum_min(cols=["MinX = X", "MinY = Y"]), by=["Letter"])
```

The following example builds on the previous by grouping on two columns instead of one. Each group is defined by a unique combination of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import cum_min
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = randomInt(10, 30)",
    ]
)

result = source.update_by(
    ops=cum_min(cols=["MinX = X", "MinY = Y"]), by=["Letter", "Truth"]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumMin(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_min)
