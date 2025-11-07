---
title: cum_max
---

`cum_max` calculates a cumulative maximum in an [`update_by`](./updateBy.md) table operation.

## Syntax

```
cum_max(cols: list[str]) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, the cumulative maximum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using the `cum_max` operation. No grouping columns are given. As a result, the cumulative maximum is calculated for all rows.

```python order=result,source
from deephaven.updateby import cum_max
from deephaven import empty_table

source = empty_table(25).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=cum_max(cols=["MaxX = X"]), by=[])
```

The following example builds on the previous example by specifying `Letter` as the grouping column. Thus, the `result` table has cumulative maximum calculated per unique letter in that column.

```python order=result,source
from deephaven.updateby import cum_max
from deephaven import empty_table

source = empty_table(25).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=cum_max(cols=["MaxX = X"]), by=["Letter"])
```

The following example builds on the previous example by calculating the cumulative maximum for two columns using a single [`UpdateByOperation`](./updateBy.md#parameters).

```python order=result,source
from deephaven.updateby import cum_max
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
        "Y = randomInt(25, 50)",
    ]
)

result = source.update_by(ops=cum_max(cols=["MaxX = X", "MaxY = Y"]), by=["Letter"])
```

The following example builds on the previous example by specifying two key columns. Thus, each group is a unique combination of letter in the `Letter` column and boolean in the `Truth` column.

```python order=result,source
from deephaven.updateby import cum_max
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = randomInt(25, 50)",
    ]
)

result = source.update_by(
    ops=cum_max(cols=["MaxX = X", "MaxY = Y"]), by=["Letter", "Truth"]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumMax(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_max)
