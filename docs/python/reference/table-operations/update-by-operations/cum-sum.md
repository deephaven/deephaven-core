---
title: cum_sum
---

`cum_sum` calculates a cumulative sum in an [`update_by`](./updateBy.md) table operation.

## Syntax

```
cum_sum(cols: list[str]) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename output columns (e.g., `NewCol = Col`). If `None`, the cumulative sum is calculated for all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using the `cum_sum` operation. No grouping columns are given, so the cumulative sum is calculated for all rows in the table.

```python order=result,source
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=cum_sum(cols=["SumX = X"]), by=[])
```

The following example builds off the previous by specifying `Letter` as the grouping column. Thus, the cumulative sum of `X` is calculated for each unique letter.

```python order=result,source
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=cum_sum(cols=["SumX = X"]), by=["Letter"])
```

The following example builds off the previous by calculating the cumulative sum of two columns using the same [UpdateByOperation](./updateBy.md#parameters).

```python order=result,source
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(1, 11)"]
)

result = source.update_by(ops=cum_sum(cols=["SumX = X", "SumY = Y"]), by=["Letter"])
```

The following example builds off the previous by specifying two grouping columns: `Letter` and `Truth`. Thus, each group is a unique combination of letter and boolean value in those two columns, respectively.

```python order=result,source
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i",
        "Y = randomInt(1, 11)",
    ]
)

result = source.update_by(
    ops=cum_sum(cols=["SumX = X", "SumY = Y"]), by=["Letter", "Truth"]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumSum(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_sum)
