---
title: forward_fill
---

`forward_fill` creates a forward-fill null replacement operator to be used in an [`update_by`](./updateBy.md) table operation. This operation is forward-only.

## Syntax

```
forward_fill(cols: list[str]) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output columns (e.g., `NewCol = Col`). If `None`, a forward fill is performed on all applicable columns in the table.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### Forward fill all columns of a table

The following example performs a `forward_fill` to replace null values with the most recent non-null. No columns are given to the [`UpdateByOperation`](./updateBy.md#parameters), so the operation is applied to all non-grouping columns in the `source` table. Also, no grouping columns are given, so the operation is applied to all rows.

```python order=result,source
from deephaven.updateby import forward_fill
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 3 == 0) ? NULL_INT : i",
        "Y = (i % 3 == 1) ? i : NULL_INT",
    ]
)

result = source.update_by(ops=forward_fill(cols=[]))
```

### Forward fill all non-key columns

The following example builds on the previous by specifying `Letter` as the grouping column. Thus, the forward fill is applied to all columns except for `Letter`, and is done on a per-letter basis.

```python order=result,source
from deephaven.updateby import forward_fill
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 3 == 0) ? NULL_INT : i",
        "Y = (i % 3 == 1) ? i : NULL_INT",
    ]
)

result = source.update_by(ops=forward_fill(cols=[]), by=["Letter"])
```

### Forward fill one column grouped by multiple key columns

The following example builds on the previous by specifying `Letter` and `Truth` as the grouping columns. Thus, groups are defined by unique combinations of letter and boolean, respectively. The `forward_fill` is only applied to the `X` column, so the `Y` column has no operations applied to it.

```python order=result,source
from deephaven.updateby import forward_fill
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = (i % 3 == 0) ? NULL_INT : i",
        "Y = (i % 3 == 1) ? i : NULL_INT",
    ]
)

result = source.update_by(ops=forward_fill(cols=["X"]), by=["Letter", "Truth"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Fill(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.forward_fill)
