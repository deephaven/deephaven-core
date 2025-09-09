---
title: delta
---

`delta` calculates the difference between the current and previous cells. Null values in the current or previous cell, by default, produces null output. This behavior can be changed through the use of [`DeltaControl`](./DeltaControl.md).

## Syntax

```
delta(cols: list[str], delta_control: DeltaControl = None) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The string names of columns to be operated on. These can include expressions to rename the output, e.g., `"new_col = col"`. When this parameter is left empty, [`update_by`](./updateBy.md) will perform the operation on all applicable columns.

</Param>
<Param name="delta_control" type="DeltaControl">

Defines how special cases should behave. See [`DeltaControl`](./DeltaControl.md).

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example performs an [`update_by`](./updateBy.md) on the `source` table using a `delta` operation, renaming the resulting column to `DeltaX`. No grouping columns are specified, so the `delta` is calculated over all rows.

```python order=result,source
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=[delta(cols="DeltaX = X")])
```

### One delta column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the `delta` is calculated on a per-letter basis.

```python order=result,source
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)

result = source.update_by(ops=[delta(cols="DeltaX = X")], by=["Letter"])
```

### Multiple delta columns, multiple grouping columns

The following example builds on the previous by calculating the `delta` of multiple columns in the same [UpdateByOperation](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = randomInt(0, 25)",
    ]
)

result = source.update_by(
    ops=delta(cols=["DeltaX = X", "DeltaY = Y"]), by=["Letter", "Truth"]
)
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the `delta` of multiple columns, each with its own [UpdateByOperation](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = randomInt(0, 25)",
    ]
)

delta_x = delta(cols=["DeltaX = X"])
delta_y = delta(cols=["DeltaY = Y"])

result = source.update_by(ops=[delta_x, delta_y], by=["Letter", "Truth"])
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`DeltaControl`](./DeltaControl.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Delta(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.delta)
