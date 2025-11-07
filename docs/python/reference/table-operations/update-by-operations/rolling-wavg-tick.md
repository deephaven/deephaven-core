---
title: rolling_wavg_tick
---

`rolling_wavg_tick` creates a rolling weighted average in an [`update_by`](./updateBy.md) table operation using table ticks as the windowing unit. Ticks are row counts. The rolling weighted average can be calculated using forward and/or backward windows.

## Syntax

```
rolling_wavg_tick(wcol: str, cols: Union[str, list[str]], rev_ticks: int, fwd_ticks: int) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="wcol" type="str">

The column containing the weight values.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). When left empty, the rolling count is calculated for all applicable columns.

</Param>
<Param name="rev_ticks" type="int">

The look-behind window size in rows. If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used. Includes the current row.

</Param>
<Param name="fwd_ticks" type="int">

The look-forward window size in rows. If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an `update_by` table operation.

## Example

The following example performs an [`update_by`](./updateBy.md) on the `source` table using a `rolling_wavg_tick` operation.

```python order=source,result
from deephaven.updateby import rolling_wavg_tick
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 40)",
        "Weight = randomInt(0, 40)",
    ]
)

result = source.update_by(
    ops=[rolling_wavg_tick(wcol="Weight", cols="X", rev_ticks=5, fwd_ticks=0)],
    by="Letter",
)
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingWAvg(long,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_wavg_tick)
