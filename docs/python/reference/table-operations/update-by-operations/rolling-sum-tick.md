---
title: rolling_sum_tick
---

`rolling_sum_tick` creates a tick-based (row-based) windowed sum operator to be used in an [`update_by`](./updateBy.md) table operation. Data is windowed by a number of reverse and forward ticks (rows) relative to the current row, and the sum of the window is calculated.

## Syntax

```python syntax
rolling_sum_tick(cols: list[str], rev_ticks: int, fwd_ticks: int) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, list[str]]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, the rolling sum is calculated for all applicable columns.

</Param>
<Param name="rev_ticks" type="int">

The look-behind window size in rows. If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used. `rev_ticks` is includes the current row, so `rev_ticks=0` means that the window starts at the current row.

</Param>
<Param name="fwd_ticks" type="int">

The look-forward window size in rows. If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used. `fwd_ticks` is _not_ inclusive of the current row, so `fwd_ticks=1` means that the window ends at the current row.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using three `rolling_sum_tick` operations. Each operation gives varying `rev_ticks` and `fwd_ticks` values to show how they affect the output. The windows for each operation are as follows:

- `op_before`: The window contains two rows. It starts two rows before the current row, and ends at the row before the current row.
- `op_after`: The window contains three rows. It starts one row after the current row, and ends three rows after the current row.
- `op_middle`: The window contains three rows. It starts one row before the current row, and ends one row ahead of the current row.

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

op_before = rolling_sum_tick(cols=["WindowBeforeX = X"], rev_ticks=3, fwd_ticks=-1)
op_after = rolling_sum_tick(cols=["WindowAfterX = X"], rev_ticks=-1, fwd_ticks=3)
op_middle = rolling_sum_tick(cols=["WindowMiddleX = X"], rev_ticks=2, fwd_ticks=1)

result = source.update_by(ops=[op_before, op_after, op_middle], by=["Letter"])
```

## Related documentation

- [How to use update_by](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingSum(long,long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_sum_tick)
