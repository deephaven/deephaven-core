---
title: rolling_count_tick
---

`rolling_count_tick` creates an [`update_by`](./updateBy.md) table operation that keeps a count of the number of values that exist in a rolling window, using using table ticks as the windowing unit. Ticks are row counts. The rolling count can be calculated using forward and/or backward windows.

## Syntax

```
rolling_count_tick(cols: list[str], rev_ticks: int, fwd_ticks: int) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="cols" type="list[str]">

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

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using `rolling_count_tick`.

```python order=source,result
from deephaven.updateby import rolling_count_tick
from deephaven import new_table
from deephaven.constants import NULL_INT
from deephaven.column import int_col, string_col

source = new_table(
    cols=[
        string_col("Letter", ["A", "B", "A", "B", "A", "B", "A", "B", "A", "B"]),
        int_col("X", [1, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 5, NULL_INT, 4]),
    ]
)

op_before = rolling_count_tick(cols=["OpBefore = X"], rev_ticks=3, fwd_ticks=-1)
op_after = rolling_count_tick(cols=["OpAfter = X"], rev_ticks=-1, fwd_ticks=3)
op_middle = rolling_count_tick(cols=["OpMiddle = X"], rev_ticks=1, fwd_ticks=1)

result = source.update_by(ops=[op_before, op_after, op_middle], by="Letter")
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingCount(java.lang.String,java.time.Duration,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_count_tick)
