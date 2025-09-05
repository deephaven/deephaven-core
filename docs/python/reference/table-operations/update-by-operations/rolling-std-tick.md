---
title: rolling_std_tick
---

`rolling_std_tick` creates a rolling sample standard deviation in an [`update_by`](./updateBy.md) table operation using table ticks as the windowing unit. Ticks are row counts. The rolling sample standard deviation can be calculated using forward and/or backward windows.

Sample standard deviation is calculated as the square root of the [Bessel-corrected sample variance](https://en.wikipedia.org/wiki/Bessel%27s_correction), which can be shown to be an [unbiased estimator](https://en.wikipedia.org/wiki/Bias_of_an_estimator) of population variance under some conditions. However, sample standard deviation is a biased estimator of population standard deviation.

## Syntax

```
rolling_std_tick(cols: list[str], rev_ticks: int, fwd_ticks: int) -> UpdateByOperation
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

The following example performs an [update_by](./updateBy.md) on the `source` table using two `rolling_std_tick` operations.

```python order=source,result
from deephaven.updateby import rolling_std_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)"]
)

op_before = rolling_std_tick(cols=["OpBefore = X"], rev_ticks=3, fwd_ticks=-1)
op_after = rolling_std_tick(cols=["OpAfter = X"], rev_ticks=-1, fwd_ticks=3)
op_middle = rolling_std_tick(cols=["OpMiddle = X"], rev_ticks=1, fwd_ticks=1)

result = source.update_by(ops=[op_before, op_after, op_middle], by="Letter")
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingStd(long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_std_tick)
