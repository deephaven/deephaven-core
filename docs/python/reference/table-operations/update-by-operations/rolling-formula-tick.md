---
title: rolling_formula_tick
---

`rolling_formula_tick` creates a rolling formula column in an [`update_by`](./updateBy.md) table operation for the supplied column names, using ticks as the windowing unit.

Ticks are row counts, and you may specify the reverse and forward window in number of rows to include. The current row is considered to belong to the reverse window, but not the forward window. Also, negative values are allowed and can be used to generate completely forward or completely reverse windows. Here are some examples of window values:

- `rev_ticks = 1, fwd_ticks = 0` - contains only the current row.
- `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row.
- `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, but excludes the current row.
- `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row, and the 10 rows following.
- `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the current row (inclusive).
- `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the current row (inclusive).
- `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following the current row (inclusive).

## Syntax

```python syntax
rolling_formula_tick(
  formula: str,
  formula_param: str,
  cols: Union[str, list[str]],
  rev_ticks: int,
  fwd_ticks: int = 0
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="formula" type="str">

The user-defined [formula](../../../how-to-guides/formulas.md) to apply to each group. The [formula](../../../how-to-guides/formulas.md) can contain a combination of any of the following:

- Built-in functions such as min, max, etc.
- Mathematical arithmetic such as `*`, `+`, `/`, etc.
- [Python functions](../../../how-to-guides/python-functions.md)

If `formula_param` is not `None`, the formula can only be applied to one column at a time, and it is applied to the specified `formula_param`. If `formula_param` is `None`, the formula is applied to any column or literal value present in the formula. The use of `formula_param` is deprecated.

Key column(s) can be used as input to the formula. When this happens, key values are treated as scalars.

</Param>
<Param name="formula_param" type="str">

The parameter name within the formula. If `formula_param` is `each`, then `formula` must contain `each`. For example, `max(each)`, `min(each)`, etc. Use of this parameter is deprecated.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). When left empty, the rolling formula operation is performed on all columns.

</Param>
<Param name="rev_ticks" type="int">

The look-behind window size in rows. If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used. Includes the current row.

</Param>
<Param name="fwd_ticks" type="int">

The look-forward window size in rows. If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using `rolling_formula_tick`. It calculates a formula using the key column `Sym` as one of the input parameters. `Sym` is a string column; it is presented to the formula as a single string, since each key value exists only once per bucket.

```python order=source,result
from deephaven.updateby import rolling_formula_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Sym = (ii % 2 == 0) ? `A` : `B`",
        "IntCol = randomInt(0, 10)",
        "LongCol = randomLong(0, 100)",
    ]
)

result = source.update_by(
    rolling_formula_tick(
        formula="OutVal = sum(IntCol) - max(LongCol) + (Sym == null ? 0 : Sym.length())",
        rev_ticks=4,
        fwd_ticks=2,
    ),
    by="Sym",
)
```

The following example performs an [`update_by`](./updateBy.md) on the `prices` table using a `rolling_formula_tick` operation to calculate a rolling sum of squares of prices. Note that since the formula is applied to one column at a time, the `each` parameter is used to refer to the current column being processed.

```python order=prices,result
from deephaven.updateby import rolling_formula_tick, rolling_formula_time
from deephaven import empty_table

prices = empty_table(20).update(
    [
        "Timestamp = '2024-02-23T09:30:00 ET' + ii * SECOND",
        "Ticker = (i % 2 == 0) ? `NVDA` : `GOOG`",
        "Price = randomDouble(100.0, 500.0)",
    ]
)

formula_tick = rolling_formula_tick(
    formula="sum(x * x)",
    formula_param="x",
    cols="SumPriceSquared_Tick = Price",
    rev_ticks=5,
)

result = prices.update_by(ops=[formula_tick], by="Ticker")
```

The following example performs an [`update_by`](./updateBy.md) on the `prices` table using a `rolling_formula_tick` operation to calculate a rolling average of multiple columns. Since the formulas are applied to multiple columns at a time, no `each` parameter is given.

```python order=source,result
from deephaven import empty_table
from deephaven.updateby import rolling_formula_tick

source = empty_table(20).update(
    ["X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`"]
)

result = source.update_by(
    ops=rolling_formula_tick(formula="Output=sum(X + Y + Z)", rev_ticks=5), by="Letter"
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingGroup(long,long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_group_tick)
