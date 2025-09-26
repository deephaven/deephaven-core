---
title: rolling_formula_time
---

`rolling_formula_time` creates a rolling formula column in an [`update_by`](./updateBy.md) table operation for the supplied column names, using time as the windowing unit.

This function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are allowed and can be used to generate completely forward or completely reverse windows. Here are some examples of window values:

- `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp.
- `rev_time = “PT00:10:00”, fwd_time = “0”` - contains rows from 10m before through the current row timestamp (inclusive).
- `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the current row timestamp (inclusive).
- `rev_time = “PT00:10:00”, fwd_time = “PT00:10:00”` - contains rows from 10m before through 10m following the current row timestamp (inclusive).
- `rev_time = “PT00:10:00”, fwd_time = “-PT00:05:00”` - contains rows from 10m before through 5m before the current row timestamp (inclusive), this is a purely backward-looking window.
- `rev_time = “-PT00:05:00”, fwd_time = “PT00:10:00”` - contains rows from 5m following through 10m following the current row timestamp (inclusive), this is a purely forward-looking window.

A row containing a null in the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will be null.

## Syntax

```python syntax
rolling_formula_time(
  ts_col: str,
  formula: str,
  formula_param: str,
  cols: Union[str, list[str]],
  rev_time: Union[int, str],
  fwd_time: Union[int, str] = 0
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="ts_col" type="str">

The timestamp column to be used for the time-based windowing.

</Param>
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
<Param name="rev_time" type="Union[int, str]">

The look-behind window size. This can be expressed as an integer representing a number of nanoseconds, or a time interval string, e.g. “PT00:00:00.001” or “PT5M”.

</Param>
<Param name="fwd_time" type="Union[int,str">

The look-ahead window size. This can be expressed as an integer representing a number of nanoseconds, or a time interval string, e.g. “PT00:00:00.001” or “PT5M”.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using `rolling_formula_time`. It calculates a formula using the key column `Sym` as one of the input parameters. `Sym` is a string column; it is presented to the formula as a single string, since each key value exists only once per bucket.

```python order=source,result
from deephaven.updateby import rolling_formula_time
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Timestamp = '2025-06-01T09:30:00Z' + ii * SECOND",
        "Sym = (ii % 2 == 0) ? `A` : `B`",
        "IntCol = randomInt(0, 10)",
        "LongCol = randomLong(0, 100)",
    ]
)

result = source.update_by(
    rolling_formula_time(
        ts_col="Timestamp",
        formula="OutVal = sum(IntCol) - max(LongCol) + (Sym == null ? 0 : Sym.length())",
        rev_time="PT4s",
        fwd_time="PT2s",
    ),
    by="Sym",
)
```

The following example performs an [`update_by`](./updateBy.md) on the `prices` table using a `rolling_formula_time` operation to calculate a rolling sum of squares of prices. Note that since the formula is applied to one column at a time, the `each` parameter is used to refer to the current column being processed.

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

formula_time = rolling_formula_time(
    ts_col="Timestamp",
    formula="sum(x * x)",
    formula_param="x",
    cols="SumPriceSquared_Time = Price",
    rev_time="PT10s",
)

result = prices.update_by(ops=[formula_time], by="Ticker")
```

The following example shows how to apply a formula across multiple columns. Since the formula is applied to multiple columns at a time, no `each` parameter is given.

```python order=source,result
from deephaven import empty_table
from deephaven.updateby import rolling_formula_time

source = empty_table(20).update(
    [
        "Timestamp = '2025-03-01T09:30:00Z' + ii * SECOND",
        "X = i",
        "Y = 2 * i",
        "Z = 3 * i",
        "Letter = (X % 2 == 0) ? `A` : `B`",
    ]
)

result = source.update_by(
    ops=rolling_formula_time(
        ts_col="Timestamp", formula="Output=sum(X + Y + Z)", rev_time="PT5s"
    ),
    by="Letter",
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
