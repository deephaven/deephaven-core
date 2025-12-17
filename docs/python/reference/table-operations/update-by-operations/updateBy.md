---
title: update_by
---

`update_by` performs one or more `UpdateByOperations` grouped by zero or more key columns to calculate cumulative or window-based aggregations of columns in a source table. Operations include cumulative sums, moving averages, EMAs, etc.

The aggregations are defined by the provided operations, which support incremental aggregations over the corresponding rows in the source table. Cumulative aggregations use all rows in the source table, whereas rolling aggregations will apply position or time-based windowing relative to the current row. Calculations are performed over all rows or each row group as identified by the provided key columns.

## Syntax

```
update_by(ops: list[UpdateByOperation], by: list[str] = []) -> Table
```

## Parameters

<ParamTable>
<Param name="ops" type="list[UpdateByOperation]">

A list containing one or more `UpdateByOperations`. The following `UpdateByOperations` are available:

- [`cum_count_where`](./cum-count-where.md)
- [`cum_max`](./cum-max.md)
- [`cum_min`](./cum-min.md)
- [`cum_prod`](./cum-prod.md)
- [`cum_sum`](./cum-sum.md)
- [`delta`](./delta.md)
- [`ema_tick`](./ema-tick.md)
- [`ema_time`](./ema-time.md)
- [`emmax_tick`](./emmax-tick.md)
- [`emmax_time`](./emmax-time.md)
- [`emmin_tick`](./emmin-tick.md)
- [`emmin_time`](./emmin-time.md)
- [`ems_tick`](./ems-tick.md)
- [`ems_time`](./ems-time.md)
- [`emstd_tick`](./emstd-tick.md)
- [`emstd_time`](./emstd-time.md)
- [`forward_fill`](./forward-fill.md)
- [`rolling_avg_tick`](./rolling-avg-tick.md)
- [`rolling_avg_time`](./rolling-avg-time.md)
- [`rolling_count_tick`](./rolling-count-tick.md)
- [`rolling_count_time`](./rolling-count-time.md)
- [`rolling_count_where_tick`](./rolling-count-where-tick.md)
- [`rolling_count_where_time`](./rolling-count-where-time.md)
- [`rolling_formula_tick`](./rolling-formula-tick.md)
- [`rolling_formula_time`](./rolling-formula-time.md)
- [`rolling_group_tick`](./rolling-group-tick.md)
- [`rolling_group_time`](./rolling-group-time.md)
- [`rolling_max_tick`](./rolling-max-tick.md)
- [`rolling_max_time`](./rolling-max-time.md)
- [`rolling_min_tick`](./rolling-min-tick.md)
- [`rolling_min_time`](./rolling-min-time.md)
- [`rolling_prod_tick`](./rolling-prod-tick.md)
- [`rolling_prod_time`](./rolling-prod-time.md)
- [`rolling_std_tick`](./rolling-std-tick.md)
- [`rolling_std_time`](./rolling-std-time.md)
- [`rolling_sum_tick`](./rolling-sum-tick.md)
- [`rolling_sum_time`](./rolling-sum-time.md)
- [`rolling_wavg_tick`](./rolling-wavg-tick.md)
- [`rolling_wavg_time`](./rolling-wavg-time.md)

</Param>
<Param name="by" type="list[str]" optional>

Zero or more key columns that group rows of the table. The default is `[]`, which means no key columns.

</Param>
</ParamTable>

## Returns

A new table with rolling window operations applied to the specified column(s).

## Examples

In the following example, a `source` table is created. The `source` table contains two columns: `Letter` and `X`. An `update_by` is applied to the `source` table, which calculates the cumulative sum of the `X` column. The `Letter` column is given as the `by` column. `Letter` is `A` when `RowIndex` is even, and `B` when odd. Thus, the `result` table contains a new column, `SumX`, which contains the cumulative sum of the `X` column, grouped by `Letter`.

```python order=source,result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=cum_sum(cols=["SumX = X"]), by=["Letter"])
```

The following example takes the same source data, but instead computes a rolling sum using [`rolling_sum_tick`](./rolling-sum-tick.md). The rolling sum is calculated given a window of two rows back, and two rows ahead. Thus, `SumX` has the windowed sum of a five-row window, where each value is at the center of the window. Rows at the beginning and end of the table don't have enough data above and below them, respectively, so their summed values are smaller.

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(
    ops=rolling_sum_tick(cols=["RollingSumX = X"], rev_ticks=3, fwd_ticks=2),
    by=["Letter"],
)
```

The following example builds on the previous examples by adding a second data column, `Y`, to the `source` table. The [`cum_sum`](./cum-sum.md) `UpdateByOperation` is then given two columns, so that the cumulative sum of the `X` and `Y` columns are both calculated.

```python order=source,result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 10)"]
)

result = source.update_by(ops=cum_sum(cols=["SumX = X", "SumY = Y"]), by=["Letter"])
```

The following example modifies the previous example by performing two separate `UpdateByOperations`. The first uses [`cum_sum`](./cum-sum.md) on the `X` column like the previous example, but instead performs a tick-based rolling sum on the `Y` column with [`rolling_sum_tick`](./rolling-sum-tick.md).

```python order=source,result
from deephaven.updateby import cum_sum, rolling_sum_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 10)"]
)

result = source.update_by(
    ops=[
        cum_sum(cols=["SumX = X"]),
        rolling_sum_tick(cols=["RollingSumY = Y"], rev_ticks=2, fwd_ticks=1),
    ],
    by=["Letter"],
)
```

The following example builds on previous examples by adding a second key column, `Truth`, which contains boolean values. Thus, groups are defined by unique combinations of the `Letter` and `Truth` columns.

```python order=source,result
from deephaven.updateby import cum_sum, rolling_sum_tick
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i",
        "Y = randomInt(0, 10)",
    ]
)

result = source.update_by(
    ops=[
        cum_sum(cols=["SumX = X"]),
        rolling_sum_tick(cols=["RollingSumY = Y"], rev_ticks=2, fwd_ticks=1),
    ],
    by=["Letter", "Truth"],
)
```

## Related documentation

- [How to use update_by](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [`OperationControl`](./OperationControl.md)
- [`cum_sum`](./cum-sum.md)
- [`rolling_sum_tick`](./rolling-sum-tick.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#updateBy(java.util.Collection))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update_by)
