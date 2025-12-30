---
title: RollingCountWhere
---

`RollingCountWhere` creates an [`updateBy`](./updateBy.md) table operation that keeps a count of values in a
rolling window that pass a set of filters in an [`updateBy`](./updateBy.md) table operation using ticks or time as the windowing unit.

Ticks are row counts. You may specify the reverse and forward window as the number of rows to include. The current row
is considered to belong to the reverse window but not the forward window. Negative values are allowed and can be used to
generate completely forward or completely reverse windows. Here are some examples of window values:

- `revTicks = 1, fwdTicks = 0` - contains only the current row.
- `revTicks = 10, fwdTicks = 0` - contains 9 previous rows and the current row.
- `revTicks = 0, fwdTicks = 10` - contains the following 10 rows, but excludes the current row.
- `revTicks = 10, fwdTicks = 10` - contains the previous 9 rows, the current row, and the 10 rows following.
- `revTicks = 10, fwdTicks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the current row (inclusive).
- `revTicks = 11, fwdTicks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the current row (inclusive).
- `revTicks = -5, fwdTicks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following the current row (inclusive).

When windowing by time, this function accepts nanoseconds or time strings as the reverse and forward window parameters.
Here are some examples of window values:

- `revTime = 0, fwdTime = 0` - contains rows that exactly match the current row timestamp.
- `revTime = 600_000_000_000, fwdDuration = 0` - contains rows from 10m before through the current row timestamp (inclusive).
- `revTime = 0, fwdTime = 600_000_000_000` - contains rows from the current row through 10m following the current row timestamp (inclusive).
- `revDuration = “PT00:10:00”, fwdDuration = “PT00:10:00”` - contains rows from 10m before through 10m following the current row timestamp (inclusive).
- `revDuration = “PT00:10:00”, fwdDuration = “-PT00:05:00”` - contains rows from 10m before through 5m before the current row timestamp (inclusive), this is a purely backward-looking window.
- `revDuration = “-PT00:05:00”, fwdDuration = “PT00:10:00”` - contains rows from 5m following through 10m following the current row timestamp (inclusive), this is a purely forward-looking window.

A row containing a null in the timestamp column belongs to no window and will not be considered in the windows of other rows;
its output will be null.

## Syntax

```
RollingCountWhere(revTicks, resultColumn, filters...);
RollingCountWhere(revTicks, fwdTicks, resultColumn, filters...);
RollingCountWhere(timestampCol, revDuration, resultColumn, filters...);
RollingCountWhere(timestampCol, revDuration, Duration fwdDuration, resultColumn, filters...);
RollingCountWhere(timestampCol, revTime, resultColumn, filters...);
RollingCountWhere(timestampCol, revTime, fwdTime, resultColumn, filters...); {
```

## Parameters

<ParamTable>
<Param name="revTicks" type="long">

The look-behind window size in ticks (rows). If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used.

</Param>
<Param name="fwdTicks" type="long">

The look-forward window size in ticks (rows). If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
<Param name="resultColumn" type="str">

The name of the column that will contain the count of values that pass the filters.

</Param>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Any filter is permitted, as long as it is not refreshing and does not use row position/key variables or arrays.

</Param>
<Param name="pairs" type="String...">

The input/output column name pairs.

</Param>
<Param name="timestampCol" type="String">

The name of the timestamp column.

</Param>
<Param name="revDuration" type="Duration">

The look-behind window size in Duration.

</Param>
<Param name="fwdDuration" type="Duration">

The look-forward window size in Duration.

</Param>
<Param name="revTime" type="long">

The look-behind window size in nanoseconds.

</Param>
<Param name="fwdTime" type="long">

The look-forward window size in nanoseconds.

</Param>
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter will result in an `AND` operation being applied to the
> filters. For example,
> `"Number % 3 == 0", "Number % 5 == 0"` will return the count of values where `Number` is evenly divisible by
> both `3` and `5`. You can also write this as a single conditional filter, `"Number % 3 == 0 && Number % 5 == 0"`, and
> receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals
> `M` or `N`.

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an `updateBy` table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using `RollingCountWhere`. The same
filter is used but we are providing three different windows for the rolling calculation.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)")

String[] filtersToApply = ["Y >= 20", "Y < 99"]

op_before = RollingCountWhere(3, -1,"count_before", filtersToApply)
op_after = RollingCountWhere(-1, 3, "count_after", filtersToApply)
op_middle = RollingCountWhere(1, 1, "count_middle", filtersToApply)

result = source.updateBy([op_before, op_after, op_middle])
```

The following example performs an `OR` filter and computes the results for each value in the
`Letter` column separately.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)")

filterToApply = "Y < 20 || Y >= 80"

op_before = RollingCountWhere(3, -1,"count_before", filterToApply)
op_after = RollingCountWhere(-1, 3, "count_after", filterToApply)
op_middle = RollingCountWhere(1, 1, "count_middle", filterToApply)

result = source.updateBy([op_before, op_after, op_middle], "Letter")
```

The following example uses a complex filter involving multiple columns, with the results bucketed by the `Letter` column.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)")

filterToApply = "(Y < 20 || Y >= 80 || Y % 7 == 0) && X >= 3"

op_before = RollingCountWhere(3, -1,"count_before", filterToApply)
op_after = RollingCountWhere(-1, 3, "count_after", filterToApply)
op_middle = RollingCountWhere(1, 1, "count_middle", filterToApply)

result = source.updateBy([op_before, op_after, op_middle], "Letter")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingCountWhere(long,long,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_count_where_tick)
