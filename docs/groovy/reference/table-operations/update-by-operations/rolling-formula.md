---
title: RollingFormula
---

The `RollingFormula` [`UpdateByOperator`](./updateBy.md#parameters) creates a rolling formula column for the supplied column name pairs, using ticks or time as the windowing unit.

Ticks are row counts. You may specify the reverse and forward window as the number of rows to include. The current row is considered to belong to the reverse window but not the forward window. Negative values are allowed and can be used to generate completely forward or completely reverse windows. Here are some examples of window values:

- `revTicks = 1, fwdTicks = 0` - contains only the current row.
- `revTicks = 10, fwdTicks = 0` - contains 9 previous rows and the current row.
- `revTicks = 0, fwdTicks = 10` - contains the following 10 rows, but excludes the current row.
- `revTicks = 10, fwdTicks = 10` - contains the previous 9 rows, the current row, and the 10 rows following.
- `revTicks = 10, fwdicks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the current row (inclusive).
- `revTicks = 11, fwdTicks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the current row (inclusive).
- `revTicks = -5, fwdTicks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows following the current row (inclusive).

When windowing by time, this function accepts nanoseconds or time strings as the reverse and forward window parameters. Here are some examples of window values:

- `revTime = 0, fwdTime = 0` - contains rows that exactly match the current row timestamp.
- `revTime = 600_000_000_000, fwdDuration = 0` - contains rows from 10m before through the current row timestamp (inclusive).
- `revTime = 0, fwdTime = 600_000_000_000` - contains rows from the current row through 10m following the current row timestamp (inclusive).
- `revDuration = “PT00:10:00”, fwdDuration = “PT00:10:00”` - contains rows from 10m before through 10m following the current row timestamp (inclusive).
- `revDuration = “PT00:10:00”, fwdDuration = “-PT00:05:00”` - contains rows from 10m before through 5m before the current row timestamp (inclusive), this is a purely backward-looking window.
- `revDuration = “-PT00:05:00”, fwdDuration = “PT00:10:00”` - contains rows from 5m following through 10m following the current row timestamp (inclusive), this is a purely forward-looking window.

A row containing a null in the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will be null.

## Syntax

```
RollingFormula(revTicks, fwdTicks, formula, paramToken, pairs...)
RollingFormula(revTicks, formula, paramToken, pairs...)
RollingFormula(timestampCol, revDuration, fwdDuration, formula, paramToken, pairs...)
RollingFormula(timestampCol, revDuration, formula, paramToken, pairs...)
RollingFormula(timestampCol, revTime, fwdTime, formula, paramToken, pairs...)
RollingFormula(timestampCol, revTime, formula, paramToken, pairs...)
```

## Parameters

<ParamTable>
<Param name="revTicks" type="long">

The look-behind window size in ticks (rows). If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used.

</Param>
<Param name="fwdTicks" type="long">

The look-forward window size in ticks (rows). If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
<Param name="formula" type="String">

The user-defined formula to apply to the group. The formula can contain a combination of any of the following:

- Built-in functions such as min, max, etc.
- Mathematical arithmetic such as \*, +, /, etc.
- User-defined functions

The formula can contain key columns used in the `by` parameter. When this happens, the key is presented to the formula as a scalar or string value, not as a vector.

</Param>
<Param name="paramToken" type="String">

The parameter name for the input column's vector within the formula. If formula is `max(each)`, then `each` is the `paramToken`.

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

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs a time-based rolling formula [`updateBy`](./updateBy.md) operation on the `source` table. It calculates a formula using the key column `Sym` as one of the input parameters. `Sym` is a string column; it is presented to the formula as a single string, since each key value exists only once per bucket.

```groovy order=source,result
source = emptyTable(20).update("Timestamp = '2025-06-01T09:30:00Z' + ii * SECOND", "Sym = (ii % 2 == 0) ? `A` : `B`", "IntCol = randomInt(0, 10)", "LongCol = randomLong(0, 100)")

result = source.updateBy([RollingFormula("Timestamp", 4_000_000_000, 2_000_000_000, "OutVal = sum(IntCol) - max(LongCol) + (Sym == null ? 0 : Sym.length())")], by="Sym")
```

The following example performs a tick-based rolling formula [`updateBy`](./updateBy.md) operation on the `source` table. It calculates a formula using the key column `Sym` as one of the input parameters. `Sym` is a string column; it is presented to the formula as a single string, since each key value exists only once per bucket.

```groovy order=source,result
source = emptyTable(20).update("Sym = (ii % 2 == 0) ? `A` : `B`", "IntCol = randomInt(0, 10)", "LongCol = randomLong(0, 100)")

result = source.updateBy([RollingFormula(4, 2, "OutVal = sum(IntCol) - max(LongCol) + (Sym == null ? 0 : Sym.length())")], by="Sym")
```

The following example performs a time-based and a tick-based rolling formula [`updateBy`](./updateBy.md) operation on the `prices` table, grouped by the `Ticker` column.

```groovy order=prices,result
prices = emptyTable(20).update("Timestamp = '2024-02-23T09:30:00 ET' + ii * SECOND", "Ticker = (i % 2 == 0) ? `NVDA` : `GOOG`", "Price = randomDouble(100.0, 500.0)")

formulaTick = RollingFormula(5, "sum(x * x)", "x", "SumPriceSquared_Tick = Price")
formulaTime = RollingFormula("Timestamp", 10_000_000_000, "sum(x * x)", "x", "SumPriceSquared_Time = Price")

result = prices.updateBy([formulaTick, formulaTime], "Ticker")
```

The following example calculates rolling formulas across multiple columns.

```groovy order=source,result
source = emptyTable(20).update("Timestamp = '2024-02-23T09:30:00 ET' + ii * SECOND", "X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`")

formulaTick = RollingFormula(5, "OutputTick = sum(X + Y + Z)")
formulaTime = RollingFormula("Timestamp", 5_000_000_000, "OutputTime = sum(X + Y + Z)")

result = source.updateBy([formulaTick, formulaTime], "Letter")
```

## Related documentation

- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingFormula(long,long,java.lang.String))
