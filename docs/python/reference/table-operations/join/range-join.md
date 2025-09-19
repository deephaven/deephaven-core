---
title: range_join
---

The `range_join` method creates a new table containing _all_ the rows and columns of the left table, plus additional columns containing aggregated data from the right table.

> [!NOTE]
> Currently, `range_join` only supports static tables and the `group` aggregation. This operation remains under active development.

`range_join` is a join plus `aggregation` that:

(1) joins arrays of data from the right table onto the left table and then
(2) aggregates over the joined data.

For columns appended to the left table (joins), cell values equal aggregations over vectors of values from the right table. These vectors are formed from all values in the right table where the right table keys fall within the ranges of keys defined by the left table (responsive ranges).

> [!NOTE]
> `null` and `NaN` cell values in the right range column are discarded. They are never included in the vectors used for aggregation. For all rows that are not discarded, the right table must be sorted according to the right range column for all rows within a group.

## Syntax

```python syntax
range_join(
    table: Table,
    on: Union[str, list[str]],
    aggs: Union[Aggregation, list[Aggregation]]
) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The right table of the join.

</Param>
<Param name="on" type="Union[str, Sequence[str]]">

The match expression(s) on which the tables will be joined.
This parameter must include zero-or-more [exact match expressions](#exact-match-expressions), and exactly one [range match expression](#range-match-expression) as described below.

</Param>
<Param name="aggs" type="Union[Aggregation, list[Aggregation]]">

The aggregation(s) to perform over the responsive ranges from the right table for each row from this table.

</Param>
</ParamTable>

### Match Expressions

Join key ranges, specified by the `on` argument, are defined by zero-or-more [exact join matches](#exact-match-expressions) and a single [range match expression](#range-match-expression). The [range match expression](#range-match-expression) must be the last match in the list.

#### Exact Match Expressions

Exact match expressions are parsed like other [join operations](../../../how-to-guides/joins-timeseries-range.md). That is, they are either a column name common to both tables or a column name from the left table followed by an equals sign followed by a column name from the right table.

For example:

- Match on the same column name in both tables: `"common_column"`
- Match on `LeftCol` and `RightCol` in the left and right tables, respectively: `"LeftCol = RightCol"`

#### Range Match Expression

The range match expression is a ternary logical expression that expresses the relationship between a left start column, right range column, and left end column.

The `<` or `<=` logical operator separates each column name pair.

Finally, the entire expression may be preceded by a left arrow (`<-`) and/or followed by a right arrow (`->`). The arrows indicate that range match can "allow preceding" or "allow following" to match values outside the explicit range.

- **Allow preceding** means that if no matching right range column value is equal to the left start column value, the immediately preceding matching right row should be included in the aggregation if such a row exists.
- **Allow following** means that if no matching right range column value is equal to the left end column value, the immediately following matching right row should be included in the aggregation if such a row exists.

For range matches that _exclude_ the left start and end column values, use the following syntax:

`"left_start_column < right_range_column < left_end_column"`

For range matches that _include_ the left start and end column values, use the following syntax:

`"left_start_column <= right_range_column <= left_end_column"`

For range matches that _include_ the left start and end column values, as well as allow preceding and following values, use the following syntax:

`"<- left_start_column <= right_range_column <= left_end_column ->"`

#### Special Cases

To produce aggregated output, [range match expressions](#range-match-expression) must define a range of values to aggregate over. There are a few noteworthy special cases of ranges.

##### Empty Range

An empty range occurs for any left row with no matching right rows. That is, no non-null, non-NaN right rows were found using the exact join matches, or none were in range according to the `range_join` match.

##### Single-Value Ranges

A single-value range is a range where the left row’s values for the left start column and left end column are equal, and both relative matches are inclusive (`<=` and `>=`, respectively). Only rows within the bucket where the right range column matches the single value are included in the output aggregations for a single-value range.

##### Invalid Ranges

An invalid range occurs in two scenarios:

- When the range is inverted - i.e., when the value of the left start column is greater than that of the left end column.
- When either relative-match is exclusive (< or >) and the value in the left start column equals the value in the left end column.

For invalid ranges, the result row will be `null` for all aggregation output columns.

##### Undefined Ranges

An undefined range occurs when either the left start column or the left end column is NaN. For rows with an undefined range, the corresponding output values will be `null` (as with invalid ranges).

##### Unbounded Ranges

A partially or fully unbounded range occurs when either the left start column or the left end column is `null`.

- If the left start column value is `null` and the left end column value is non-null, the range is unbounded at the beginning, and only the left end column subexpression will be - used for the match.
- If the left start column value is non-null and the left end column value is `null`, the range is unbounded at the end, and only the left start column subexpression will be used for the match.
- If the left start column _and_ left end column values are `null`, the range is unbounded, and all rows will be included.

## Returns

A new Table.

## Examples

The following example creates a left table (`lt`) and right table (`rt`), then calls `range_join`. The right table is joined to the left table on the `Y` column, and the [range match expression](#range-match-expression) specifies that matching rows should contain a value in the `RValue` column that is greater than the corresponding `LStartValue` row and less than the corresponding `LEndValue` row. The last argument calls the [`group`](../group-and-aggregate/AggGroup.md) aggregation to group results by `X`.

```python order=lt,rt,result
from deephaven import empty_table
from deephaven.agg import group
from deephaven.table import Table

lt = empty_table(20).update_view(
    ["X=ii", "Y=X % 5", "LStartValue=ii / 0.7", "LEndValue=ii / 0.1"]
)
rt = empty_table(20).update_view(["X=ii", "Y=X % 5", "RValue=ii / 0.3"])

result = lt.range_join(
    table=rt, on=["Y", "LStartValue < RValue < LEndValue"], aggs=group("X")
)
```

Let's break down the output to understand why `X` is grouped as it is.

- `X` = `0` in `lt`
  - When `Y` is `0`, the range expression creates a [single value range](#single-value-ranges). The range expression uses `<`, which results in a `null` cell for the grouped X column.
- `X` = `1` in `lt`
  - When `X` is `1`, `Y` is `1`. The range expression specifies that `RValue` must be greater than `1.4286` and less than `10`. The range join searches rows where `Y` = `1`, and checks if the corresponding `RValue` cell satisfies the criteria. In this case, it's true only when `X` is `1`.
- `X` = `3` in `lt`
  - When `X` is `3`, `Y` is `3`. `RValue` must be greater than `4.2857` and less than `30`. The `X` values where `Y` = `3` and `RValue` satisfies these criteria are `3` and `8`.
- `X` = `6` in `lt`
  - When `X` is `6`, `Y` is `1`. `RValue` must be greater than `8.5714` and less than `60`. The `X` values where `Y` = `1` and `RValue` satisfies these criteria are `6`, `11`, and `16`.

## Related documentation

- [Joins: Time-Series and Range](../../../how-to-guides/joins-timeseries-range.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [`empty_table`](../create/emptyTable.md)
- [`group`](../group-and-aggregate/AggGroup.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.range_join)
