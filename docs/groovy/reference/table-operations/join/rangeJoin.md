---
title: rangeJoin
---

The `rangeJoin` method creates a new table containing _all_ the rows and columns of the left table, plus additional columns containing aggregated data from the right table. The range of rows that respond to the join operation is determined by one or more [exact-match criteria](#exact-match-expressions) and a [range-match criteria](#range-match-expressions).

> [!NOTE]
> Currently, implementations only support static (i.e., `!isRefreshing`) Tables and `group` aggregations. This operation remains under active development.

For columns appended to the left table (joins), cell values equal aggregations over vectors of values from the right table. These vectors are formed from all values in the right table where the right table keys fall within the ranges of keys defined by the left table (responsive ranges).

> [!NOTE]
> `null` and `NaN` cell values in the right range column are discarded. They are never included in the vectors used for aggregation. For all rows that are not discarded, the right table must be sorted according to the right range column for all rows within a group.

## Syntax

```groovy syntax
table.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations)
```

## Parameters

<ParamTable>
<Param name="rightTable" type="Table">

The table to join with.

</Param>
<Param name="exactMatches" type="Collection<? extends JoinMatch">

A collection of [`JoinMatch`es](/core/javadoc/io/deephaven/api/JoinMatch.html) that dictate exact-match criteria. That is, rows from `rightTable` that might be responsive to rows from the left table will have identical values for the column pairs expressed by these matches.

For example:

- Match on the same column name in both tables: `"commonColumn"`
- Match on `LeftCol` and `RightCol` in the left and right tables, respectively: `"LeftCol = RightCol"`

This collection may be left empty.

</Param>
<Param name="rangeMatch" type="RangeJoinMatch">

Specifies the range match criteria for determining the responsive rows from `rightTable` for each row from this Table, within the buckets created by matching on `the exactMatches`.

For example:

`"leftStartColumn < rightRangeColumn < leftEndColumn"`

`"leftStartColumn <= rightRangeColumn <= leftEndColumn"`

`"<- leftStartColumn <= rightRangeColumn <= leftEndColumn ->"`

</Param>
<Param name="aggregations" type="Collection<? extends Aggregation>">

The aggregations to perform over the responsive ranges from `rightTable` for each row from this Table.

</Param>
</ParamTable>

### Match Expressions

Join key ranges are defined by zero-or-more [exact join matches](#exact-match-expressions) and a single [range match expression](#range-match-expressions).

#### Exact Match Expressions

Exact match expressions are parsed like other [join operations](../../../how-to-guides/joins-exact-relational.md). That is, they are either a column name common to both tables or a column name from the left table followed by an equals sign followed by a column name from the right table.

For example:

- Match on the same column name in both tables: `"commonColumn"`
- Match on `LeftCol` and `RightCol` in the left and right tables, respectively: `"LeftCol = RightCol"`

#### Range Match Expressions

The range match expression is a ternary logical expression that expresses the relationship between a left start column, right range column, and left end column.

The `<` or `<=` logical operator separates each column name pair.

Finally, the entire expression may be preceded by a left arrow (`<-`) and/or followed by a right arrow (`->`). The arrows indicate that range match can "allow preceding" or "allow following" to match values outside the explicit range.

- **Allow preceding** means that if no matching right range column value is equal to the left start column value, the immediately preceding matching right row should be included in the aggregation if such a row exists.
- **Allow following** means that if no matching right range column value is equal to the left end column value, the immediately following matching right row should be included in the aggregation if such a row exists.

For range matches that _exclude_ the left start and end column values, use the following syntax:

`"leftStartColumn < rightRangeColumn < leftEndColumn"`

For range matches that _include_ the left start and end column values, use the following syntax:

`"leftStartColumn <= rightRangeColumn <= leftEndColumn"`

For range matches that _include_ the left start and end column values, as well as allow preceding and following values, use the following syntax:

`"<- leftStartColumn <= rightRangeColumn <= leftEndColumn ->"`

#### Special Cases

To produce aggregated output, [range match expressions](#range-match-expressions) must define a range of values to aggregate over. There are a few noteworthy special cases of ranges.

##### Empty Range

An empty range occurs for any left row with no matching right rows. That is, no non-null, non-NaN right rows were found using the exact join matches, or none were in range according to the `rangeMatch`.

##### Single-Value Ranges

A single-value range is a range where the left row’s values for the left start column and left end column are equal, and both relative matches are inclusive (`<=` and `>=`, respectively). Only rows within the bucket where the right range column matches the single value are included in the output aggregations for a single-value range.

##### Invalid Ranges

An invalid range occurs in two scenarios:

- When the range is inverted - i.e., when the value of the left start column is greater than that of the left end column.
- When either relative-match is exclusive (< or >) and the value in the left start column equals the value in the left end column.

For invalid ranges, the result row will be `null` for all aggregation output columns.

##### Undefined Ranges

An undefined range occurs when either the left start column or the left end column is `NaN`. For rows with an undefined range, the corresponding output values will be `null` (as with invalid ranges).

##### Unbounded Ranges

A partially or fully unbounded range occurs when either the left start column or the left end column is `null`.

- If the left start column value is `null` and the left end column value is non-null, the range is unbounded at the beginning, and only the left end column subexpression will be used for the match.
- If the left start column value is non-null and the left end column value is `null`, the range is unbounded at the end, and only the left start column subexpression will be used for the match.
- If the left start column _and_ left end column values are `null`, the range is unbounded, and all rows will be included.

## Returns

A Table.

## Examples

The following example creates a left table (`lt`) and right table (`rt`), then calls `rangeJoin`. The right table is joined to the left table on the `Y` column, and the [range match expression](#range-match-expressions) specifies that matching rows should contain a value in the `RValue` column that is greater than the corresponding `LStartValue` row and less than the corresponding `LEndValue` row. The last argument calls the [`group`](../group-and-aggregate/AggGroup.md) aggregation to group results by `X`.

```groovy order=lt,rt,result
lt = emptyTable(20).updateView("X=ii", "Y=X % 5", "LStartValue=ii / 0.7", "LEndValue=ii / 0.1")
rt = emptyTable(20).updateView("X=ii", "Y=X % 5", "RValue=ii / 0.3")

result = lt.rangeJoin(rt, List.of("Y", "LStartValue < RValue < LEndValue"), List.of(AggGroup("X")))
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

- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [`empty_table`](../create/emptyTable.md)
- [`group`](../group-and-aggregate/AggGroup.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#rangeJoin(TABLE,java.util.Collection,java.util.Collection))
