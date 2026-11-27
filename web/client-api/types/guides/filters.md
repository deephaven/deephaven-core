---
title: Filters
---

# Filters

Filters are built rather than written as strings. {@link dh.FilterValue.ofColumn | FilterValue.ofColumn} names a column,
and {@link dh.FilterValue.ofString | ofString}, {@link dh.FilterValue.ofNumber | ofNumber} and
{@link dh.FilterValue.ofBoolean | ofBoolean} wrap a value; comparing them gives a {@link dh.FilterCondition}. A value is
always sent as a value, never parsed as part of an expression. Both {@link dh.TableOperations.where | where} and
{@link dh.Table.applyFilter | Table.applyFilter} take an array of conditions, and any resulting data will match all of
them.

{@includeCode ../examples/use-cases.ts#filterConditions}

## Comparisons

{@link dh.FilterValue.eq | eq}, {@link dh.FilterValue.notEq | notEq}, {@link dh.FilterValue.lessThan | lessThan},
{@link dh.FilterValue.lessThanOrEqualTo | lessThanOrEqualTo}, {@link dh.FilterValue.greaterThan | greaterThan} and
{@link dh.FilterValue.greaterThanOrEqualTo | greaterThanOrEqualTo} compare a column with a value or with another column.
{@link dh.FilterValue.eqIgnoreCase | eqIgnoreCase} and {@link dh.FilterValue.notEqIgnoreCase | notEqIgnoreCase} ignore
case.

## Sets

{@link dh.FilterValue.in | in} and {@link dh.FilterValue.notIn | notIn} test against a list of values, and their
`IgnoreCase` variants ignore case.

## Text

{@link dh.FilterValue.contains | contains} finds a substring, and {@link dh.FilterValue.matches | matches} tests a Java
regular expression against the whole value, so `"AA"` does not match `AAPL` but `"AA.*"` does. Each has an `IgnoreCase`
variant.

## Booleans and nulls

{@link dh.FilterValue.isTrue | isTrue}, {@link dh.FilterValue.isFalse | isFalse} and
{@link dh.FilterValue.isNull | isNull} test a column on its own.

## Combining and negating conditions

{@link dh.FilterCondition.and | and} combines a condition with any number of others into one that holds when all of them
hold. {@link dh.FilterCondition.or | or} does the same, holding when any of them holds.

{@link dh.FilterCondition.not | not} negates a condition. A condition made by {@link dh.FilterCondition.search | search}
cannot be negated: the server rejects it.

## Methods and searches

{@link dh.FilterValue.invoke | FilterValue.invoke} calls a method on a value, such as `startsWith` on a string, and
{@link dh.FilterCondition.invoke | FilterCondition.invoke} calls a function.
{@link dh.FilterCondition.search | FilterCondition.search} looks for a value across all columns, or the ones given. It
takes its value from {@link dh.FilterValue.ofString | ofString}, even to find a number; a value from `ofNumber` matches
nothing.

## With a dh.Table

A {@link dh.Table} takes the same conditions with {@link dh.Table.applyFilter | applyFilter}. Unlike
{@link dh.TableOperations.where | where}, which makes a new table, it replaces the table's own filters; the table
changes asynchronously, and fires {@link dh.Table.EVENT_FILTERCHANGED} once the filters apply. Filters equal to the
current ones change nothing, so no event fires. {@link dh.Column.filter | Column.filter} starts a condition from one of
the table's columns, so conditions can be built from the columns a table has, such as all those of one
{@link dh.Column.type | type}.

{@includeCode ../examples/use-cases.ts#filterMutableTable}

The columns of a {@link dh.ResolvedTable} have the same {@link dh.Column.filter | filter} method, for use with
{@link dh.TableOperations.where | where}.
