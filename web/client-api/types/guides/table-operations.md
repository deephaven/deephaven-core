---
title: Table operations by example
---

# Table operations by example

The interface {@link dh.TableOperations} exposes methods for each table operation. Each returns a new
{@link dh.PendingTable} for a server-side table still being made, and awaiting it will give a {@link dh.ResolvedTable}
which represents the finished server side table, and can provide details about the table's definitions and its data.
Each example here shows an operation, awaits the work, then {@link dh.ResolvedTable#close | closes the table} when
finished.

The examples use an {@link dh.IdeSession} named `session`, and server tables named `source`, `left` and `right`. Several
await more than one table at once with {@link dh.resolveAll}, which closes the tables that arrived if any fails; see
[Several tables at once](#several-tables-at-once).

{@includeCode ../examples/use-cases.ts#createAndTransform}

[Reading data](read-data.md) covers snapshots, subscriptions and the other ways to read a table's data.

## Selecting and computing columns

{@link dh.TableOperations.view | view}, {@link dh.TableOperations.select | select},
{@link dh.TableOperations.updateView | updateView} and {@link dh.TableOperations.update | update} choose and compute
columns. They differ in which columns the new table keeps, and in when formulas are computed:

| | Keeps only the listed columns | Keeps the other columns, adding or replacing the listed ones |
|---|---|---|
| Computes a formula each time a value is read | `view` | `updateView` |
| Computes formulas up front and stores the results | `select` | `update` |

{@includeCode ../examples/use-cases.ts#selectColumns}

{@link dh.TableOperations.selectDistinct | selectDistinct} keeps one row for each distinct value of the given columns.

{@includeCode ../examples/use-cases.ts#selectDistinct}

## Filtering

{@link dh.TableOperations.where | where} takes conditions built with {@link dh.FilterValue}; [Filters](filters.md)
covers them.

{@includeCode ../examples/use-cases.ts#filterNamed}

{@link dh.TableOperations.whereIn | whereIn} and {@link dh.TableOperations.whereNotIn | whereNotIn} filter by the keys
in another table.

{@includeCode ../examples/use-cases.ts#filterByTable}

## Sorting

{@link dh.TableOperations.sort | sort} takes column names or {@link dh.Column}s for ascending order, or a
{@link dh.SimpleSort} or {@link dh.Sort} with a direction.

{@includeCode ../examples/use-cases.ts#tickingSorted}

## Head, tail, slice and reverse

{@link dh.TableOperations.head | head} and {@link dh.TableOperations.tail | tail} keep the first or last rows.

{@includeCode ../examples/use-cases.ts#headAndTail}

{@link dh.TableOperations.slice | slice} keeps rows by position, and {@link dh.TableOperations.reverse | reverse}
reverses their order.

{@includeCode ../examples/use-cases.ts#sliceAndReverse}

## Grouping and ungrouping

{@link dh.TableOperations.groupBy | groupBy} collects each group's values into arrays,
{@link dh.TableOperations.ungroup | ungroup} expands them, and {@link dh.TableOperations.flatten | flatten} renumbers
rows.

{@includeCode ../examples/use-cases.ts#groupAndUngroup}

## Merging tables

{@link dh.IdeSession.mergeTables | mergeTables} stacks tables with the same columns.

{@includeCode ../examples/use-cases.ts#mergeTwo}

## Joins

{@link dh.TableOperations.naturalJoin | naturalJoin} adds columns from the right row with the same key.

{@includeCode ../examples/use-cases.ts#naturalJoin}

{@link dh.TableOperations.exactJoin | exactJoin} requires exactly one right row for each left row.

{@includeCode ../examples/use-cases.ts#exactJoin}

{@link dh.TableOperations.join | join} pairs each left row with every right row with the same key.

{@includeCode ../examples/use-cases.ts#crossJoin}

{@link dh.TableOperations.aj | aj} and {@link dh.TableOperations.raj | raj} match the closest right row at or before, or
at or after, each left row.

{@includeCode ../examples/use-cases.ts#asOfJoins}

{@link dh.TableOperations.rangeJoin | rangeJoin} aggregates the right rows in each left row's
{@link dh.RangeJoinMatch | range}.

{@includeCode ../examples/use-cases.ts#rangeJoin}

## Aggregations

{@link dh.TableOperations.aggBy | aggBy} takes {@link dh.agg.AggByOptions}, with aggregations from
{@link dh.agg.Aggregation} or written as object literals. {@link dh.TableOperations.countBy | countBy} is shorthand for
{@link dh.TableOperations.aggBy | aggBy} with {@link dh.agg.Aggregation.AggCount | AggCount}.

{@includeCode ../examples/use-cases.ts#aggregate}

{@link dh.agg.Aggregation.AggUnique | AggUnique} shows the single unique value in a column, with a typed sentinel for
groups with more than one distinct value; a group of only nulls gives null.

{@includeCode ../examples/use-cases.ts#uniqueWithSentinel}

{@link dh.agg.Aggregation.AggWAvg | AggWAvg} weights one column's average by another.

{@includeCode ../examples/use-cases.ts#weightedAverages}

## Aggregating every column

{@link dh.TableOperations.aggAllBy | aggAllBy} applies one aggregation to every column but the keys.

{@includeCode ../examples/use-cases.ts#aggregateAll}

{@link dh.TableOperations.sumBy | sumBy}, {@link dh.TableOperations.wavgBy | wavgBy} and the other shorthands here are
{@link dh.TableOperations.aggAllBy | aggAllBy} with one aggregation.

{@includeCode ../examples/use-cases.ts#shorthandAggregations}

## Update-by

{@link dh.TableOperations.updateBy | updateBy} takes {@link dh.updateby.UpdateByOptions}; here a
{@link dh.updateby.UpdateByCumulativeSum} per key.

{@includeCode ../examples/use-cases.ts#updateBy}

A {@link dh.updateby.UpdateByRollingSum} over a window of rows.

{@includeCode ../examples/use-cases.ts#rollingUpdateBy}

A {@link dh.updateby.UpdateByRollingWAvg} weights each window's average by another column.

{@includeCode ../examples/use-cases.ts#rollingWeightedAverage}

## Snapshots

{@link dh.TableOperations.snapshot | snapshot} copies a table's current rows into one that does not tick.

{@includeCode ../examples/use-cases.ts#snapshot}

{@link dh.TableOperations.snapshotWhen | snapshotWhen} takes a copy each time a trigger table ticks, with
{@link dh.SnapshotWhenOptions}.

{@includeCode ../examples/use-cases.ts#snapshotWhen}

## Downsampling for charts

{@link dh.TableOperations.downsample | downsample} takes {@link dh.DownsampleTableOptions}.

{@includeCode ../examples/use-cases.ts#downsampleForChart}

## Several tables at once

{@link dh.resolveAll} closes the tables that arrived if any fails.

{@includeCode ../examples/use-cases.ts#severalThatMayFail}

`Promise.allSettled` keeps each outcome instead.

{@includeCode ../examples/use-cases.ts#keepWhatSucceeded}

## Failures and canceling

A pending table has no `catch`; use `try`/`await`.

{@includeCode ../examples/use-cases.ts#handleFailure}

{@link dh.PendingTable.cancel | cancel} abandons a pending table; {@link dh.PendingTable.resolve | resolve} takes a
signal in its {@link dh.ResolveOptions}.

{@includeCode ../examples/use-cases.ts#cancelAndAbort}

## Reading results

{@link dh.ResolvedTable.createViewportSubscription | createViewportSubscription} reads a window of rows as they change;
[Reading data](read-data.md) covers the other ways.

{@includeCode ../examples/use-cases.ts#viewport}

A function lent a {@link dh.ResolvedTable} reads it and closes only what it made.

{@includeCode ../examples/use-cases.ts#readTable}

## With the mutable Table

{@link dh.Table}, {@link dh.TotalsTable} and {@link dh.ResolvedTable} share their read members;
{@link dh.Table.operations | operations} chains on a {@link dh.Table} as it is now.
[Working with a dh.Table](mutable-table.md) covers the mutable table.

{@includeCode ../examples/use-cases.ts#readEither}

{@includeCode ../examples/use-cases.ts#mutableTable}
