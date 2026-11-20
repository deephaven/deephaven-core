---
title: Table operations by example
---

# Table operations by example

{@link dh.TableOperations} need none of a table's data, so they chain without waiting: each returns a new
{@link dh.PendingTable} for one server-side table, and awaiting it gives a {@link dh.ResolvedTable} that its owner
closes when done. No operation throws; a failure rejects the pending table it returned.

## Chaining, then awaiting once

Each operation returns a {@link dh.PendingTable}, so operations chain without waiting; awaiting the last gives a
{@link dh.ResolvedTable}, read here with {@link dh.ResolvedTable.createSnapshot | createSnapshot}.

{@includeCode ../examples/use-cases.ts#createAndTransform}

## Selecting and computing columns

{@link dh.TableOperations.view | view}, {@link dh.TableOperations.select | select} and
{@link dh.TableOperations.updateView | updateView} choose and compute columns.

{@includeCode ../examples/use-cases.ts#selectColumns}

## Filtering

{@link dh.TableOperations.where | where} takes conditions built with {@link dh.FilterValue}.

{@includeCode ../examples/use-cases.ts#filterNamed}

## Sorting

{@link dh.TableOperations.sort | sort} takes column names for ascending order, or a {@link dh.SimpleSort} with a
direction.

{@includeCode ../examples/use-cases.ts#tickingSorted}

## Tail, slice and reverse

{@link dh.TableOperations.tail | tail}, {@link dh.TableOperations.slice | slice} and
{@link dh.TableOperations.reverse | reverse} select rows by position.

{@includeCode ../examples/use-cases.ts#rowRanges}

## Grouping and ungrouping

{@link dh.TableOperations.groupBy | groupBy} collects each group's values into arrays,
{@link dh.TableOperations.ungroup | ungroup} expands them, and {@link dh.TableOperations.flatten | flatten} renumbers
rows.

{@includeCode ../examples/use-cases.ts#groupAndUngroup}

## Merging tables

{@link dh.IdeSession.mergeTables | mergeTables} stacks tables with the same columns.

{@includeCode ../examples/use-cases.ts#mergeTwo}

## Joins

{@link dh.TableOperations.naturalJoin | naturalJoin}, {@link dh.TableOperations.aj | aj} and
{@link dh.TableOperations.exactJoin | exactJoin}, resolved together with {@link dh.resolveAll}.

{@includeCode ../examples/use-cases.ts#joins}

{@link dh.TableOperations.whereIn | whereIn} filters by another table's keys;
{@link dh.TableOperations.rangeJoin | rangeJoin} aggregates the right rows in each left row's
{@link dh.RangeJoinMatch | range}.

{@includeCode ../examples/use-cases.ts#whereInAndRangeJoin}

{@link dh.TableOperations.raj | raj}, {@link dh.TableOperations.join | join} and
{@link dh.TableOperations.whereNotIn | whereNotIn}.

{@includeCode ../examples/use-cases.ts#moreJoins}

## Aggregations

{@link dh.TableOperations.aggBy | aggBy} takes {@link dh.agg.AggByOptions}, with aggregations from
{@link dh.agg.Aggregation} or written as object literals. {@link dh.TableOperations.countBy | countBy} is shorthand
for {@link dh.TableOperations.aggBy | aggBy} with {@link dh.agg.Aggregation.AggCount | AggCount}.

{@includeCode ../examples/use-cases.ts#aggregate}

{@link dh.agg.Aggregation.AggUnique | AggUnique} with a typed sentinel for groups without one unique value.

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

## Head, distinct values and snapshots

{@link dh.TableOperations.head | head}, {@link dh.TableOperations.selectDistinct | selectDistinct},
{@link dh.TableOperations.snapshot | snapshot} and {@link dh.TableOperations.snapshotWhen | snapshotWhen} with
{@link dh.SnapshotWhenOptions}.

{@includeCode ../examples/use-cases.ts#misc}

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

{@link dh.ResolvedTable.createViewportSubscription | createViewportSubscription} reads a window of rows as they change.

{@includeCode ../examples/use-cases.ts#viewport}

A function lent a {@link dh.ResolvedTable} reads it and closes only what it made.

{@includeCode ../examples/use-cases.ts#readTable}

## With the mutable Table

{@link dh.Table}, {@link dh.TotalsTable} and {@link dh.ResolvedTable} share their read members;
{@link dh.Table.operations | operations} chains on a {@link dh.Table} as it is now.

{@includeCode ../examples/use-cases.ts#readEither}

{@includeCode ../examples/use-cases.ts#legacy}
