---
title: Table operations by example
---

# Table operations by example

Draft scaffold: each section embeds one region of `examples/use-cases.ts`, which the docs build type-checks. Each
example's first comment is the Python equivalent.

## Chaining, then awaiting once

{@includeCode ../examples/use-cases.ts#createAndTransform}

## Selecting and computing columns

{@includeCode ../examples/use-cases.ts#selectColumns}

## Filtering

{@includeCode ../examples/use-cases.ts#filterNamed}

## Sorting

{@includeCode ../examples/use-cases.ts#tickingSorted}

## Tail, slice and reverse

{@includeCode ../examples/use-cases.ts#rowRanges}

## Grouping and ungrouping

{@includeCode ../examples/use-cases.ts#groupAndUngroup}

## Merging tables

{@includeCode ../examples/use-cases.ts#mergeTwo}

## Joins

{@includeCode ../examples/use-cases.ts#joins}

{@includeCode ../examples/use-cases.ts#whereInAndRangeJoin}

{@includeCode ../examples/use-cases.ts#moreJoins}

## Aggregations

{@includeCode ../examples/use-cases.ts#aggregate}

{@includeCode ../examples/use-cases.ts#uniqueWithSentinel}

{@includeCode ../examples/use-cases.ts#weightedAverages}

{@includeCode ../examples/use-cases.ts#shorthandAggregations}

{@includeCode ../examples/use-cases.ts#aggregateAll}

## Update-by

{@includeCode ../examples/use-cases.ts#updateBy}

{@includeCode ../examples/use-cases.ts#rollingUpdateBy}

## Head, distinct values and snapshots

{@includeCode ../examples/use-cases.ts#misc}

## Downsampling for charts

{@includeCode ../examples/use-cases.ts#downsampleForChart}

## Several tables at once

{@includeCode ../examples/use-cases.ts#severalThatMayFail}

{@includeCode ../examples/use-cases.ts#keepWhatSucceeded}

## Failures and canceling

{@includeCode ../examples/use-cases.ts#handleFailure}

{@includeCode ../examples/use-cases.ts#cancelAndAbort}

## Reading results

{@includeCode ../examples/use-cases.ts#viewport}

{@includeCode ../examples/use-cases.ts#readTable}

## With the mutable Table

{@includeCode ../examples/use-cases.ts#readEither}

{@includeCode ../examples/use-cases.ts#legacy}
