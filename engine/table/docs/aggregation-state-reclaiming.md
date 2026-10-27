# Reclaiming aggregation states

An incremental aggregation keeps one state per group. This document describes how a refreshing keyed aggregation
reclaims the states of groups whose rows have all been removed, the modes that control it, and their parameters. It is
written for contributors to the engine.

## Terms

- **State:** the per-group storage of an aggregation — its entry in the hash table plus one cell in each result column.
- **Output position:** the index of a state's cells in the result columns. It is also the state's row key in the result
  table.
- **Block:** a run of `ArrayBackedColumnSource.BLOCK_SIZE` (2048) output positions. The array-backed column sources
  allocate storage one block at a time.
- **Closed block:** a block whose positions have all been assigned to states. New states are only ever assigned after
  every existing state, so no state is ever assigned to a closed block again.
- **Released block:** a closed block whose states have all been removed, and whose storage has been freed.
- **Tombstone:** a hash table slot whose state has been removed. It keeps its key so that probes stay correct, and a
  rehash drops it.

## What reclaiming changes

Without reclaiming, a group that empties keeps its state and its output position. A group that returns reuses that
state. Memory therefore grows with every group ever seen, not with the groups that currently have rows.

With reclaiming, a state that is empty at the end of a cycle leaves the result and the hash table. Every reclaiming
mode keeps these rules:

- **New states go after every existing state.** A group that returns on a later cycle is a new state at a new output
  position, after every group that has rows. States are never placed into free positions in the middle of the result.
- **Moves keep the order of the states.** When the collapse moves states to free blocks, the states keep their
  relative order. Downstream listeners see the moves as the shifts of a `TableUpdate`.
- **Output positions are never given back.** Blocks are released in place, so the positions assigned grow with every
  group ever created. See [Costs and limits](#costs-and-limits) for what bounds them.
- **A group keeps its row key while it has rows,** unless the mode collapses blocks. A group that empties and returns
  is a new row, at the end of the result. This differs from the encounter-order promise of an aggregation that does not
  reclaim.
- **Storage is released after the cycle.** A block's storage is freed by a terminal notification, once every listener
  in the cycle has run, so previous values stay readable for the whole cycle that removed the states. The array sources
  return a released block's storage to their element type's recycler, where a later block borrows it rather than
  allocating one.

## Modes

`StateReclaimMode` selects the mode. The public `aggBy` family uses `StateReclaimMode.configured()`, which reads the
[configuration properties](#parameters) when the aggregation is created. Engine code can choose a mode for a single
aggregation — see [Choosing a mode per call](#choosing-a-mode-per-call).

### None

`StateReclaimMode.none()` never removes a state. An empty state keeps its output position, and a returning group
reuses it. This is how aggregations behaved before reclaiming existed. It never tombstones a state.

### Release blocks

`StateReclaimMode.releaseBlocks(collapseFreeFraction)` frees the storage of each closed block once every state in it
has been removed. With the default parameter, no state ever moves:

- The result columns' storage tracks the groups that have rows, but only at block granularity. A block that holds even
  one long-lived state is never released, so random churn with some long-lived groups releases few blocks. The hash
  table does not shrink (see [Costs and limits](#costs-and-limits)), so it stays sized for the most groups the
  aggregation has had at once.
- Output positions are never given back, so the positions assigned grow with every group ever created.

The **collapse** (`collapseFreeFraction` below 1) releases blocks that random churn would otherwise leave nearly empty.
A closed block at least this fraction free, and with at least one position free, is sparse; a full block never is. A run
is two or more sparse blocks with nothing but released blocks between them. A run is collapsed by packing its live
states into its sparse blocks, first to last, keeping their order, and releasing the sparse blocks this empties. Nothing
moves onto a released block. A run collapses only if that frees at least one block. The live states moved are limited by
a credit, to which each cycle adds its input rows added, modified, and removed. The runs are collapsed in order while the
credit covers them; the first run it does not cover is collapsed as far as the credit reaches, and then it and the runs
after it wait. While a run waits, the unused credit carries over to the next cycle, so a run larger than one cycle's
input rows is collapsed after enough cycles instead of blocking every run behind it forever; once no run waits, the
credit is dropped, so quiet cycles do not save up for a burst. Over any span of cycles the states moved are no more than
their input rows, although the cycle that collapses a waiting run may move the input rows of the cycles it waited
through. The collapse frees memory but does not give output positions back.

## Parameters

The configuration properties set the mode that `StateReclaimMode.configured()` returns. It reads them as each
aggregation is created, so changing them affects only aggregations created afterward. Tests change them through the
matching public static fields of `ChunkedOperatorAggregationHelper`.

| Property | Field | Default | Meaning |
| --- | --- | --- | --- |
| `ChunkedOperatorAggregationHelper.reclaimStates` | `RECLAIM_STATES` | `true` | Whether states are reclaimed at all. `false` selects `none`. |
| `ChunkedOperatorAggregationHelper.collapseFreeFraction` | `COLLAPSE_FREE_FRACTION` | `1.0` | The fraction free at which a closed block is sparse and may be collapsed. 1 or more never collapses. |

The defaults select `releaseBlocks(1)`: blocks are released as they empty and no state moves. The fraction must not be
`NaN`; `releaseBlocks` rejects one.

## Choosing a mode per call

Engine code that builds an aggregation can choose a mode for that aggregation alone:

- `ChunkedOperatorAggregationHelper.aggregation(control, factory, input, preserveEmpty, initialKeys, reclaimMode,
  groupByColumns)`
- `QueryTable.aggNoMemo(factory, preserveEmpty, initialGroups, groupByColumns, reclaimMode)`

A consumer that looks up a group's current row key and then reads previous values at that row needs a mode that never
moves states. The tree table's source row lookup uses `none` for this reason, and the table-backed data index uses
`none` as well.

## When a mode applies

A mode other than `none` can apply only when all of the following hold:

- Every operator of the aggregation can reclaim states (`IterativeChunkedAggregationOperator.canReclaimStates()`, false
  by default). Group-by, partition-by, formula, and rollup operators cannot. Neither can the operators whose states
  never become empty: approximate percentiles, whose t-digests cannot remove values, and the min, max, first, and last
  operators used only for add-only and blink input, which are accepted with any mode as described below.
- The aggregation does not preserve empty groups.
- The aggregation has no initial groups.

What happens when they do not hold depends on where the mode came from:

- **The configured mode** (`StateReclaimMode.configured()`, which the public `aggBy` family uses) degrades silently: the
  aggregation behaves as with `none`.
- **A mode chosen explicitly** (`releaseBlocks(...)` rather than `configured()`) fails the
  aggregation with an `IllegalArgumentException` naming the reason — for example, the result columns whose operators
  cannot reclaim states. `StateReclaimMode.isConfigured()` tells the two apart.

Static, add-only, append-only, and blink input never reclaims states, since no state is ever removed (a blink
aggregation ignores its input's removals), so any mode is accepted for it and none of these conditions is checked.

## Costs and limits

- **Returning groups cost more.** A group that empties and returns is a new state, and a new row at the end of the
  result, where `none` would reuse its state. Workloads whose groups keep returning run slower with reclaiming.
- **Output positions are ints.** No mode gives positions back, so every reclaiming mode fails with `Aggregation output
  positions exhausted` once 2^31 - 1 states have been created over the life of the aggregation. At 10,000 new groups
  per second that is about 2.5 days. Rather than moving states to keep positions low, which was tried and rejected (see
  [Block shifting](#block-shifting)), an aggregation that needs more can migrate to `long` output positions (see
  [Long output positions](#long-output-positions)).
- **The hash table does not shrink.** Tombstones keep their keys until a rehash drops them. Growth is driven by the live
  states: when tombstones alone cross the load factor, the table rehashes at the same size.
- **Key columns track previous values only when states can move.** The copied key columns start tracking previous
  values when the mode collapses, but not for release blocks without moves.

## Benchmark results

`AggregationIncrementalBenchmark` in `engine/benchmark` measures batches of 900 update cycles of a keyed `sumBy` over a
table of 1,000,000 rows, with 10,000 rows added and removed each cycle:

- **Add only:** rows are only added, each with a new group.
- **Sliding window:** each cycle removes the oldest rows, so groups empty in the order they were created.
- **Random churn:** each cycle removes rows chosen at random, so blocks thin out unevenly.

Each group owns one row or 100 consecutive rows. With one row per group every removal empties a group and every
addition creates one; with 100, most changes add rows to or remove rows from groups that already exist. A group
"returns" when its key comes back after its state has been removed; with returning groups, the keys cycle through a
space twice the number of groups in the table.

The same results are charted in
[aggregation-state-reclaiming-benchmarks.html](aggregation-state-reclaiming-benchmarks.html); open that file in a
browser, since GitHub shows only its source.

The columns:

- **Baseline:** main at eb84801525 with the adaptive `SoftRecycler` of
  [#8698](https://github.com/deephaven/deephaven-core/pull/8698), which is not part of this branch's reclaiming but
  speeds up every aggregation. Main already has two other changes from this work: not recording previous values for
  blocks allocated during an update cycle, from [#8713](https://github.com/deephaven/deephaven-core/pull/8713), and
  releasing the drained alternate hash table's keys once a rehash completes, from
  [#9000](https://github.com/deephaven/deephaven-core/pull/9000). Every percentage is the change against this column.
- **`none`:** this branch without reclaiming, which isolates its other improvements to the update cycle.
- **Blocks:** `releaseBlocks(1)`, the default, which releases blocks and never moves a state.
- **Collapse 0.75** and **Collapse 0.5:** `releaseBlocks(0.75)` and `releaseBlocks(0.5)`, which also collapse runs of
  sparse blocks.
- **Baseline again:** the baseline once more, after every other configuration, to show any drift over the run.

The branch was measured at 615ff26e7e, which merges the same main, with #8698 merged into it. Both builds therefore
have all three changes, and the columns differ only by this branch's own work.

The numbers come from an Intel i9-14900KS (hybrid performance and efficiency cores) in an LXC container with 24 logical
CPUs and 128 GB, with the performance governor and turbo boost enabled. Nothing else ran on it except idle Gradle
daemons from another session, holding about 25 GB, which exited at some point during the run. Each configuration ran
two JVM forks of three warmup and five measured batches; the error bounds are JMH's 99.9% intervals over the ten
measured batches.

Baseline and Baseline again are within the error of each other in every workload at one million rows. At ten million
rows, Baseline again was 5% to 6% faster than Baseline for the sliding window and for random churn with returning
groups, in both forks of each. Baseline and `none` ran while the one-minute load average was at times around 3; the
cause was not identified. The findings give the ten million row changes against both.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 1410 ± 82 | 1337 ± 83 (−5%) | 1307 ± 73 (−7%) | 1278 ± 115 (−9%) | 1338 ± 105 (−5%) | 1356 ± 56 (−4%) |
| Sliding window | 1 | No | 2047 ± 83 | 1928 ± 87 (−6%) | 1327 ± 67 (−35%) | 1366 ± 30 (−33%) | 1325 ± 34 (−35%) | 2076 ± 77 (+1%) |
| Sliding window | 1 | Yes | 1116 ± 59 | 1051 ± 30 (−6%) | 1236 ± 77 (+11%) | 1242 ± 55 (+11%) | 1288 ± 39 (+15%) | 1128 ± 65 (+1%) |
| Random churn | 1 | No | 3934 ± 46 | 3465 ± 106 (−12%) | 3294 ± 86 (−16%) | 3503 ± 101 (−11%) | 3596 ± 88 (−9%) | 3856 ± 69 (−2%) |
| Random churn | 1 | Yes | 2957 ± 80 | 2630 ± 69 (−11%) | 3309 ± 91 (+12%) | 3344 ± 52 (+13%) | 3403 ± 77 (+15%) | 2945 ± 110 (−0%) |
| Add only | 100 | — | 187 ± 27 | 164 ± 20 (−12%) | 184 ± 27 (−1%) | 174 ± 26 (−7%) | 204 ± 29 (+9%) | 179 ± 25 (−4%) |
| Sliding window | 100 | No | 304 ± 35 | 296 ± 39 (−3%) | 283 ± 36 (−7%) | 284 ± 35 (−7%) | 285 ± 30 (−6%) | 313 ± 38 (+3%) |
| Sliding window | 100 | Yes | 319 ± 37 | 298 ± 34 (−7%) | 342 ± 52 (+7%) | 329 ± 32 (+3%) | 324 ± 37 (+1%) | 325 ± 36 (+2%) |
| Random churn | 100 | No | 838 ± 70 | 796 ± 39 (−5%) | 814 ± 57 (−3%) | 813 ± 68 (−3%) | 847 ± 53 (+1%) | 797 ± 52 (−5%) |
| Random churn | 100 | Yes | 765 ± 48 | 792 ± 62 (+3%) | 795 ± 61 (+4%) | 780 ± 53 (+2%) | 779 ± 45 (+2%) | 776 ± 70 (+1%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection, averaged over the measured
batches. It includes the source table and the benchmark's own data, and the blocks the recyclers hold, so compare the
modes with each other rather than reading the values as the aggregation's size.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 675 | 571 (−15%) | 572 (−15%) | 578 (−14%) | 579 (−14%) | 674 (−0%) |
| Sliding window | 1 | No | 680 | 579 (−15%) | 186 (−73%) | 186 (−73%) | 186 (−73%) | 679 (−0%) |
| Sliding window | 1 | Yes | 149 | 149 (−0%) | 130 (−13%) | 130 (−13%) | 130 (−13%) | 149 (+0%) |
| Random churn | 1 | No | 728 | 627 (−14%) | 484 (−34%) | 274 (−62%) | 246 (−66%) | 729 (+0%) |
| Random churn | 1 | Yes | 188 | 188 (+0%) | 453 (+141%) | 218 (+16%) | 194 (+3%) | 188 (+0%) |
| Add only | 100 | — | 28 | 28 (+2%) | 28 (+0%) | 32 (+12%) | 32 (+14%) | 28 (+0%) |
| Sliding window | 100 | No | 29 | 29 (−1%) | 26 (−12%) | 26 (−10%) | 26 (−10%) | 29 (+0%) |
| Sliding window | 100 | Yes | 21 | 21 (+0%) | 25 (+19%) | 25 (+19%) | 25 (+19%) | 21 (+2%) |
| Random churn | 100 | No | 51 | 52 (+2%) | 48 (−6%) | 54 (+5%) | 54 (+6%) | 52 (+1%) |
| Random churn | 100 | Yes | 43 | 42 (−2%) | 43 (+0%) | 47 (+10%) | 47 (+10%) | 43 (−0%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over the ten measured batches. A single worst sample is
noisy, as the difference between Baseline and Baseline again shows, so read the percentages for the modes whose longest
cycles are consistently high rather than for small differences. With 100 rows per group every longest cycle is a few
milliseconds, where one slow sample swings the percentage widely.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 69.5 | 62.3 (−10%) | 61.8 (−11%) | 61.9 (−11%) | 71.0 (+2%) | 61.0 (−12%) |
| Sliding window | 1 | No | 62.6 | 62.3 (−0%) | 21.1 (−66%) | 19.9 (−68%) | 17.2 (−72%) | 63.6 (+2%) |
| Sliding window | 1 | Yes | 14.0 | 19.0 (+35%) | 18.8 (+34%) | 14.7 (+5%) | 14.3 (+2%) | 15.8 (+13%) |
| Random churn | 1 | No | 51.0 | 39.6 (−22%) | 22.1 (−57%) | 23.9 (−53%) | 10.7 (−79%) | 51.0 (−0%) |
| Random churn | 1 | Yes | 12.6 | 16.5 (+30%) | 13.7 (+8%) | 14.0 (+11%) | 13.3 (+5%) | 15.4 (+22%) |
| Add only | 100 | — | 1.5 | 1.1 (−25%) | 1.3 (−13%) | 1.1 (−25%) | 1.5 (−2%) | 1.0 (−36%) |
| Sliding window | 100 | No | 1.6 | 1.4 (−10%) | 0.9 (−42%) | 1.9 (+21%) | 1.1 (−33%) | 1.4 (−9%) |
| Sliding window | 100 | Yes | 1.5 | 1.0 (−30%) | 0.9 (−38%) | 1.3 (−12%) | 1.5 (+1%) | 2.9 (+100%) |
| Random churn | 100 | No | 3.4 | 3.5 (+2%) | 3.5 (+2%) | 3.0 (−12%) | 3.1 (−8%) | 2.7 (−20%) |
| Random churn | 100 | Yes | 2.6 | 2.4 (−5%) | 2.8 (+8%) | 2.6 (+1%) | 2.6 (+1%) | 2.8 (+9%) |

### Ten million rows

The same workloads with one row per group over a table of 10,000,000 rows, with 100,000 rows added and removed each
cycle, run with a 48 GB heap.

Milliseconds per batch of 900 cycles:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 15911 ± 437 | 15008 ± 187 (−6%) | 14809 ± 288 (−7%) | 14340 ± 145 (−10%) | 14408 ± 411 (−9%) | 16022 ± 562 (+1%) |
| Sliding window | No | 23552 ± 842 | 22639 ± 244 (−4%) | 16387 ± 111 (−30%) | 16423 ± 199 (−30%) | 16379 ± 205 (−30%) | 22251 ± 313 (−6%) |
| Sliding window | Yes | 13252 ± 147 | 12279 ± 111 (−7%) | 13904 ± 443 (+5%) | 14293 ± 104 (+8%) | 14029 ± 134 (+6%) | 12647 ± 207 (−5%) |
| Random churn | No | 43856 ± 341 | 40289 ± 384 (−8%) | 36974 ± 549 (−16%) | 37117 ± 283 (−15%) | 37937 ± 734 (−13%) | 43475 ± 560 (−1%) |
| Random churn | Yes | 32486 ± 322 | 27401 ± 262 (−16%) | 35360 ± 628 (+9%) | 34773 ± 332 (+7%) | 35552 ± 467 (+9%) | 30422 ± 397 (−6%) |

Retained heap, in megabytes:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 5079 | 5079 (−0%) | 5078 (−0%) | 5089 (+0%) | 5091 (+0%) | 5077 (−0%) |
| Sliding window | No | 5116 | 5145 (+1%) | 898 (−82%) | 899 (−82%) | 899 (−82%) | 5113 (−0%) |
| Sliding window | Yes | 1170 | 1180 (+1%) | 898 (−23%) | 898 (−23%) | 898 (−23%) | 1171 (+0%) |
| Random churn | No | 5620 | 5647 (+0%) | 4326 (−23%) | 2208 (−61%) | 1927 (−66%) | 5621 (+0%) |
| Random churn | Yes | 1547 | 1552 (+0%) | 4116 (+166%) | 1752 (+13%) | 1510 (−2%) | 1545 (−0%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 496.3 | 505.0 (+2%) | 518.6 (+4%) | 507.6 (+2%) | 479.4 (−3%) | 502.9 (+1%) |
| Sliding window | No | 512.9 | 526.0 (+3%) | 100.2 (−80%) | 96.6 (−81%) | 87.1 (−83%) | 523.8 (+2%) |
| Sliding window | Yes | 144.8 | 96.6 (−33%) | 138.6 (−4%) | 92.5 (−36%) | 114.8 (−21%) | 119.6 (−17%) |
| Random churn | No | 523.8 | 403.7 (−23%) | 135.1 (−74%) | 88.8 (−83%) | 91.3 (−83%) | 524.4 (+0%) |
| Random churn | Yes | 131.9 | 79.4 (−40%) | 80.4 (−39%) | 86.0 (−35%) | 75.6 (−43%) | 130.0 (−1%) |

### Findings

- This branch without reclaiming (`none`) is 12% faster than the baseline for random churn with one row per group when
  groups do not return, and 11% faster when they do. At ten million rows those are 7% to 8% and 10% to 16%, the ranges
  spanning Baseline and Baseline again, and `none` is also 6% faster for add only and 3% to 7% faster for the sliding
  window with returning groups. The baseline already has #8698, #8713, and #9000, so this comes from the branch's other
  changes to the update cycle and the state manager; which one has not been isolated. Elsewhere it is within the error.
- At one million rows, `none` retains about 100 MB less heap than the baseline (14% to 15%) in the three workloads
  whose groups do not return, because its rehash has already released the old hash table when the heap is measured (see
  [A separate state manager for reclaiming](#a-separate-state-manager-for-reclaiming)). At ten million rows the two are
  the same.
- When groups do not return, every reclaiming mode is faster than the baseline for the sliding window, by 33% to 35%,
  and retains about a quarter of its heap; at ten million rows, 26% to 30% faster with a sixth of the heap. For random
  churn, releasing blocks alone is 16% faster and retains a third less heap; collapsing is 9% to 11% faster and retains
  34% to 38% of the heap. At ten million rows, releasing blocks alone is 15% to 16% faster and collapsing 13% to 15%,
  and add only is 7% to 10% faster with reclaiming, most of which `none` shares.
- When groups return, every reclaiming mode is slower, since returning groups are new states rather than reused ones.
  The sliding window is 11% slower releasing blocks alone, within the error, and 11% to 15% slower collapsing; at ten
  million rows, 5% to 8% slower against Baseline and 10% to 13% against Baseline again. Random churn with returning
  groups is 12% slower releasing blocks alone and 13% to 15% slower collapsing; at ten million rows, 7% to 9% slower
  against Baseline and 14% to 17% against Baseline again. Against this branch's own `none`, which also reuses states,
  the reclaiming modes cost 26% to 30% there, at both sizes.
- With returning groups, releasing blocks alone retains 2.4 times the heap of the baseline for random churn (2.7 times
  at ten million rows), and collapsing 3% to 16% more (at ten million rows, 2% less to 13% more). For the sliding
  window, every reclaiming mode retains 13% less (23% less at ten million rows).
- An earlier run measured against main without #8698 and #8713, and there random churn with returning groups at ten
  million rows was 8% to 10% faster with reclaiming. That gain was the two changes', not reclaiming's.
- For random churn, releasing blocks alone is 6% to 9% faster than collapsing when groups do not return, and 1% to 3%
  faster when they do; at ten million rows, they are within 3% of each other either way. Collapsing retains far less
  heap.
- No mode gives output positions back, so every reclaiming mode assigns the same positions: with one row per group,
  10 million when groups do not return, and 8.71 to 10 million when they do, where the baseline reuses 2 million; with
  100 rows per group, 100,000, or 20,000 for random churn with returning groups.
- With 100 rows per group, every configuration is within the error of the baseline for time.
- Reclaiming shortens the longest cycles when groups do not return: 17 to 21 ms for the sliding window and 11 to 24 ms
  for random churn, against 51 to 64 ms for the baseline and baseline again; at ten million rows, 87 to 100 ms and 89
  to 135 ms, against 513 to 524 ms. When groups return, the longest cycles are 13 to 19 ms for the baseline and every
  mode, and 76 to 145 ms at ten million rows.

## Long output positions

Output positions are `int`s, and no mode gives them back, so an aggregation runs out after 2^31 - 1 states (see
[Costs and limits](#costs-and-limits)). Storing them as `long`s would remove the limit without moving states to give
positions back, which was tried and rejected (see [Block shifting](#block-shifting)): releasing blocks as they empty
bounds memory, and positions can keep increasing. Three experiments, not part of this change, measured what `long`
positions cost. They were run while block shifting was still a parameter of release blocks, so the modes below are
written with its parameters.

### What changed in the experiments

The two places that hold positions were widened separately:

- **The hash table's positions:** the incremental state manager stores each slot's output position as a `long`, in both
  the main and the alternate table, with `long` sentinels. Its generated hashers use a `long` state type.
- **Everything downstream of the hash table:**
  - the destination chunk every aggregation operator receives, `LongChunk<RowKeys>` instead of `IntChunk<RowKeys>`,
    in 149 operator files;
  - the state managers' output position chunks;
  - the helper's shared position counter, its sort and run finding over destinations, and the initial state and
    capacity counts;
  - the block tracker's position arguments.

The experiments cover three of the four combinations:

| | `int` hash table | `long` hash table |
| --- | --- | --- |
| **`int` downstream** | this change | the first experiment |
| **`long` downstream** | the third experiment | the second experiment, a complete conversion |

Even the complete conversion keeps some `int`s:

- The block tracker's block index and the modified-states bitmap's word index, which limit positions to 2^42 and 2^37.
- `findPositionForKey` and `AggregationRowLookup`, which return `int` positions.
- The hash tables that never reclaim store `int` positions, since they never exceed them.

Releasing blocks also leaves each array-backed source's block array as long as the highest position ever assigned:
one reference per 2048 positions, most of them empty. Positions that grow without limit would eventually need a sparse
structure for the blocks.

### Results

The same benchmarks as [Benchmark results](#benchmark-results), one row per group, comparing this change's `int`
positions with each experiment; milliseconds per batch of 900 cycles. An asterisk marks a difference outside the error
bounds. Unless a table says otherwise, the numbers come from an Apple silicon Mac.

**The complete conversion, without reclaiming.** Both position domains are widened, but without reclaiming the
aggregation uses a hash table that never reclaims, which keeps `int` positions. So only the downstream positions are
wider in this benchmark, and nothing measurable changes:

| Size | Workload | Groups return | `int` | `long` | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 90 ± 18 | 94 ± 18 | +4% | 103 → 103 |
| 1M | Random churn | No | 217 ± 40 | 214 ± 27 | −1% | 110 → 110 |
| 10M | Add only | — | 948 ± 76 | 961 ± 102 | +1% | 673 → 674 |
| 10M | Sliding window | No | 1437 ± 78 | 1436 ± 43 | −0% | 680 → 682 |
| 10M | Random churn | No | 2582 ± 79 | 2622 ± 61 | +2% | 718 → 717 |
| 10M | Random churn | Yes | 1611 ± 65 | 1559 ± 104 | −3% | 217 → 218 |
| 100M | Add only | — | 12300 ± 755 | 12230 ± 1624 | −1% | 5617 → 5623 |
| 100M | Random churn | No | 30299 ± 12134 | 30175 ± 1882 | −0% | 5925 → 5932 |

**The complete conversion, releasing blocks with no moves** (`releaseBlocks(1, -1, false)`, now `releaseBlocks(1)`).
The hash table's positions are wider too:

| Size | Workload | Groups return | `int` | `long` | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 95 ± 21 | 104 ± 37 | +9% | 83 → 91 |
| 1M | Random churn | No | 213 ± 52 | 220 ± 20 | +4% | 58 → 62 |
| 10M | Add only | — | 849 ± 37 | 968 ± 30 | +14% * | 569 → 632 |
| 10M | Sliding window | No | 878 ± 49 | 939 ± 51 | +7% | 175 → 208 |
| 10M | Sliding window | Yes | 724 ± 46 | 759 ± 50 | +5% | 119 → 136 |
| 10M | Random churn | No | 2419 ± 103 | 2457 ± 161 | +2% | 467 → 500 |
| 10M | Random churn | Yes | 2441 ± 93 | 2485 ± 224 | +2% | 433 → 448 |
| 100M | Add only | — | 11340 ± 1511 | 11722 ± 1724 | +3% | 5072 → 5588 |
| 100M | Sliding window | No | 10815 ± 690 | 11336 ± 1357 | +5% | 887 → 1014 |
| 100M | Random churn | No | 29504 ± 1626 | 30508 ± 1765 | +3% | 4053 → 4312 |

**The third experiment, releasing blocks with no moves.** Downstream positions are `long` and the hash table's stay
`int`. Nothing measurable changes, including the heap:

| Size | Workload | Groups return | `int` | `long` downstream | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 91 ± 12 | 94 ± 7 | +3% | 83 → 83 |
| 1M | Random churn | No | 218 ± 56 | 220 ± 20 | +1% | 59 → 59 |
| 10M | Add only | — | 924 ± 50 | 859 ± 90 | −7% | 571 → 570 |
| 10M | Sliding window | No | 822 ± 27 | 872 ± 59 | +6% | 175 → 175 |
| 10M | Sliding window | Yes | 759 ± 182 | 724 ± 22 | −5% | 120 → 119 |
| 10M | Random churn | No | 2396 ± 101 | 2409 ± 177 | +1% | 468 → 469 |
| 10M | Random churn | Yes | 2448 ± 118 | 2409 ± 104 | −2% | 433 → 433 |
| 100M | Add only | — | 11465 ± 3351 | 11566 ± 1643 | +1% | 5076 → 5079 |
| 100M | Sliding window | No | 10909 ± 753 | 11082 ± 985 | +2% | 886 → 884 |
| 100M | Random churn | No | 29766 ± 1089 | 30058 ± 3970 | +1% | 4054 → 4059 |

**The first experiment, on a second machine.** Only the hash table's positions are wider. These numbers come from a
Linux container on an Intel Core i9-14900KS with 128 GB of RAM, which is slower than the Mac in absolute terms:

| Size | Mode | Workload | Groups return | `int` | `long` hash table | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10M | `releaseBlocks(1, -1, false)` | Add only | — | 1446 ± 212 | 1654 ± 278 | +14% | 570 → 634 |
| 10M | `releaseBlocks(1, -1, false)` | Sliding window | No | 1528 ± 289 | 1775 ± 254 | +16% | 186 → 218 |
| 10M | `releaseBlocks(1, -1, false)` | Random churn | No | 4242 ± 389 | 4458 ± 155 | +5% | 473 → 505 |
| 10M | `releaseBlocks(0.5, 0, true)` | Add only | — | 1482 ± 261 | 1639 ± 147 | +11% | 578 → 641 |
| 10M | `releaseBlocks(0.5, 0, true)` | Sliding window | No | 1683 ± 184 | 1826 ± 195 | +9% | 194 → 226 |
| 10M | `releaseBlocks(0.5, 0, true)` | Random churn | No | 4015 ± 54 | 4180 ± 377 | +4% | 238 → 270 |
| 100M | `releaseBlocks(1, -1, false)` | Add only | — | 13746 ± 6732 | 14964 ± 2324 | +9% | 5068 → 5596 |
| 100M | `releaseBlocks(1, -1, false)` | Sliding window | No | 16183 ± 1638 | 16967 ± 358 | +5% | 896 → 1025 |
| 100M | `releaseBlocks(1, -1, false)` | Random churn | No | 49006 ± 5216 | 48964 ± 3746 | −0% | 4059 → 4314 |
| 100M | `releaseBlocks(0.5, 0, true)` | Add only | — | 14118 ± 3163 | 15585 ± 846 | +10% | 5087 → 5607 |
| 100M | `releaseBlocks(0.5, 0, true)` | Random churn | No | 44588 ± 2668 | 44728 ± 1234 | +0% | 1701 → 1957 |

`releaseBlocks(0.5, 0, true)` collapsed at 0.5 and shifted blocks in bulk.

The initial build of 10M rows, with reclaiming:

- The complete conversion was within 5% either way without reclaiming, and 6% (`String` keys) to 10% (`long` keys)
  slower at 5 million keys with reclaiming.
- The third experiment was within 3% of `int` at 5 million keys.
- On the second machine, the first experiment was 5% slower at 5 million keys, and within 2% at 100,000.

### Findings

- Widening the positions downstream of the hash table costs nothing measurable, with or without reclaiming. The whole
  cost is the hash table's slots growing from 4 to 8 bytes: 2% to 16% more time, most where adding states dominates
  and least for random churn, and 5% to 15% more retained heap. Both machines agree.
- `long` positions everywhere would pay that cost in every aggregation that reclaims, to remove a limit that few
  reach.
- A hybrid would pay it only when needed: `long` positions everywhere except the hash table, which stores `int`
  positions until they reach a threshold such as 2^30 and is then converted once, as part of a rehash, to a hasher that
  stores `long`s. The third experiment shows that below the threshold this costs nothing, and with the downstream
  positions already `long`, the conversion is confined to the hash table.

The numbers come from one JVM fork per configuration, with five measured batches each, or three at 100M; treat
differences within the error bounds as noise.

## Rejected Alternatives

These designs were implemented or measured and then rejected.

### Block shifting

Block shifting moved whole blocks of states down over released blocks, keeping the states in order, so that output
positions were given back as well as memory. It was a pair of parameters of release blocks:

- `blockShiftFraction` started a shift once the released blocks were at least this fraction of the positions assigned,
  so that many blocks did not shift to fill very small holes.
- `bulkShift` chose how the shift proceeded. Without it, the shift swept toward the end over several cycles, each
  moving no more live states than its input rows, and gave the released blocks back when it reached the end. With it,
  the shift waited until it could move every block after the first released one in one cycle, paid for by a credit that
  each cycle's added and removed states earned and that carried across cycles.

A sweeping shift chases a tail that grows with each cycle's new groups, and it moves blocks near the front that empty
soon after, so for a sliding window with `L` live groups the positions assigned peaked near `3 L`. The bulk shift ran
whenever the credit reached `L`, so the positions assigned stayed near `1.5 L`, but it concentrated the work: every live
state after the first released block moved in one cycle.

Its benefit was lower output positions. Its costs:

- **Latency.** The bulk shift had the longest cycles for random churn, 19 to 42 ms against 8 to 15 ms for
  releasing blocks alone or the sweeping shift.
- **Memory.** Every shifting mode retained slightly more heap than releasing blocks alone for the sliding window,
  180 to 191 MB against 175 MB.
- **Code.** The array sources moved whole blocks by reference, completed a block's previous values before moving it,
  allocated destination blocks that an earlier move had left unallocated, and shrank their capacity when blocks were
  released through the end. The helper composed the collapse and the shift into one shift for downstream listeners,
  and the state manager updated the hash table for every live state in the moved blocks.

The positions it kept low matter only for the `int` limit of 2^31 - 1 states created over the life of an aggregation.
Rather than moving states to stay under it, an aggregation that needs more can migrate to `long` output positions: the
[Long output positions](#long-output-positions) experiments show that `long` positions downstream of the hash table
cost nothing, and a hash table that converts to `long` positions at a threshold pays their cost only once it is
needed.

Measured with the benchmark described in [Benchmark results](#benchmark-results), one row per group, on an Apple silicon
Mac, with percentages against main with #8676, which was not yet merged. The Blocks column releases blocks without
moving any state. The shifting modes were `releaseBlocks(0.75, 0, false)`, which collapsed and swept (Collapse, sweep);
`releaseBlocks(1, 0, true)`, which shifted in bulk without collapsing (Bulk); and `releaseBlocks(0.75, 0, true)` and
`releaseBlocks(0.5, 0, true)`, which collapsed and shifted in bulk. The last two columns collapse without shifting, from
a later run on the same Mac.

Time, in milliseconds per batch of 900 cycles:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 949 ± 116 | 873 ± 83 (−8%) | 922 ± 137 (−3%) | 950 ± 32 (+0%) | 917 ± 65 (−3%) | 922 ± 43 (−3%) | 891 ± 43 (−6%) | 986 ± 141 (+4%) |
| Sliding window | No | 1367 ± 87 | 822 ± 43 (−40%) | 907 ± 77 (−34%) | 968 ± 128 (−29%) | 906 ± 16 (−34%) | 926 ± 70 (−32%) | 867 ± 116 (−37%) | 860 ± 30 (−37%) |
| Sliding window | Yes | 595 ± 30 | 726 ± 90 (+22%) | 890 ± 51 (+50%) | 784 ± 56 (+32%) | 810 ± 47 (+36%) | 871 ± 22 (+46%) | 763 ± 59 (+28%) | 753 ± 24 (+27%) |
| Random churn | No | 2827 ± 127 | 2318 ± 70 (−18%) | 2537 ± 121 (−10%) | 2818 ± 89 (−0%) | 2374 ± 92 (−16%) | 2420 ± 79 (−14%) | 2302 ± 109 (−19%) | 2325 ± 70 (−18%) |
| Random churn | Yes | 1873 ± 49 | 2460 ± 211 (+31%) | 2470 ± 175 (+32%) | 2868 ± 392 (+53%) | 2393 ± 155 (+28%) | 2274 ± 210 (+21%) | 2242 ± 79 (+20%) | 2215 ± 23 (+18%) |

Retained heap, in megabytes:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 675 | 568 (−16%) | 569 (−16%) | 568 (−16%) | 567 (−16%) | 568 (−16%) | 567 (−16%) | 570 (−16%) |
| Sliding window | No | 676 | 175 (−74%) | 180 (−73%) | 191 (−72%) | 190 (−72%) | 190 (−72%) | 183 (−73%) | 183 (−73%) |
| Sliding window | Yes | 179 | 119 (−34%) | 124 (−31%) | 135 (−25%) | 135 (−25%) | 135 (−25%) | 127 (−29%) | 127 (−29%) |
| Random churn | No | 715 | 468 (−35%) | 254 (−64%) | 469 (−34%) | 253 (−65%) | 234 (−67%) | 259 (−64%) | 235 (−67%) |
| Random churn | Yes | 217 | 433 (+100%) | 197 (−9%) | 432 (+99%) | 196 (−10%) | 175 (−19%) | 203 (−6%) | 178 (−18%) |

Output positions assigned, with the change against Blocks. Without a shift, every mode assigns as many positions as
Blocks, and add only assigns 10 million in every mode, so those are omitted; the sliding window assigns the same
whether groups return or not:

| Workload | Groups return | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- |
| Sliding window | Either | 10.00M | 2.96M (−70%) | 1.49M (−85%) | 1.49M (−85%) | 1.49M (−85%) |
| Random churn | No | 10.00M | 4.02M (−60%) | 8.59M (−14%) | 2.76M (−72%) | 2.57M (−74%) |
| Random churn | Yes | 8.71M | 3.25M (−63%) | 8.65M (−1%) | 2.62M (−70%) | 2.40M (−72%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 19.3 | 25.8 (+34%) | 11.4 (−41%) | 13.5 (−30%) | 14.9 (−23%) | 14.0 (−28%) | 18.2 (−6%) | 13.2 (−31%) |
| Sliding window | No | 46.4 | 8.7 (−81%) | 6.8 (−85%) | 10.3 (−78%) | 10.0 (−79%) | 10.0 (−78%) | 9.6 (−79%) | 9.6 (−79%) |
| Sliding window | Yes | 5.9 | 6.8 (+14%) | 6.2 (+4%) | 6.1 (+3%) | 6.1 (+4%) | 6.7 (+13%) | 7.6 (+28%) | 6.2 (+5%) |
| Random churn | No | 14.4 | 9.5 (−34%) | 8.5 (−41%) | 36.6 (+155%) | 28.9 (+101%) | 19.1 (+33%) | 19.7 (+37%) | 9.9 (−31%) |
| Random churn | Yes | 12.1 | 15.3 (+26%) | 8.0 (−34%) | 42.4 (+250%) | 25.3 (+108%) | 20.7 (+71%) | 10.1 (−17%) | 5.8 (−53%) |

Collapsing alone releases as much memory as collapsing with a shift did. For random churn it retains 235 to 259 MB
when groups do not return and 178 to 203 MB when they do, against 234 to 254 MB and 175 to 197 MB with a shift. An
earlier collapse that combined only adjacent sparse blocks retained 322 to 338 MB and 255 to 277 MB: a released block
between two sparse blocks kept them apart, until a shift removed it. The collapse now reaches across released blocks
instead.

### Reading block counts from the row set

`OutputPositionBlockTracker` stores a live count for every block, and bitsets of the released and the sparse blocks. A
prototype kept none of that. Every block's live count is already in the result's row set, as the difference of two
ranks, and a closed block with no live states is a released one. Each cycle it examined only the blocks the cycle's
removals touched and the blocks that closed: a block that had become empty was released, and one that had become sparse
seeded a run, found by stepping to the previous and next live block across released ones. The only state it kept
between cycles was the number of closed blocks and the sparse blocks whose run the budget of states to move had left
for a later cycle.

It made the same decisions. Two aggregations of the same random churn at a collapse fraction of 0.5, one with each
tracker, had identical result row sets and identical shifts downstream in all of 600 cycles, 426 of which collapsed.

It saved little memory:

| Positions assigned | Blocks | Counting tracker | Row-set tracker |
| --- | --- | --- | --- |
| 10 million | 4,883 | about 17 KB: 2 byte counts for up to 7,824 blocks, and two bitsets | a few hundred bytes |
| 2^31 - 1 | 1,048,576 | about 2.3 MB | a few hundred bytes |

Even at the limit, the counts take 2 bytes a block, where every result column's array source already keeps an 8 byte
reference a block.

And it cost more time. The tracker's own work per cycle, timed around its calls, for 1,000,000 live states with 10,000
removed at random and 10,000 added each cycle, over 2,000 cycles after 1,000 of warmup; two runs agreed within 3%:

| Collapse fraction | Tracker | Update | Collapse | Total |
| --- | --- | --- | --- | --- |
| 1 | counting | 67 µs | — | 67 µs |
| 1 | row set | 174 µs | — | 174 µs |
| 0.5 | counting | 61 µs | 56 µs | 117 µs |
| 0.5 | row set | 101 µs | 238 µs | 340 µs |

A cycle of the random churn benchmark takes about 2.6 ms at 0.5, so the counting tracker is about 4.5% of it and the
row-set tracker about 13%.

Random churn touches nearly every block each cycle, and a block's count costs a search of the row set where the
counting tracker adjusts an array element. At 0.5 each cycle touched 793 blocks, counted in one forward pass of an
iterator over the live states, and seeded 99 runs, whose neighbors took about 191 searches for the previous or next
live block. Those searches, `RspArray.get` and `find` in a profile, were the prototype's cost; a cache kept any block
from being counted twice. Taking the next live block from the forward pass would remove about half of the neighbor
searches, but not the previous live block's, since a block between two touched ones can be sparse, nor the counts of
the touched blocks that are not sparse, 694 of the 793. It would likely remain 1.5 to 2 times the counting tracker.

### Compaction

Compaction shifted the states after removed ones down into their positions, cell by cell where the positions did not
line up with blocks, and gave the positions past the new end back. It released no blocks in the middle of the result.
Each cycle could move no more live states than its input rows, and each cycle started again from the first free
position.

Compaction falls behind whenever removals are concentrated at the front. Keeping the result dense then means moving
every live state each cycle, and the budget covers only the cycle's changes. The sequence below is a sliding window of
6 live groups that removes the 2 oldest and adds 2 new groups each cycle, so each cycle may move 4 states:

| Cycle | States moved | Positions after | Gap | Last row key |
| --- | --- | --- | --- | --- |
| start | — | 0–5 | 0 | 5 |
| 1 | 4 | 0–3, 6–7 | 2 | 7 |
| 2 | 4 | 0–3, 8–9 | 4 | 9 |
| 3 | 4 | 0–3, 10–11 | 6 | 11 |
| 4 | 4 | 0–3, 12–13 | 8 | 13 |
| 5 | 4 | 0–3, 14–15 | 10 | 15 |

Every cycle spends its budget moving the same front states, the gap after them grows by the churn, and the last row
key grows exactly as it would without reclaiming. Compaction falls behind like this whenever the live groups exceed
twice the churn, which is the common case of a large table with small changes.

It also moved cells into holes one value at a time, where the other modes move cells only to free a whole block, and it
needed a `clear` hook on every operator, a range `setNull` on the column sources, and free-position tracking in the
state manager, none of which any other mode uses.

Measured with the benchmark described in [Benchmark results](#benchmark-results), compaction was the weakest mode:

| Workload | Groups return | Time, ms | Heap, MB | Positions, millions |
| --- | --- | --- | --- | --- |
| Add only | — | 865 ± 179 | 567 | 10.00 |
| Sliding window | No | 1053 ± 65 | 481 | 10.00 |
| Sliding window | Yes | 928 ± 105 | 423 | 10.00 |
| Random churn | No | 2750 ± 125 | 525 | 10.00 |
| Random churn | Yes | 2915 ± 153 | 425 | 8.71 |

It never lowered the positions assigned, it was the slowest mode wherever groups return, and for the sliding window it
retained 2.5 to 3.4 times the heap of the modes that collapse. These numbers come from an earlier run than the
tables above, before block shifting had a bulk variant.

### Combining any two blocks that fit

An earlier credit mode, before the bulk shift became a parameter of release blocks, combined blocks in pairs rather
than collapsing runs of sparse blocks: two closed blocks, consecutive among the blocks not released, were combined
whenever their live states fit in one block, releasing the upper one, whatever their fractions free. It shifted blocks
in bulk (see [Block shifting](#block-shifting)).

Collapsing at 0.5 with the bulk shift does nearly as well, and uses one combining mechanism rather than two. Measured in
an earlier run, the pairwise combine assigned 2.44 million positions for random churn with one row per group and
retained 229 MB, against 2.57 million and 234 MB for collapsing at 0.5 with the bulk shift in the tables of
[Block shifting](#block-shifting); with
returning groups, 2.35 million and 170 MB against 2.40 million and 175 MB. Their times were within the error of each
other.

A variant combined the pairs of blocks with the fewest live states first, wherever they were, since every combination
frees one block and the cheapest free the most blocks for the credit. It assigned the same number of positions as
combining from the first block on, because at the benchmark's churn every pair that fits is combined either way. Over
30 measured batches of random churn its longest cycle had a median of 18.2 ms against 16.9 ms, and its batches took
about 1% longer, from sorting the candidate pairs each cycle.

### Assigning new states to released positions

Assigning new states to released positions in the middle of the result would bound the positions assigned without
moving any state. It would give new groups lower row keys than older ones, so the result would no longer list groups
in the order they were first seen, even among groups that never empty. New states therefore always go after every
existing state, and only moves that keep the states in order give positions back.

### A separate state manager for reclaiming

Reclaiming first had a state manager of its own: a copy of the incremental aggregation state manager that supported
tombstones and bounded its incremental rehash by the slots it examined, so that `none` kept the original manager
unchanged. To measure what one manager for every mode would cost `none`, a temporary mode ran `none` on the
tombstone manager, which then never tombstoned a state. It cost nothing, so there is now one manager, and `none` never
creates a tombstone.

Median milliseconds per batch of 900 cycles, with one row per group. The 1M rows are the tables' workloads; the 10M
rows add and remove 100,000 rows each cycle. The run was on an Apple M5 Max that was not otherwise idle, with two forks
of five measured batches per configuration.

| Rows | Workload | Groups return | Original manager | Tombstone manager | Change |
| --- | --- | --- | --- | --- | --- |
| 1M | Add only | - | 927 | 840 | -9.4% |
| 1M | Sliding window | No | 1399 | 1302 | -6.9% |
| 1M | Sliding window | Yes | 657 | 625 | -4.9% |
| 1M | Random churn | No | 2424 | 2351 | -3.0% |
| 1M | Random churn | Yes | 1581 | 1527 | -3.4% |
| 10M | Add only | - | 13233 | 11491 | -13.2% |
| 10M | Sliding window | No | 17985 | 17545 | -2.4% |
| 10M | Sliding window | Yes | 8191 | 7827 | -4.4% |
| 10M | Random churn | No | 27614 | 26749 | -3.1% |
| 10M | Random churn | Yes | 17503 | 17862 | +2.1% |

- **Time:** with 100 rows per group, the two were within 4% of each other in every workload. The tombstone manager's
  rehash examines three old-table slots for each insert, so it most likely gains by finishing sooner: fewer cycles
  search the old table as well as the new one.
- **Longest cycle:** within the run-to-run variation of each other.
- **Retained heap:** the same at 10M. At 1M, the tombstone manager retained about 100 MB less in the three workloads
  whose groups do not return, because its rehash had already released the old table when the heap was measured; the
  peak is the same. The tables in [Benchmark results](#benchmark-results), measured on x86 with the one manager, show
  the same difference between `none` and the baseline.
- **Keys:** every key in the benchmark is a single `long` column. The original manager also had pregenerated hashers
  for multiple key columns, and the one manager keeps them.
