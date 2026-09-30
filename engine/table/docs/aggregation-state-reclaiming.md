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
reuses it. This is how aggregations behaved before reclaiming existed. It uses the hash table without tombstones.

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

- **Baseline:** main at f93afcf3d4 with two changes that are not part of this branch's reclaiming but speed up every
  aggregation: the adaptive `SoftRecycler` of [#8698](https://github.com/deephaven/deephaven-core/pull/8698), and not
  recording previous values for blocks allocated during an update cycle, from
  [#8713](https://github.com/deephaven/deephaven-core/pull/8713). Every percentage is the change against this column.
- **`none`:** this branch without reclaiming, which isolates its other improvements to the update cycle.
- **Blocks:** `releaseBlocks(1)`, the default, which releases blocks and never moves a state.
- **Collapse 0.75** and **Collapse 0.5:** `releaseBlocks(0.75)` and `releaseBlocks(0.5)`, which also collapse runs of
  sparse blocks.
- **Baseline again:** the baseline once more, after every other configuration, to show any drift over the run.

The branch was measured at 82d1ff9aa1, which contains the change of #8713 and merges the same main, with #8698
cherry-picked onto it. Both builds therefore have both changes, and the columns differ only by this branch's own work.

The numbers come from an Intel i9-14900KS (hybrid performance and efficiency cores) in an LXC container with 24 logical
CPUs and 128 GB, with the performance governor and turbo boost enabled, and otherwise idle. Each configuration ran two
JVM forks of three warmup and five measured batches; the error bounds are JMH's 99.9% intervals over the ten measured
batches. Baseline and Baseline again are within the error of each other in every workload, so the run did not drift.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 1377 ± 70 | 1398 ± 115 (+2%) | 1343 ± 115 (−2%) | 1289 ± 85 (−6%) | 1301 ± 60 (−5%) | 1425 ± 82 (+4%) |
| Sliding window | 1 | No | 1995 ± 111 | 2062 ± 78 (+3%) | 1308 ± 37 (−34%) | 1352 ± 32 (−32%) | 1345 ± 64 (−33%) | 2078 ± 120 (+4%) |
| Sliding window | 1 | Yes | 1085 ± 59 | 1123 ± 65 (+4%) | 1216 ± 55 (+12%) | 1263 ± 57 (+16%) | 1247 ± 59 (+15%) | 1065 ± 33 (−2%) |
| Random churn | 1 | No | 3920 ± 82 | 3607 ± 68 (−8%) | 3300 ± 52 (−16%) | 3477 ± 86 (−11%) | 3562 ± 101 (−9%) | 3917 ± 114 (−0%) |
| Random churn | 1 | Yes | 2930 ± 99 | 2584 ± 107 (−12%) | 3292 ± 54 (+12%) | 3364 ± 76 (+15%) | 3414 ± 50 (+17%) | 2872 ± 96 (−2%) |
| Add only | 100 | — | 185 ± 24 | 186 ± 33 (+0%) | 174 ± 17 (−6%) | 181 ± 26 (−2%) | 198 ± 20 (+7%) | 201 ± 36 (+8%) |
| Sliding window | 100 | No | 293 ± 33 | 307 ± 34 (+5%) | 273 ± 30 (−7%) | 281 ± 30 (−4%) | 298 ± 30 (+2%) | 278 ± 23 (−5%) |
| Sliding window | 100 | Yes | 317 ± 32 | 326 ± 23 (+3%) | 312 ± 28 (−2%) | 329 ± 32 (+4%) | 331 ± 23 (+4%) | 317 ± 35 (−0%) |
| Random churn | 100 | No | 839 ± 70 | 801 ± 74 (−4%) | 834 ± 72 (−1%) | 835 ± 83 (−0%) | 835 ± 51 (−0%) | 810 ± 72 (−3%) |
| Random churn | 100 | Yes | 784 ± 36 | 769 ± 71 (−2%) | 758 ± 39 (−3%) | 771 ± 53 (−2%) | 767 ± 46 (−2%) | 770 ± 40 (−2%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection, averaged over the measured
batches. It includes the source table and the benchmark's own data, and the blocks the recyclers hold, so compare the
modes with each other rather than reading the values as the aggregation's size.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 674 | 675 (+0%) | 573 (−15%) | 578 (−14%) | 578 (−14%) | 675 (+0%) |
| Sliding window | 1 | No | 680 | 683 (+0%) | 186 (−73%) | 186 (−73%) | 186 (−73%) | 679 (−0%) |
| Sliding window | 1 | Yes | 181 | 182 (+0%) | 130 (−28%) | 130 (−28%) | 131 (−28%) | 181 (−0%) |
| Random churn | 1 | No | 729 | 732 (+0%) | 484 (−34%) | 267 (−63%) | 240 (−67%) | 729 (+0%) |
| Random churn | 1 | Yes | 220 | 221 (+0%) | 452 (+105%) | 212 (−4%) | 186 (−16%) | 219 (−1%) |
| Add only | 100 | — | 30 | 29 (−3%) | 28 (−7%) | 32 (+6%) | 32 (+6%) | 29 (−3%) |
| Sliding window | 100 | No | 29 | 30 (+2%) | 26 (−12%) | 26 (−13%) | 26 (−12%) | 30 (+2%) |
| Sliding window | 100 | Yes | 21 | 22 (+2%) | 25 (+19%) | 25 (+19%) | 25 (+19%) | 21 (+0%) |
| Random churn | 100 | No | 52 | 52 (+1%) | 49 (−7%) | 54 (+3%) | 54 (+3%) | 52 (+0%) |
| Random churn | 100 | Yes | 43 | 43 (+0%) | 42 (−1%) | 47 (+10%) | 47 (+10%) | 42 (−1%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over the ten measured batches. A single worst sample is
noisy, as the difference between Baseline and Baseline again shows, so read the percentages for the modes whose longest
cycles are consistently high rather than for small differences. With 100 rows per group every longest cycle is a few
milliseconds, where one slow sample swings the percentage widely.

| Workload | Rows per group | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 60.6 | 61.5 (+2%) | 62.0 (+2%) | 61.8 (+2%) | 63.2 (+4%) | 62.0 (+2%) |
| Sliding window | 1 | No | 53.3 | 66.4 (+25%) | 17.3 (−68%) | 14.6 (−73%) | 15.8 (−70%) | 67.2 (+26%) |
| Sliding window | 1 | Yes | 14.5 | 11.8 (−19%) | 15.3 (+6%) | 15.1 (+4%) | 18.8 (+30%) | 16.7 (+15%) |
| Random churn | 1 | No | 50.0 | 64.5 (+29%) | 23.6 (−53%) | 23.9 (−52%) | 11.2 (−78%) | 64.7 (+29%) |
| Random churn | 1 | Yes | 11.2 | 12.4 (+10%) | 12.9 (+15%) | 14.9 (+33%) | 13.2 (+18%) | 11.4 (+2%) |
| Add only | 100 | — | 1.3 | 1.4 (+6%) | 1.2 (−10%) | 1.2 (−8%) | 1.0 (−29%) | 1.0 (−24%) |
| Sliding window | 100 | No | 3.3 | 1.2 (−63%) | 0.9 (−72%) | 1.2 (−63%) | 1.2 (−63%) | 1.2 (−64%) |
| Sliding window | 100 | Yes | 1.0 | 1.3 (+25%) | 1.3 (+25%) | 1.1 (+8%) | 1.0 (−1%) | 0.9 (−15%) |
| Random churn | 100 | No | 3.0 | 3.3 (+7%) | 2.8 (−7%) | 2.8 (−7%) | 2.4 (−22%) | 3.5 (+14%) |
| Random churn | 100 | Yes | 2.6 | 2.4 (−6%) | 3.2 (+23%) | 3.2 (+23%) | 2.9 (+14%) | 2.6 (+1%) |

### Ten million rows

The same workloads with one row per group over a table of 10,000,000 rows, with 100,000 rows added and removed each
cycle, run with a 48 GB heap.

Milliseconds per batch of 900 cycles:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 16201 ± 245 | 16020 ± 303 (−1%) | 14216 ± 302 (−12%) | 14328 ± 232 (−12%) | 14413 ± 203 (−11%) | 16073 ± 306 (−1%) |
| Sliding window | No | 22773 ± 243 | 22812 ± 884 (+0%) | 16407 ± 119 (−28%) | 16661 ± 123 (−27%) | 16543 ± 101 (−27%) | 22444 ± 279 (−1%) |
| Sliding window | Yes | 12619 ± 297 | 13007 ± 230 (+3%) | 13992 ± 368 (+11%) | 14213 ± 103 (+13%) | 14213 ± 138 (+13%) | 12782 ± 109 (+1%) |
| Random churn | No | 43274 ± 294 | 40136 ± 329 (−7%) | 35951 ± 359 (−17%) | 37003 ± 400 (−14%) | 37868 ± 655 (−12%) | 43098 ± 366 (−0%) |
| Random churn | Yes | 30879 ± 131 | 26750 ± 149 (−13%) | 34813 ± 283 (+13%) | 34852 ± 339 (+13%) | 35232 ± 218 (+14%) | 30676 ± 181 (−1%) |

Retained heap, in megabytes:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 5623 | 5622 (−0%) | 5077 (−10%) | 5091 (−9%) | 5086 (−10%) | 5623 (+0%) |
| Sliding window | No | 5658 | 5694 (+1%) | 898 (−84%) | 898 (−84%) | 899 (−84%) | 5658 (+0%) |
| Sliding window | Yes | 1426 | 1434 (+1%) | 898 (−37%) | 899 (−37%) | 899 (−37%) | 1428 (+0%) |
| Random churn | No | 6164 | 6192 (+0%) | 4327 (−30%) | 2139 (−65%) | 1864 (−70%) | 6164 (−0%) |
| Random churn | Yes | 1803 | 1809 (+0%) | 4114 (+128%) | 1692 (−6%) | 1407 (−22%) | 1802 (−0%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Baseline | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Baseline again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 507.2 | 493.5 (−3%) | 520.5 (+3%) | 506.8 (−0%) | 499.0 (−2%) | 514.8 (+2%) |
| Sliding window | No | 508.1 | 510.9 (+1%) | 92.8 (−82%) | 99.4 (−80%) | 113.6 (−78%) | 513.2 (+1%) |
| Sliding window | Yes | 110.3 | 143.4 (+30%) | 126.4 (+15%) | 114.9 (+4%) | 133.8 (+21%) | 145.4 (+32%) |
| Random churn | No | 213.8 | 297.0 (+39%) | 135.4 (−37%) | 133.5 (−38%) | 94.5 (−56%) | 514.3 (+140%) |
| Random churn | Yes | 99.1 | 79.7 (−20%) | 81.7 (−18%) | 77.1 (−22%) | 74.1 (−25%) | 86.7 (−13%) |

### Findings

- This branch without reclaiming (`none`) is 8% faster than the baseline for random churn with one row per group when
  groups do not return, and 12% faster when they do; at ten million rows, 7% and 13%. The baseline already has #8698
  and #8713, so this comes from the branch's other changes to the update cycle; which one has not been isolated.
  Elsewhere it is within the error.
- When groups do not return, every reclaiming mode is faster than the baseline for the sliding window, by 32% to 34%,
  and retains about a quarter of its heap; at ten million rows, 27% to 28% faster with a sixth of the heap. For random
  churn, releasing blocks alone is 16% faster and retains a third less heap; collapsing is 9% to 11% faster and retains
  a third of the heap. At ten million rows, releasing blocks alone is 17% faster and collapsing 12% to 14%, and add only
  is 11% to 12% faster with reclaiming.
- When groups return, every reclaiming mode is slower, since returning groups are new states rather than reused ones.
  The sliding window is 12% slower releasing blocks alone and 15% to 16% slower collapsing, and 11% to 13% slower at ten
  million rows. Random churn with returning groups is 12% slower releasing blocks alone and 15% to 17% slower
  collapsing, and 13% to 14% slower at ten million rows. Against this branch's own `none`, which also reuses states, the
  reclaiming modes cost 27% to 32% there. Releasing blocks alone retains twice the heap of the baseline for random churn
  with returning groups, and collapsing 4% to 16% less (at ten million rows, 6% to 22% less).
- An earlier run measured against main without #8698 and #8713, and there random churn with returning groups at ten
  million rows was 8% to 10% faster with reclaiming. That gain was the two changes', not reclaiming's.
- For random churn, releasing blocks alone is 5% to 8% faster than collapsing when groups do not return, and 2% to 4%
  faster when they do; at ten million rows, 3% to 5% and within 2%. Collapsing retains far less heap.
- No mode gives output positions back, so every reclaiming mode assigns the same positions: with one row per group,
  10 million when groups do not return, and 8.71 to 10 million when they do, where the baseline reuses 2 million; with
  100 rows per group, 100,000, or 20,000 for random churn with returning groups.
- With 100 rows per group, the modes are within the error of each other for time.
- Reclaiming shortens the longest cycles when groups do not return: 15 to 17 ms for the sliding window and 11 to 24 ms
  for random churn, against 50 to 67 ms for the baseline and baseline again; at ten million rows, 93 to 114 ms and 95
  to 135 ms, against 214 to 514 ms. When groups return, the longest cycles are 11 to 19 ms for the baseline and every
  mode, and 74 to 145 ms at ten million rows.

## Long output positions

Output positions are `int`s, and no mode gives them back, so an aggregation runs out after 2^31 - 1 states (see
[Costs and limits](#costs-and-limits)). Storing them as `long`s would remove the limit without moving states to give
positions back, which was tried and rejected (see [Block shifting](#block-shifting)): releasing blocks as they empty
bounds memory, and positions can keep increasing. Three experiments, not part of this change, measured what `long`
positions cost. They were run while block shifting was still a parameter of release blocks, so the modes below are
written with its parameters.

### What changed in the experiments

The two places that hold positions were widened separately:

- **The hash table's positions:** the tombstone state manager stores each slot's output position as a `long`, in both
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
